package main

import (
	"context"
	crand "crypto/rand"
	"fmt"
	log "log/slog"
	"slices"
	"solver-handler/external"
	"solver-handler/redisconn"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type BucketHandler struct {
	//TODO: map[string]map[string]SolverInterface[any] - key -> solver name
	solvers map[string][]SolverInterface[any]
	stats   map[string]*KeyStats

	config atomic.Pointer[external.Config]        // current config, replaced whole on each load
	sm     *external.SessionManager[Product[any]] // external stock of products, nil means no stock
	proxy  *external.ProxyProvider[ProxyInfo]     // hands out proxies by key, nil means no provider

	redis *redis.Client
	id    string       // identifies this task in the inflight marks
	seq   atomic.Int64 // numbers the stock reservations of this task

	states  sync.Map // key -> []solverState, refreshed by every stock tick
	proxies sync.Map // key -> string, proxy the stock of the key uses until it fails
	probes  sync.Map // key/solver -> *probeState
}

func NewBucketHandler() *BucketHandler {
	redis, err := redisconn.Connect(context.Background())
	if err != nil {
		log.Warn("redis connection failed")
		return nil
	}

	stats := make(map[string]*KeyStats, len(registry))
	for key := range registry {
		stats[key] = &KeyStats{}
	}

	return &BucketHandler{redis: redis, solvers: registry, stats: stats, id: crand.Text()}
}

// TODO: Criar uma struct de erro com informações de métricas importantes para avaliação.
// GetProduct returns a product from the stock or solves one now, in up to solveTries attempts.
func (b *BucketHandler) GetProduct(ctxClient context.Context, req Request) (Product[any], error) {
	budget := float64(maxSolveTimeout)
	if req.Timeout != nil && *req.Timeout > 0 {
		budget = *req.Timeout
	}
	ctx, cancel := context.WithTimeout(ctxClient, seconds(budget))
	defer cancel()

	key := req.Key
	if _, ok := b.stats[key]; !ok {
		return Product[any]{}, fmt.Errorf("nenhuma chave registrada: %s", key)
	}
	b.countRequest(key)

	// Any failure of the external stock falls back to solving now.
	if b.sm != nil {
		if product, err := b.sm.Get(ctx, key); err == nil {
			if ctx.Err() != nil { // the caller left, so give the product back
				go func() {
					if _, err := b.sm.Save(context.Background(), key, product); err != nil {
						log.Warn("falha ao devolver produto ao estoque", "key", key, "err", err)
					}
				}()
				return Product[any]{}, ctxError(ctxClient, budget)
			}
			return product, nil
		}
	}
	if ctx.Err() != nil {
		return Product[any]{}, ctxError(ctxClient, budget)
	}

	order, err := b.TryGetSolver(key)
	if err != nil {
		return Product[any]{}, err
	}

	candidates := b.firstCandidates(key, order)
	var lastErr error
	for range solveTries {
		solver, r, gone := b.attemptNow(ctx, key, candidates, req.Params)
		if gone {
			return Product[any]{}, ctxError(ctxClient, budget)
		}
		if r.info.err == nil {
			return r.prod, nil
		}
		lastErr = r.info.err
		// Proxy, target limit and client errors would fail on another solver too.
		if !r.info.retryable() || ctx.Err() != nil {
			break
		}
		if candidates = b.retryCandidates(key, order, solver); len(candidates) == 0 {
			break
		}
	}
	return Product[any]{}, lastErr
}

const (
	maxSolveTimeout = 30 // seconds, also the default budget of GetProduct
	minSolveTimeout = 2  // seconds
	solveTries      = 3  // attempts of a generation solved now
)

type attemptResult struct {
	prod Product[any]
	info Info
}

// ctxError is the error of a finished GetProduct context: the caller's own, or the budget running out.
func ctxError(client context.Context, budget float64) error {
	if err := client.Err(); err != nil {
		return err
	}
	return fmt.Errorf("orçamento de %gs esgotado: %w", budget, context.DeadlineExceeded)
}

// attemptNow solves one product with the first candidate with room. If ctx ends first it
// returns gone, and the product goes to the stock when ready.
func (b *BucketHandler) attemptNow(ctx context.Context, key string, candidates []SolverInterface[any], params Params) (SolverInterface[any], attemptResult, bool) {
	solver, member := b.startNow(key, candidates)
	b.probeOf(key, solver.GetName()).used()

	solveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), seconds(b.TimeoutSolve(solver)))
	done := make(chan attemptResult, 1)
	go func() {
		defer cancel()
		defer b.finishNow(key, solver.GetName(), member)
		prod, info := b.solve(solveCtx, key, solver, params)
		done <- attemptResult{prod, info}
	}()

	select {
	case r := <-done:
		return solver, r, false
	case <-ctx.Done():
		go func() {
			if r := <-done; r.info.err == nil {
				b.savePool(context.Background(), key, r.prod)
			}
		}()
		return solver, attemptResult{}, true
	}
}

// TimeoutSolve returns the timeout of one solve by solver, in seconds.
func (b *BucketHandler) TimeoutSolve(solver SolverInterface[any]) float64 {
	return solver.GetStatistic().TimeoutGen(maxSolveTimeout, minSolveTimeout)
}

// seconds converts s seconds to a Duration.
func seconds(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }

// solve generates a product of key now with solver and records the generation in the global metrics.
func (b *BucketHandler) solve(ctx context.Context, key string, solver SolverInterface[any], params Params) (Product[any], Info) {
	solver.AddInflights()
	start := time.Now()
	prod, info := NewProductBySolve(ctx, key, solver, params)
	if info.err == nil || info.solverFault() { // a failure that is not the solver's must not trip its metrics
		b.recordGeneration(key, solver.GetName(), time.Since(start), info.err != nil)
	}
	if info.kind() == external.KindTargetLimit {
		b.recordThrottle(key)
	}
	solver.SubInflights()

	return prod, info
}

// TODO: O report também pode ser a um sistema externo. Por exemplo: um gerenciador de proxy que precisa de um report de sucesso ou falha para poder gerenciar a proxy.
// ReportUsage records whether a product of the named Solver worked when used.
func (b *BucketHandler) ReportUsage(key string, solver string, success bool) error {
	for _, s := range b.solvers[key] {
		if s.GetName() == solver {
			s.GetStatistic().AddUsage(time.Now(), success)
			return nil
		}
	}
	return fmt.Errorf("nenhum Solver %q registrado com a chave fornecida", solver)
}

// TryGetSolver orders the key's solvers by preference, adaptive score or fixed weight, per config.
// Callers take the first one with room.
func (b *BucketHandler) TryGetSolver(key string) ([]SolverInterface[any], error) {
	solvers := b.solvers[key]
	if len(solvers) == 0 {
		return nil, fmt.Errorf("nenhum Solver registrado com a chave fornecida")
	}
	if b.cfg().AdaptiveChoice {
		return PickSolver(solvers, b.solverStates(key)), nil
	}
	return DrawSolver(solvers), nil
}

// DrawSolver orders the solvers by draws without replacement, proportional to their weight.
// Solvers with weight 0 are disabled, unless all are.
func DrawSolver(solvers []SolverInterface[any]) []SolverInterface[any] {
	var pool []SolverInterface[any]
	var weights []float64
	for _, s := range solvers {
		if s.GetWeight() > 0 {
			pool = append(pool, s)
			weights = append(weights, float64(s.GetWeight()))
		}
	}
	if len(pool) == 0 {
		return slices.Clone(solvers)
	}
	return drawOrder(pool, weights)
}

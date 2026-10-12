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
// TODO: Cliente que abrir requisição e desistir do produto, o produto deve ser salvo na fila e não descartado.
func (b *BucketHandler) GetProduct(req Request) (Product[any], error) {
	key := req.Key
	if _, ok := b.stats[key]; !ok {
		return Product[any]{}, fmt.Errorf("nenhuma chave registrada: %s", key)
	}
	b.countRequest(key)

	// Any failure of the external stock falls back to solving now.
	if b.sm != nil {
		if product, err := b.sm.Get(context.Background(), key); err == nil {
			return product, nil
		}
	}

	order, err := b.TryGetSolver(key)
	if err != nil {
		return Product[any]{}, err
	}
	solver, member := b.startNow(key, order)
	defer b.finishNow(key, solver.GetName(), member)

	// The solver's timeout, limited by the client's (in seconds).
	t := b.TimeoutSolve(solver)
	if req.Timeout != nil && *req.Timeout > 0 {
		t = min(t, *req.Timeout)
	}
	ctx, cancel := context.WithTimeout(context.Background(), seconds(t))
	defer cancel()

	prod, info := b.solve(ctx, key, solver, req.Params)

	return prod, info.err
}

const (
	maxSolveTimeout = 30 // seconds, also the timeout while the solver's latency is not measured
	minSolveTimeout = 2  // seconds
)

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

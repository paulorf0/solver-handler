package main

import (
	"context"
	crand "crypto/rand"
	"errors"
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
	return b.solve(key, solver, req.Params)
}

// solve generates a product of key now with solver and records the generation in the global metrics.
func (b *BucketHandler) solve(key string, solver SolverInterface[any], params Params) (Product[any], error) {
	solver.AddInflights()
	start := time.Now()
	prod, err := NewProductBySolve(key, solver, params)
	if !errors.Is(err, ErrProxy) { // a bad proxy must not trip the solver's metrics
		b.recordGeneration(key, solver.GetName(), time.Since(start), err)
	}
	solver.SubInflights()

	return prod, err
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

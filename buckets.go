package main

import (
	"context"
	crand "crypto/rand"
	"fmt"
	log "log/slog"
	"math/rand/v2"
	"solver-handler/external"
	"solver-handler/redisconn"
	"time"

	"github.com/redis/go-redis/v9"
)

type BucketHandler struct {
	solvers map[Key][]SolverInterface[any]
	stats   map[Key]*KeyStats

	config external.Config
	sm     external.SessionManager[Product[any]] // external stock of products

	redis *redis.Client
	id    string // identifies this task in the Signal lock
}

func NewBucketHandler() *BucketHandler {
	redis, err := redisconn.Connect(context.Background())
	if err != nil {
		log.Warn("redis connection failed")
		return nil
	}

	stats := make(map[Key]*KeyStats, len(registry))
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
	product, err := b.sm.Get(context.Background(), key)
	if err == nil {
		return product, nil
	}

	solver, err := b.TryGetSolver(key)
	if err != nil {
		return Product[any]{}, err
	}

	solver.AddInflights()
	prod, err := NewProductBySolve(key, solver, req.Params)
	solver.SubInflights()

	return prod, err
}

// TODO: O report também pode ser a um sistema externo. Por exemplo: um gerenciador de proxy que precisa de um report de sucesso ou falha para poder gerenciar a proxy.
// ReportUsage records whether a product of the named Solver worked when used.
func (b *BucketHandler) ReportUsage(key Key, solver string, success bool) error {
	for _, s := range b.solvers[key] {
		if s.GetName() == solver {
			s.GetStatistic().AddUsage(time.Now(), success)
			return nil
		}
	}
	return fmt.Errorf("nenhum Solver %q registrado com a chave fornecida", solver)
}

// TryGetSolver picks one of the Key's solvers: adaptive score or fixed weight, per config.
func (b *BucketHandler) TryGetSolver(key Key) (SolverInterface[any], error) {
	solve, ok := b.solvers[key]
	if !ok || len(solve) == 0 {
		return nil, fmt.Errorf("nenhum Solver registrado com a chave fornecida")
	}
	if b.config.AdaptiveChoice {
		return PickSolver(solve), nil
	}
	return DrawSolver(solve), nil
}

// DrawSolver sorteia um Solver com probabilidade proporcional ao seu peso.
func DrawSolver(solvers []SolverInterface[any]) SolverInterface[any] {
	if len(solvers) == 0 {
		return nil
	}

	total := 0
	for _, s := range solvers {
		total += s.GetWeight()
	}
	if total == 0 {
		return solvers[0]
	}

	n := rand.IntN(total)
	for _, s := range solvers {
		n -= s.GetWeight()
		if n < 0 {
			return s
		}
	}
	return solvers[len(solvers)-1]
}

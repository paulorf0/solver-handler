package main

import (
	"context"
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

	sCfg SolverConfig
	sm   external.SessionManager[Product[any]] // external stock of products

	redis *redis.Client
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

	return &BucketHandler{redis: redis, solvers: registry, stats: stats}
}

// TODO: Criar uma struct de erro com informações de métricas importantes para avaliação.
func (b *BucketHandler) GetProduct(key Key) (Product[any], error) {
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
	prod, err := NewProductBySolve(key, solver)
	solver.SubInflights()

	return prod, err
}

// countRequest adds 1 to the global request counter of key in Redis, plus earlier failed increments.
func (b *BucketHandler) countRequest(key Key) {
	if b.redis == nil {
		return
	}
	s := b.stats[key]
	n := s.failedIncr.Swap(0) + 1
	if err := b.redis.IncrBy(context.Background(), requestsKey(key), n).Err(); err != nil {
		s.failedIncr.Add(n)
	}
}

// RequestRate returns the global requests per second of key since the last measure.
// The first call only takes the measure and returns 0.
func (b *BucketHandler) RequestRate(ctx context.Context, key Key) (float64, error) {
	s, ok := b.stats[key]
	if !ok {
		return 0, fmt.Errorf("nenhuma estatística registrada com a chave fornecida")
	}
	total, err := b.redis.Get(ctx, requestsKey(key)).Int64()
	if err != nil && err != redis.Nil {
		return 0, fmt.Errorf("falha ao ler o contador de requisições: %w", err)
	}
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()
	rate := 0.0
	if !s.lastAt.IsZero() {
		rate = float64(total-s.lastCount) / now.Sub(s.lastAt).Seconds()
	}
	s.lastCount, s.lastAt = total, now
	return rate, nil
}

func requestsKey(key Key) string { return "requests:" + key.String() }

// ReportUsage records whether a product of the named solver worked when used.
func (b *BucketHandler) ReportUsage(key Key, solver string, success bool) error {
	for _, s := range b.solvers[key] {
		if s.GetName() == solver {
			s.GetStatistic().AddUsage(time.Now(), success)
			return nil
		}
	}
	return fmt.Errorf("nenhum solver %q registrado com a chave fornecida", solver)
}

// TryGetSolver picks one of the key's solvers: adaptive score or fixed weight, per sCfg.
func (b *BucketHandler) TryGetSolver(key Key) (SolverInterface[any], error) {
	solve, ok := b.solvers[key]
	if !ok || len(solve) == 0 {
		return nil, fmt.Errorf("nenhum solver registrado com a chave fornecida")
	}
	if b.sCfg.adaptiveChoice {
		return PickSolver(solve), nil
	}
	return DrawSolver(solve), nil
}

// DrawSolver sorteia um solver com probabilidade proporcional ao seu peso.
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

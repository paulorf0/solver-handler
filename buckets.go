package main

import (
	"context"
	"fmt"
	log "log/slog"
	"math/rand/v2"
	"solver-handler/fifo"
	"solver-handler/redisconn"
	"time"

	"github.com/redis/go-redis/v9"
)

type BucketHandler struct {
	queues  map[Key]*Queue
	solvers map[Key][]SolverInterface[any]
	stats   map[Key]*KeyStats

	redis *redis.Client
}

func NewBucketHandler() *BucketHandler {
	redis, err := redisconn.Connect(context.Background())
	if err != nil {
		log.Warn("redis connection failed")
		return nil
	}

	queues := make(map[Key]*Queue, len(registry))
	stats := make(map[Key]*KeyStats, len(registry))
	for key := range registry {
		queues[key] = fifo.New[Product[any]]()
		stats[key] = &KeyStats{}
	}

	return &BucketHandler{redis: redis, queues: queues, solvers: registry, stats: stats}
}

// TODO: Criar uma struct de erro com informações de métricas importantes para avaliação.
func (b *BucketHandler) GetProduct(key Key) (Product[any], error) {
	queue, err := b.TryGetQueue(key)
	if err != nil {
		return Product[any]{}, err
	}

	product, ok := queue.Peek()
	b.stats[key].AddRequest(time.Now(), ok)
	if ok {
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

// TryGetQueue return the queue that contains the Product generate by the solver.
func (b *BucketHandler) TryGetQueue(key Key) (*Queue, error) {
	queue, ok := b.queues[key]
	if !ok {
		return nil, fmt.Errorf("nenhuma queue registrada com a chave fornecida")
	}
	return queue, nil
}

// TryGetSolver return the solver that contains the logic to generate the product.
// Por enquanto devolve sempre o primeiro solver da lista da chave.
func (b *BucketHandler) TryGetSolver(key Key) (SolverInterface[any], error) {
	solve, ok := b.solvers[key]
	if !ok || len(solve) == 0 {
		return nil, fmt.Errorf("nenhum solver registrado com a chave fornecida")
	}
	return solve[0], nil
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

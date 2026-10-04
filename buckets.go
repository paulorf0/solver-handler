package main

import (
	"context"
	"fmt"
	log "log/slog"
	"solver-handler/fifo"
	"solver-handler/redisconn"

	"github.com/redis/go-redis/v9"
)

type BucketHandler struct {
	queues  map[Key]*Queue
	solvers map[Key]Solver[any]

	redis *redis.Client
}

func NewBucketHandler() *BucketHandler {
	redis, err := redisconn.Connect(context.Background())
	if err != nil {
		log.Warn("redis connection failed")
		return nil
	}

	queues := make(map[Key]*Queue, len(registry))
	for key := range registry {
		queues[key] = fifo.New[Product[any]]()
	}

	return &BucketHandler{redis: redis, queues: queues, solvers: registry}
}

// TODO: Criar uma struct de erro com informações de métricas importantes para avaliação.
func (b *BucketHandler) GetProduct(key Key) (Product[any], error) {
	queue, err := b.TryGetQueue(key)
	if err != nil {
		return Product[any]{}, err
	}

	product, ok := queue.Peek()
	if ok {
		return product, nil
	}

	solver, err := b.TryGetSolver(key)
	if err != nil {
		return Product[any]{}, err
	}

	prod, err := NewProductBySolve(key, solver)
	return prod, err
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
func (b *BucketHandler) TryGetSolver(key Key) (Solver[any], error) {
	solve, ok := b.solvers[key]
	if !ok {
		return nil, fmt.Errorf("nenhum solver registrado com a chave fornecida")
	}
	return solve, nil
}

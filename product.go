package main

import (
	"fmt"
	"time"
)

type Product[T any] struct {
	id     string
	key    Key
	solver string // name of the solver that generated it, used by ReportUsage

	at      time.Time
	elapsed int64

	token T
}

func GetHash(key Key) string {
	fixedHash := fmt.Sprintf("%s|%s", Version, key) // This initial hash never change.
	return fixedHash
}

func NewProductBySolve[T any](key Key, solver SolverInterface[T]) (Product[any], error) {
	now := time.Now()
	token, err := solver.Solve()
	elapsed := time.Since(now)
	solver.GetStatistic().AddGeneration(time.Now(), elapsed, err)
	if err != nil {
		return Product[any]{}, err
	}

	hash := GetHash(key)

	prod := Product[any]{id: hash, key: key, solver: solver.GetName(), at: now, elapsed: int64(elapsed), token: token}

	return prod, nil
}

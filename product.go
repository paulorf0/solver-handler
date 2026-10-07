package main

import (
	"fmt"
	"time"
)

type Product[T any] struct {
	Id     string
	Key    Key
	Solver string // name of the Solver that generated it, used by ReportUsage

	At      time.Time
	Elapsed int64

	Token T
	TTL   TTL
}

func GetHash(key Key) string {
	fixedHash := fmt.Sprintf("%s|%s", Version, key) // This initial hash never change.
	return fixedHash
}

func NewProductBySolve[T any](key Key, solver SolverInterface[T], params Params) (Product[any], error) {
	now := time.Now()
	token, err := solver.Solve(params)
	elapsed := time.Since(now)
	solver.GetStatistic().AddGeneration(time.Now(), elapsed, err)
	if err != nil {
		return Product[any]{}, err
	}

	hash := GetHash(key)

	prod := Product[any]{Id: hash, Key: key, Solver: solver.GetName(), At: now, Elapsed: int64(elapsed), Token: token}

	return prod, nil
}

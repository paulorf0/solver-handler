package main

import (
	"errors"
	"fmt"
	"time"
)

type Product[T any] struct {
	Id     string
	Key    string
	Solver string // name of the Solver that generated it, used by ReportUsage

	At      time.Time
	Elapsed int64

	Token T
}

func GetHash(key string) string {
	fixedHash := fmt.Sprintf("%s|%s", Version, key) // This initial hash never change.
	return fixedHash
}

func NewProductBySolve[T any](key string, solver SolverInterface[T], params Params) (Product[any], error) {
	now := time.Now()
	token, err := solver.Solve(params)
	elapsed := time.Since(now)
	if !errors.Is(err, ErrProxy) { // a bad proxy says nothing about the solver
		solver.GetStatistic().AddGeneration(time.Now(), elapsed, err)
	}
	if err != nil {
		return Product[any]{}, err
	}

	hash := GetHash(key)

	prod := Product[any]{Id: hash, Key: key, Solver: solver.GetName(), At: now, Elapsed: int64(elapsed), Token: token}

	return prod, nil
}

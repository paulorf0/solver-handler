package main

import (
	"context"
	"fmt"
	"time"
)

type Product[T any] struct {
	Id     string `json:"id"`
	Key    string `json:"key"`
	Solver string `json:"solver"` // name of the Solver that generated it, used by ReportUsage

	At      time.Time `json:"at"`
	Elapsed int64     `json:"elapsed"`

	Token  T      `json:"token"`
	Params Params `json:"params,omitzero"` // what the solver used, e.g. the proxy the client must reuse
}

func GetHash(key string) string {
	fixedHash := fmt.Sprintf("%s|%s", Version, key) // This initial hash never change.
	return fixedHash
}

func NewProductBySolve[T any](ctx context.Context, key string, solver SolverInterface[T], params Params) (Product[any], Info) {
	now := time.Now()
	token, info := solver.Solve(ctx, params)
	elapsed := time.Since(now)

	solver.GetStatistic().AddGeneration(time.Now(), elapsed, info)
	if info.err != nil {
		return Product[any]{}, info
	}

	hash := GetHash(key)
	prod := Product[any]{Id: hash, Key: key, Solver: solver.GetName(), At: now, Elapsed: int64(elapsed), Token: token, Params: params}

	return prod, info
}

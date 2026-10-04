package main

import (
	"fmt"
	"time"
)

type Product[T any] struct {
	id  string
	key Key

	at      time.Time
	elapsed int64

	token T
}

func GetHash(key Key) string {
	fixedHash := fmt.Sprintf("%s|%s", Version, key) // This initial hash never change.
	return fixedHash
}

func NewProductBySolve[T any](key Key, solver Solver[T]) (Product[any], error) {
	now := time.Now()
	token, err := solver.Solve()
	if err != nil {
		return Product[any]{}, err
	}
	elapsed := time.Since(now)
	hash := GetHash(key)

	prod := Product[any]{id: hash, key: key, at: now, elapsed: int64(elapsed), token: token}

	return prod, nil
}

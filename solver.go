package main

type Solver[T any] interface {
	Key() Key // The key pattern is: "something|client".
	Solve() (T, error)
}

type BaseSolver struct {
	key Key // The solver is related to the key.
}

func NewBaseSolver(key string) BaseSolver { return BaseSolver{key: Key(key)} }
func (b *BaseSolver) Key() Key {
	return b.key
}

// registry maps the full key ("something|client") to the solver that handles it.
var registry = map[Key]Solver[any]{}

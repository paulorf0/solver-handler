package main

import "sync/atomic"

type SolverInterface[T any] interface {
	GetName() string
	GetKey() Key
	GetWeight() int
	GetStatistic() *SolverStats
	GetInflights() int

	AddInflights()
	SubInflights()

	Solve() (T, error)
}

// BaseSolver is the "parent" struct: concrete solvers embed it and only implement Solve.
type BaseSolver struct {
	name      string // identifies the solver among the solvers of the same key
	key       Key
	weight    uint8        // In percent, 0 - 100 %
	inflights atomic.Int32 // Quantity of product currently being generated.

	stats SolverStats
}

func NewBaseSolver(name, key string, weight uint8) BaseSolver {
	return BaseSolver{name: name, key: Key(key), weight: weight}
}

func (b *BaseSolver) GetName() string            { return b.name }
func (b *BaseSolver) GetKey() Key                { return b.key }
func (b *BaseSolver) GetWeight() int             { return int(b.weight) }
func (b *BaseSolver) GetStatistic() *SolverStats { return &b.stats }
func (b *BaseSolver) GetInflights() int          { return int(b.inflights.Load()) }

func (b *BaseSolver) AddInflights() { b.inflights.Add(+1) }
func (b *BaseSolver) SubInflights() {
	if b.inflights.Load() > 0 {
		b.inflights.Add(-1)
	}
}

// registry maps the full key ("something|client") to the solver that handles it.
var registry = map[Key][]SolverInterface[any]{}

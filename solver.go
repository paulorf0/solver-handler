package main

import "sync/atomic"

type SolverInterface[T any] interface {
	GetName() string
	GetKey() Key
	GetWeight() int
	GetStatistic() *SolverStats
	GetInflights() int
	GetLimit() int // concurrent limit, 0 means no limit
	SetLimit(limit int)

	AddInflights()
	SubInflights()

	Solve(params Params) (T, error) // params: client data from Request, may be empty
}

// BaseSolver is the "parent" struct: concrete solvers embed it and only implement Solve.
type BaseSolver struct {
	name      string // identifies the Solver among the solvers of the same key
	key       Key
	weight    uint8        // In percent, 0 - 100 %
	inflights atomic.Int32 // Quantity of product currently being generated.
	limit     atomic.Int32 // Concurrent limit, from the config. 0 means no limit.

	stats SolverStats
}

func NewBaseSolver(name string, key Key, weight uint8) BaseSolver {
	return BaseSolver{name: name, key: key, weight: weight}
}

func (b *BaseSolver) GetName() string            { return b.name }
func (b *BaseSolver) GetKey() Key                { return b.key }
func (b *BaseSolver) GetWeight() int             { return int(b.weight) }
func (b *BaseSolver) GetStatistic() *SolverStats { return &b.stats }
func (b *BaseSolver) GetInflights() int          { return int(b.inflights.Load()) }

func (b *BaseSolver) GetLimit() int          { return int(b.limit.Load()) }
func (b *BaseSolver) SetLimit(limit int)     { b.limit.Store(int32(limit)) }

func (b *BaseSolver) AddInflights() { b.inflights.Add(+1) }
func (b *BaseSolver) SubInflights() {
	for {
		n := b.inflights.Load()
		if n <= 0 || b.inflights.CompareAndSwap(n, n-1) {
			return
		}
	}
}

// registry maps each Key to the solvers that handle it.
var registry = map[Key][]SolverInterface[any]{}

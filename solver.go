package main

import (
	"context"
	"solver-handler/external"
	"sync/atomic"
)

// Info is the outcome of a Solve: err is nil on success, errCode is the http code of the
// failure (0 when there is none).
type Info struct {
	err     error
	errCode int
}

// solverFault tells whether the solve failed by the solver's fault. Failures that are not its
// fault (proxy, target limit, no http code) must not touch its metrics.
func (i Info) solverFault() bool {
	return i.err != nil && external.ItsSolverError(i.errCode)
}

type SolverInterface[T any] interface {
	GetName() string
	GetKey() string
	GetWeight() int
	GetStatistic() *SolverStats
	GetInflights() int
	GetLimit() int // concurrent limit, 0 means no limit
	SetLimit(limit int)

	AddInflights()
	SubInflights()

	Solve(ctx context.Context, params Params) (T, Info) // params: client data from Request, may be empty
}

// BaseSolver is the "parent" struct: concrete solvers embed it and only implement Solve.
type BaseSolver struct {
	name      string // identifies the Solver among the solvers of the same key
	key       string
	weight    uint8        // In percent, 0 - 100 %
	inflights atomic.Int32 // Quantity of product currently being generated.
	limit     atomic.Int32 // Concurrent limit, from the config. 0 means no limit.

	stats SolverStats
}

func NewBaseSolver(name string, key string, weight uint8) BaseSolver {
	return BaseSolver{name: name, key: key, weight: weight}
}

func (b *BaseSolver) GetName() string            { return b.name }
func (b *BaseSolver) GetKey() string             { return b.key }
func (b *BaseSolver) GetWeight() int             { return int(b.weight) }
func (b *BaseSolver) GetStatistic() *SolverStats { return &b.stats }
func (b *BaseSolver) GetInflights() int          { return int(b.inflights.Load()) }

func (b *BaseSolver) GetLimit() int      { return int(b.limit.Load()) }
func (b *BaseSolver) SetLimit(limit int) { b.limit.Store(int32(limit)) }

func (b *BaseSolver) AddInflights() { b.inflights.Add(+1) }
func (b *BaseSolver) SubInflights() {
	for {
		n := b.inflights.Load()
		if n <= 0 || b.inflights.CompareAndSwap(n, n-1) {
			return
		}
	}
}

// registry maps each key to the solvers that handle it.
var registry = map[string][]SolverInterface[any]{}

package main

import (
	"math"
	"math/rand/v2"
	"slices"
	"time"
)

// TODO: Essas métricas precisam vir de um sistema externo.
const (
	priorSamples   = 2.0  // fake successes and failures added to every rate
	minShare       = 0.05 // minimum traffic share, so a bad Solver keeps being tested
	maxShare       = 0.90 // maximum traffic share, so no Solver takes everything
	minSpeed       = 0.5
	maxSpeed       = 2.0
	solverCapacity = 10.0 // busy count at which the load factor halves, for a Solver without limit
	neutralScore   = 0.25 // score with no samples: 0.5 × 0.5
)

// PickSolver orders the solvers by draws without replacement, proportional to their adaptive score.
// Solvers with weight 0 are disabled, unless all are. states, when known, is the global state of
// each solver, aligned with solvers.
func PickSolver(solvers []SolverInterface[any], states []solverState) []SolverInterface[any] {
	var pool []SolverInterface[any]
	var busy []int
	for i, s := range solvers {
		if s.GetWeight() > 0 {
			pool = append(pool, s)
			n := s.GetInflights()
			if i < len(states) {
				n = states[i].busy
			}
			busy = append(busy, n)
		}
	}
	if len(pool) == 0 {
		return slices.Clone(solvers)
	}

	now := time.Now()
	counters := make([]solverCounters, len(pool))
	var poolElapsed, poolSuccess float64
	for i, s := range pool {
		counters[i] = s.GetStatistic().Snapshot(now)
		poolElapsed += counters[i].genElapsed
		poolSuccess += counters[i].genSuccess
	}
	poolLatency := 0.0
	if poolSuccess > 0 {
		poolLatency = poolElapsed / poolSuccess
	}

	scores := make([]float64, len(pool))
	for i, s := range pool {
		scores[i] = solverScore(counters[i], poolLatency, usage(s, busy[i])) * float64(s.GetWeight())
	}
	return drawOrder(pool, boundedShares(scores, minShare, maxShare))
}

// usage is the share of the solver's capacity in use: busy over its limit, or over solverCapacity
// when it has none.
func usage(s SolverInterface[any], busy int) float64 {
	capacity := float64(s.GetLimit())
	if capacity <= 0 {
		capacity = solverCapacity
	}
	return float64(busy) / capacity
}

// drawOrder orders solvers by successive draws without replacement, each proportional to its weight.
func drawOrder(solvers []SolverInterface[any], weights []float64) []SolverInterface[any] {
	left, w := slices.Clone(solvers), slices.Clone(weights)
	out := make([]SolverInterface[any], 0, len(left))
	for len(left) > 0 {
		total := 0.0
		for _, x := range w {
			total += x
		}
		i, n := len(left)-1, rand.Float64()*total
		for j, x := range w {
			if n -= x; n < 0 {
				i = j
				break
			}
		}
		out = append(out, left[i])
		left, w = slices.Delete(left, i, i+1), slices.Delete(w, i, i+1)
	}
	return out
}

// solverScore is usage quality × generation availability² × speed × load. The availability
// counts twice, so a fast Solver that fails often does not outscore a healthy slower one.
func solverScore(c solverCounters, poolLatency float64, used float64) float64 {
	quality := (c.usageSuccess + priorSamples) / (c.usageSuccess + c.usageFailure + 2*priorSamples)
	availability := (c.genSuccess + priorSamples) / (c.genSuccess + c.genFailure + 2*priorSamples)

	// Latency pulled toward the pool average when there are few samples.
	speed := 1.0
	if poolLatency > 0 {
		latency := (c.genElapsed + priorSamples*poolLatency) / (c.genSuccess + priorSamples)
		speed = min(max(poolLatency/latency, minSpeed), maxSpeed)
	}
	load := 1 / (1 + used)

	score := quality * availability * availability * speed * load
	if math.IsNaN(score) || math.IsInf(score, 0) || score <= 0 {
		return neutralScore
	}
	return score
}

// boundedShares returns clamp(λ·score, lo, hi) for each score, with λ chosen so the shares sum to 1.
func boundedShares(scores []float64, lo, hi float64) []float64 {
	n := float64(len(scores))
	lo, hi = min(lo, 1/n), max(hi, 1/n)

	shares := make([]float64, len(scores))
	fill := func(lambda float64) float64 {
		total := 0.0
		for i, s := range scores {
			shares[i] = min(max(lambda*max(s, 1e-12), lo), hi)
			total += shares[i]
		}
		return total
	}

	// λ = 0 puts everyone on the floor (sum <= 1). Grow until the sum reaches 1, then bisect.
	low, high := 0.0, 1.0
	for fill(high) < 1 {
		high *= 2
	}
	for range 100 {
		mid := (low + high) / 2
		if fill(mid) < 1 {
			low = mid
		} else {
			high = mid
		}
	}
	fill(high)
	return shares
}

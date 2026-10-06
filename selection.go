package main

import (
	"math"
	"math/rand/v2"
	"time"
)

const (
	priorSamples   = 2.0  // fake successes and failures added to every rate
	minShare       = 0.05 // minimum traffic share, so a bad solver keeps being tested
	maxShare       = 0.90 // maximum traffic share, so no solver takes everything
	minSpeed       = 0.5
	maxSpeed       = 2.0
	solverCapacity = 10.0 // inflights at which the load factor halves
	neutralScore   = 0.25 // score with no samples: 0.5 × 0.5
)

// PickSolver draws a solver by its adaptive score. Solvers with weight 0 are disabled.
func PickSolver(solvers []SolverInterface[any]) SolverInterface[any] {
	pool := make([]SolverInterface[any], 0, len(solvers))
	for _, s := range solvers {
		if s.GetWeight() > 0 {
			pool = append(pool, s)
		}
	}
	if len(pool) == 0 {
		if len(solvers) == 0 {
			return nil
		}
		return solvers[0]
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
		scores[i] = solverScore(counters[i], poolLatency, s.GetInflights()) * float64(s.GetWeight())
	}

	shares := boundedShares(scores, minShare, maxShare)
	total := 0.0
	for _, w := range shares {
		total += w
	}
	n := rand.Float64() * total
	for i, w := range shares {
		n -= w
		if n < 0 {
			return pool[i]
		}
	}
	return pool[len(pool)-1]
}

// solverScore is usage quality × generation availability × speed × load.
func solverScore(c solverCounters, poolLatency float64, inflights int) float64 {
	quality := (c.usageSuccess + priorSamples) / (c.usageSuccess + c.usageFailure + 2*priorSamples)
	availability := (c.genSuccess + priorSamples) / (c.genSuccess + c.genFailure + 2*priorSamples)

	// Latency pulled toward the pool average when there are few samples.
	speed := 1.0
	if poolLatency > 0 {
		latency := (c.genElapsed + priorSamples*poolLatency) / (c.genSuccess + priorSamples)
		speed = min(max(poolLatency/latency, minSpeed), maxSpeed)
	}
	load := 1 / (1 + float64(inflights)/solverCapacity)

	score := quality * availability * speed * load
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

package main

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	halfLife   = 30 * time.Minute // solver samples lose half their weight every halfLife
	minDecayed = 0.01             // decayed counters below this are zeroed
)

// TODO: This project will run on AWS with more than one task behind a load balancer.
// Should this data live in a persistent Redis instead of a local variable?
// SolverStats holds the decayed counters of one solver.
type SolverStats struct {
	mu        sync.Mutex
	updatedAt time.Time

	counters solverCounters
}

type solverCounters struct {
	genSuccess   float64 // solver delivered a product
	genFailure   float64 // solver failed to deliver a product
	usageSuccess float64 // product worked when used
	usageFailure float64 // product was rejected when used
	genElapsed   float64 // seconds summed over successful generations
}

// decay ages the counters to now. Caller must hold the lock.
func (s *SolverStats) decay(now time.Time) {
	if !now.After(s.updatedAt) {
		return
	}
	if !s.updatedAt.IsZero() {
		f := math.Pow(0.5, now.Sub(s.updatedAt).Seconds()/halfLife.Seconds())
		c := &s.counters
		for _, v := range []*float64{&c.genSuccess, &c.genFailure, &c.usageSuccess, &c.usageFailure, &c.genElapsed} {
			if *v *= f; *v < minDecayed {
				*v = 0
			}
		}
		if c.genSuccess == 0 {
			c.genElapsed = 0
		}
	}
	s.updatedAt = now
}

func (s *SolverStats) AddGeneration(now time.Time, elapsed time.Duration, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.decay(now)
	if err != nil {
		s.counters.genFailure++
		return
	}
	s.counters.genSuccess++
	s.counters.genElapsed += elapsed.Seconds()
}

func (s *SolverStats) AddUsage(now time.Time, success bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.decay(now)
	if success {
		s.counters.usageSuccess++
	} else {
		s.counters.usageFailure++
	}
}

// Snapshot returns the counters decayed to now.
func (s *SolverStats) Snapshot(now time.Time) solverCounters {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.decay(now)
	return s.counters
}

// KeyStats holds the global request counter state of one key.
type KeyStats struct {
	mu sync.Mutex

	failedIncr atomic.Int64 // global increments that failed to reach Redis

	lastCount int64     // global counter at the last measure, guarded by mu
	lastAt    time.Time // when lastCount was measured
}

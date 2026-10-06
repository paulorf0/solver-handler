package main

import (
	"math"
	"sync"
	"time"
)

const (
	bucketSize  = time.Minute
	bucketCount = 120 // 2 hours of history

	shortWindow  = 15 * time.Minute
	mediumWindow = time.Hour
	longWindow   = 2 * time.Hour

	halfLife   = 30 * time.Minute // solver samples lose half their weight every halfLife
	minDecayed = 0.01             // decayed counters below this are zeroed
)

// ring is a circular buffer of time buckets of bucketSize each.
type ring[T any] struct {
	starts [bucketCount]time.Time
	data   [bucketCount]T
}

// at returns the bucket of t, resetting it if it still holds an older period.
func (r *ring[T]) at(t time.Time) *T {
	start := t.Truncate(bucketSize)
	i := int(start.Unix()/int64(bucketSize/time.Second)) % bucketCount
	if !r.starts[i].Equal(start) {
		var zero T
		r.starts[i] = start
		r.data[i] = zero
	}
	return &r.data[i]
}

// each calls fn for every bucket inside the window that ends at now.
func (r *ring[T]) each(now time.Time, window time.Duration, fn func(*T)) {
	from := now.Truncate(bucketSize).Add(-window)
	for i := range r.data {
		if r.starts[i].After(from) && !r.starts[i].After(now) {
			fn(&r.data[i])
		}
	}
}

// TODO: This project will run on AWS with more than one task behind a load balancer.
// Should this data live in a persistent Redis instead of a local variable?
// SolverStats holds the decayed counters of one solver.
type SolverStats struct {
	mu        sync.Mutex
	updatedAt time.Time

	counters solverCounters

	lastSuccessAt time.Time
	lastFailureAt time.Time
	lastErr       error
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
		s.lastFailureAt = now
		s.lastErr = err
		return
	}
	s.counters.genSuccess++
	s.counters.genElapsed += elapsed.Seconds()
	s.lastSuccessAt = now
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

type demandBucket struct {
	requests  int64
	stockHits int64 // requests served from the queue without solving
}

// KeyStats holds the demand measurements of one key, used to size the stock of products.
type KeyStats struct {
	mu      sync.Mutex
	buckets ring[demandBucket]
}

type KeySummary struct {
	requests      int64
	stockHits     int64
	ratePerMinute float64
}

func (k *KeyStats) AddRequest(at time.Time, stockHit bool) {
	k.mu.Lock()
	defer k.mu.Unlock()

	b := k.buckets.at(at)
	b.requests++
	if stockHit {
		b.stockHits++
	}
}

func (k *KeyStats) Window(now time.Time, window time.Duration) KeySummary {
	k.mu.Lock()
	defer k.mu.Unlock()

	var sum KeySummary
	k.buckets.each(now, window, func(b *demandBucket) {
		sum.requests += b.requests
		sum.stockHits += b.stockHits
	})
	sum.ratePerMinute = float64(sum.requests) / window.Minutes()
	return sum
}

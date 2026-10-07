package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	halfLife   = 30 * time.Minute // Solver samples lose half their weight every halfLife
	minDecayed = 0.01             // decayed counters below this are zeroed
)

// TODO: This project will run on AWS with more than one task behind a load balancer.
// Should this data live in a persistent Redis instead of a local variable?
// SolverStats holds the decayed counters of one Solver.
type SolverStats struct {
	mu        sync.Mutex
	updatedAt time.Time

	counters solverCounters
}

type solverCounters struct {
	genSuccess   float64 // Solver delivered a product
	genFailure   float64 // Solver failed to deliver a product
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

// KeyStats holds the global request counter state of one Key.
type KeyStats struct {
	mu sync.Mutex

	failedIncr atomic.Int64 // global increments that failed to reach Redis

	lastCount int64     // global counter At the last measure, guarded by mu
	lastAt    time.Time // when lastCount was measured

	rpm []float64 // last signalWindow samples taken by Signal, guarded by mu
}

// addRPM keeps the sample and, once the window is full, returns its lowest rpm and the trend.
// The trend is the mean of the last trendSamples over the mean of the window, capped at 1:
// below 1 means demand is falling.
func (k *KeyStats) addRPM(rpm float64) (floor, trend float64, full bool) {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.rpm = append(k.rpm, rpm)
	if len(k.rpm) > signalWindow { //
		k.rpm = k.rpm[1:]
	}
	if len(k.rpm) < signalWindow {
		return 0, 0, false
	}
	window := mean(k.rpm)
	if window == 0 {
		return 0, 0, true
	}
	trend = min(mean(k.rpm[len(k.rpm)-trendSamples:])/window, 1)
	return slices.Min(k.rpm), trend, true
}

func mean(v []float64) float64 {
	sum := 0.0
	for _, x := range v {
		sum += x
	}
	return sum / float64(len(v))
}

// countRequest adds 1 to the global request counter of Key in Redis, plus earlier failed increments.
func (b *BucketHandler) countRequest(key Key) {
	if b.redis == nil {
		return
	}
	s := b.stats[key]
	n := s.failedIncr.Swap(0) + 1
	if err := b.redis.IncrBy(context.Background(), requestsKey(key), n).Err(); err != nil {
		s.failedIncr.Add(n)
	}
}

// RequestRate returns the global requests per second of Key since the last measure.
// The first call only takes the measure and returns ok false. Only Signal should call it.
func (b *BucketHandler) RequestRate(ctx context.Context, key Key) (rate float64, ok bool, err error) {
	s, found := b.stats[key]
	if !found {
		return 0, false, fmt.Errorf("nenhuma estatística registrada com a chave fornecida")
	}
	total, err := b.redis.Get(ctx, requestsKey(key)).Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return 0, false, fmt.Errorf("falha ao ler o contador de requisições: %w", err)
	}
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.lastAt.IsZero() {
		rate, ok = float64(total-s.lastCount)/now.Sub(s.lastAt).Seconds(), true
	}
	s.lastCount, s.lastAt = total, now
	return rate, ok, nil
}

func requestsKey(key Key) string { return "requests:" + key.String() }

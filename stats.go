package main

import (
	"context"
	"errors"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	halfLife        = 30 * time.Minute // Solver samples lose half their weight every halfLife
	latencyHalfLife = 5 * time.Minute  // same for the latency samples of the timeout, which must follow a slowdown fast
	minDecayed      = 0.01             // decayed counters below this are zeroed
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
	genSuccess           float64 // Solver delivered a product
	genFailure           float64 // Solver failed to deliver a product
	usageSuccess         float64 // product worked when used
	usageFailure         float64 // product was rejected when used
	genElapsed           float64 // seconds summed over successful generations
	w                    float64 // total weight of the latency samples
	m2                   float64 // weighted sum of squared deviations, in seconds²
	meanElapsedGenSucess float64 // weighted mean latency, in seconds
}

// decay ages the counters to now. Caller must hold the lock.
func (s *SolverStats) decay(now time.Time) {
	if !now.After(s.updatedAt) {
		return
	}
	if !s.updatedAt.IsZero() {
		dt := now.Sub(s.updatedAt).Seconds()
		f := math.Pow(0.5, dt/halfLife.Seconds()) // (1/2)^(delta_t / halfLife)
		c := &s.counters
		for _, v := range []*float64{&c.genSuccess, &c.genFailure, &c.usageSuccess, &c.usageFailure, &c.genElapsed} {
			if *v *= f; *v < minDecayed {
				*v = 0
			}
		}
		if c.genSuccess == 0 {
			c.genElapsed = 0
		}
		fl := math.Pow(0.5, dt/latencyHalfLife.Seconds())
		if c.w *= fl; c.w < minDecayed {
			c.w = 0
		}
		if c.m2 *= fl; c.w == 0 {
			c.m2 = 0
		}
	}
	s.updatedAt = now
}

func (s *SolverStats) AddGeneration(now time.Time, elapsed time.Duration, info Info) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.decay(now)
	if info.err != nil {
		// A failure that is not the solver's fault says nothing about it.
		if info.solverFault() {
			s.counters.genFailure++
		}
		// A solve cut by its timeout took at least that long: left out, the timeout would only
		// learn from the solves that fit in it and never rise when the solver slows down.
		if errors.Is(info.err, context.DeadlineExceeded) {
			s.addLatency(elapsed)
		}
		return
	}

	s.counters.genSuccess++
	s.counters.genElapsed += elapsed.Seconds()
	s.addLatency(elapsed)
}

// addLatency adds one latency sample of the timeout, by weighted Welford with weight 1; decay
// scales w and m2 alike. Caller must hold the lock.
func (s *SolverStats) addLatency(elapsed time.Duration) {
	x := elapsed.Seconds()
	s.counters.w++
	d := x - s.counters.meanElapsedGenSucess
	s.counters.meanElapsedGenSucess += d / s.counters.w
	s.counters.m2 += d * (x - s.counters.meanElapsedGenSucess)
}

// deviation is the weighted standard deviation of the latency, in seconds. Caller must hold the lock.
func (s *SolverStats) deviation() float64 {
	if s.counters.w <= 0 {
		return 0
	}
	return math.Sqrt(s.counters.m2 / s.counters.w)
}

// TimeoutGen returns the solve timeout in seconds from the latency metrics:
//
//	t = mean + 4 × deviation, clamped to [min, max]
//
// mean and deviation weigh the samples by age (latencyHalfLife). A solve cut by the timeout
// counts as a sample of the timeout's length. With no samples it returns max.
func (s *SolverStats) TimeoutGen(max, min float64) float64 {
	return s.timeoutGenAt(time.Now(), max, min)
}

// timeoutGenAt is TimeoutGen with the metrics decayed to now.
func (s *SolverStats) timeoutGenAt(now time.Time, max, min float64) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.decay(now)
	if s.counters.w == 0 {
		return max
	}
	return clamp(s.counters.meanElapsedGenSucess+4*s.deviation(), max, min)
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

const (
	genWeight  = 0.05  // weight of each generation: latency and failure follow the last ~20
	baseWeight = 0.002 // weight on the latency baseline: follows the last ~500
)

// KeyStats holds the request counter state of one key in this task.
type KeyStats struct {
	failedIncr atomic.Int64 // global increments that failed to reach Redis
}

// redisKey names a Redis key of key. The hash tag puts every Redis key of one key in the same slot,
// so the stock scripts can touch several of them.
func redisKey(key string, name string) string { return "{" + key + "}:" + name }

// busyKey holds the generations in progress of one solver of key, from every task.
func busyKey(key string, solver string) string { return redisKey(key, "busy:"+solver) }

// stockKey holds the stock generations in progress of one solver of key, from every task.
func stockKey(key string, solver string) string { return redisKey(key, "stock:"+solver) }

// solverGenKey holds the generation metrics of one solver of key, from every task.
func solverGenKey(key string, solver string) string { return redisKey(key, "gen:"+solver) }

// countRequest adds 1 to the global request counter of key in Redis, plus earlier failed increments.
func (b *BucketHandler) countRequest(key string) {
	if b.redis == nil {
		return
	}
	s := b.stats[key]
	n := s.failedIncr.Swap(0) + 1
	if err := b.redis.IncrBy(context.Background(), redisKey(key, "requests"), n).Err(); err != nil {
		s.failedIncr.Add(n)
	}
}

// recordGenScript updates the moving latency mean, variance and baseline (successes only) and the
// moving failure ratio of the key's generations, in the key's metrics and in the solver's. The baseline
// stays frozen while the latency is above it × ARGV[5], so it does not learn a slowdown.
// ARGV[1] is the latency in ms, -1 on failure. KEYS: gen, gen of the solver (same hash tag).
var recordGenScript = redis.NewScript(`
local x, w, lf = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[5])
for k = 1, 2 do
	local h = redis.call('HMGET', KEYS[k], 'mean', 'var', 'base', 'fail')
	local fail = tonumber(h[4]) or 0
	if x < 0 then
		fail = fail + w * (1 - fail)
	else
		fail = fail - w * fail
		local mean, var, base = tonumber(h[1]), tonumber(h[2]), tonumber(h[3])
		if not mean then
			mean, var, base = x, 0, x
		else
			local d = x - mean
			mean = mean + w * d
			var = (1 - w) * (var + w * d * d)
			if not (lf > 0 and mean > base * lf) then base = base + tonumber(ARGV[3]) * (x - base) end
		end
		redis.call('HSET', KEYS[k], 'mean', tostring(mean), 'var', tostring(var), 'base', tostring(base))
	end
	redis.call('HSET', KEYS[k], 'fail', tostring(fail))
	redis.call('PEXPIRE', KEYS[k], ARGV[4])
end
return 1
`)

// The metrics below are best effort: a Redis error only loses one sample.

// recordGeneration adds one generation of key by solver, from the stock or solved now, to the
// global metrics.
func (b *BucketHandler) recordGeneration(key string, solver string, elapsed time.Duration, err bool) {
	if b.redis == nil {
		return
	}
	ms := min(elapsed, genLife).Milliseconds()
	if err {
		ms = -1
	}
	recordGenScript.Run(context.Background(), b.redis, []string{redisKey(key, "gen"), solverGenKey(key, solver)},
		ms, genWeight, baseWeight, metricsTTL.Milliseconds(), b.cfg().Stock[key].LatencyFactor)
}

// pickNowScript marks busy the first solver of the preference order with room, or the first one
// when all are full, and returns its position (1-based).
// KEYS: busy of each solver, in preference order (same hash tag). ARGV: 5.. limit of each solver.
var pickNowScript = redis.NewScript(`
local pick = 1
for i = 1, #KEYS do
	redis.call('ZREMRANGEBYSCORE', KEYS[i], '-inf', ARGV[1])
	local lim = tonumber(ARGV[4 + i])
	if lim <= 0 or redis.call('ZCARD', KEYS[i]) < lim then
		pick = i
		break
	end
end
redis.call('ZADD', KEYS[pick], ARGV[2], ARGV[3])
redis.call('PEXPIRE', KEYS[pick], ARGV[4])
return pick
`)

// startNow picks the solver of a generation solved now: the first of order with room, or the
// first one when all are full, and marks it busy. It never blocks: the limits only hold back the
// stock. It returns the solver and the mark that finishNow removes.
func (b *BucketHandler) startNow(key string, order []SolverInterface[any]) (SolverInterface[any], string) {
	member := b.id + ":now:" + strconv.FormatInt(b.seq.Add(1), 10)
	if b.redis == nil {
		return order[0], member
	}
	now := time.Now()
	redisKeys := make([]string, len(order))
	args := []any{now.UnixMilli(), now.Add(genLife).UnixMilli(), member, (2 * genLife).Milliseconds()}
	for i, s := range order {
		redisKeys[i] = busyKey(key, s.GetName())
		args = append(args, s.GetLimit())
	}
	i, err := pickNowScript.Run(context.Background(), b.redis, redisKeys, args...).Int()
	if err != nil || i < 1 || i > len(order) {
		return order[0], member
	}
	return order[i-1], member
}

// finishNow removes the mark of a generation solved now.
func (b *BucketHandler) finishNow(key string, solver, member string) {
	if b.redis != nil {
		b.redis.ZRem(context.Background(), busyKey(key, solver), member)
	}
}

func clamp(val, max, min float64) float64 {
	return math.Min(math.Max(val, min), max)
}

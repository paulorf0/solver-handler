package main

import (
	"context"
	"errors"
	"fmt"
	log "log/slog"
	"math"
	"slices"
	"strconv"
	"time"

	"solver-handler/external"

	"github.com/redis/go-redis/v9"
)

const (
	stockTick       = time.Second      // refill period of every task, also the brake window
	defaultCoverage = 2 * time.Second  // coverage while the generation time is not measured
	genLife         = 30 * time.Second // an inflight mark lasts this long if its task dies mid generation
	latencySigmas   = 2
	rateTauUp       = 2 * time.Second // rate memory when demand rises: follows spikes fast
	rateTauDown     = 2 * time.Second // rate memory when demand falls: less stock left to expire
	metricsTTL      = 10 * time.Minute
	breakerCut      = 0.25 // share of the ceiling left when the breaker trips
)

// brakeFields are the fields of the brake hash, in the order stockTick reads them.
var brakeFields = []string{"limit", "cover", "limited", "used", "usedAt", "satSince", "coolUntil", "made", "expired", "waste"}

const (
	fLimit     = iota // stock limit, generations/s
	fCover            // coverage factor, shrunk by waste
	fLimited          // last window cut by the limit
	fUsed             // orders reserved in window usedAt
	fUsedAt           //
	fSatSince         // when saturation at the ceiling with failing generation began, ms
	fCoolUntil        // the limit cannot grow until then, ms
	fMade             // made counter at the last window
	fExpired          // session manager's expired counter at the last window
	fWaste            // moving share of the generations that expired
)

// reserveScript counts what is being generated and reserves what is missing in one step, so two
// tasks never reserve the same gap. Each order takes a free slot of its picked solver or is
// skipped. A cut by the limit marks the window as saturated. Returns the solver index (1-based) of
// each reserved order.
// KEYS: inflight, brake, busy of each solver (same hash tag).
// ARGV: mark expiry, now, missing, member prefix, mark TTL, limit, window, limit of each solver
// (0 = no limit), then the picked solver index of each order.
var reserveScript = redis.NewScript(`
local ns = #KEYS - 2
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[2])
local want = math.min(tonumber(ARGV[3]) - redis.call('ZCARD', KEYS[1]), #ARGV - 7 - ns)
local n = math.max(0, math.min(want, tonumber(ARGV[6])))
if n < want then redis.call('HSET', KEYS[2], 'limited', ARGV[7]) end
local free, got = {}, {}
for i = 1, ns do
	redis.call('ZREMRANGEBYSCORE', KEYS[2 + i], '-inf', ARGV[2])
	local lim = tonumber(ARGV[7 + i])
	free[i] = lim > 0 and lim - redis.call('ZCARD', KEYS[2 + i]) or n
end
for p = 8 + ns, #ARGV do
	if #got == n then break end
	local i = tonumber(ARGV[p])
	if free[i] > 0 then
		free[i] = free[i] - 1
		local m = ARGV[4] .. (#got + 1)
		redis.call('ZADD', KEYS[1], ARGV[1], m)
		redis.call('ZADD', KEYS[2 + i], ARGV[1], m)
		got[#got + 1] = i
	end
end
redis.call('PEXPIRE', KEYS[1], ARGV[5])
for i = 1, ns do redis.call('PEXPIRE', KEYS[2 + i], ARGV[5]) end
redis.call('HSET', KEYS[2], 'used', #got, 'usedAt', ARGV[7])
return got
`)

// solverState is the global state of one solver of a key, refreshed by every stock tick.
type solverState struct {
	busy int  // generations in progress, from every task
	sick bool // failing or slow against its own baseline
}

// solverStates returns the last known state of each solver of key, nil before the first tick.
func (b *BucketHandler) solverStates(key Key) []solverState {
	if v, ok := b.states.Load(key); ok {
		return v.([]solverState)
	}
	return nil
}

// RunStock refills the stock of every key once per stockTick until ctx ends. Every task runs it.
func (b *BucketHandler) RunStock(ctx context.Context) {
	ticker := time.NewTicker(stockTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		tickCtx, cancel := context.WithTimeout(ctx, stockTick)
		for key := range b.stats {
			if err := b.stockTick(tickCtx, key); err != nil {
				log.Warn("falha no refill do estoque", "key", key.String(), "err", err)
			}
		}
		cancel()
	}
}

// stockTick refreshes the solver states and, when this task leads the window (the first one to
// claim it), generates what the stock of key misses:
//
//	target = ceil(rate × coverage)
//	order  = target − pool − inflight, capped by the brake and the solver limits
//
// Only the leader writes the rate and brake state, once per window, so it needs no script.
func (b *BucketHandler) stockTick(ctx context.Context, key Key) error {
	cfg, ok := b.config.Stock[key]
	solvers := b.solvers[key]
	if !ok || cfg.MaxPerSecond <= 0 || len(solvers) == 0 {
		return nil
	}
	now := time.Now()
	nowMs := float64(now.UnixMilli())
	window := now.UnixMilli() / stockTick.Milliseconds()
	rateKey, brakeKey, genKey := redisKey(key, "rate"), redisKey(key, "brake"), redisKey(key, "gen")

	// One round trip for every task: the window lead, the solver states and what the leader needs.
	pipe := b.redis.Pipeline()
	lead := pipe.SetNX(ctx, redisKey(key, "leader:"+strconv.FormatInt(window, 10)), 1, 2*stockTick)
	busy := make([]*redis.IntCmd, len(solvers))
	health := make([]*redis.SliceCmd, len(solvers))
	for i, s := range solvers {
		busy[i] = pipe.ZCount(ctx, busyKey(key, s.GetName()), strconv.FormatInt(now.UnixMilli(), 10), "+inf")
		health[i] = pipe.HMGet(ctx, solverGenKey(key, s.GetName()), "mean", "base", "fail")
	}
	rateCmd := pipe.HMGet(ctx, rateKey, "rate", "total", "at")
	totalCmd := pipe.Get(ctx, redisKey(key, "requests"))
	brakeCmd := pipe.HMGet(ctx, brakeKey, brakeFields...)
	genCmd := pipe.HMGet(ctx, genKey, "mean", "var", "fail", "made")
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("falha ao ler a janela: %w", err)
	}

	states := make([]solverState, len(solvers))
	for i := range states {
		h := hvals(health[i])
		states[i] = solverState{busy: int(busy[i].Val()), sick: sick(cfg, h[0], h[1], h[2])}
	}
	b.states.Store(key, states)
	if !lead.Val() {
		return nil
	}

	r, k, g := hvals(rateCmd), hvals(brakeCmd), hvals(genCmd)
	total, _ := totalCmd.Float64()
	rate, saveRate := nextRate(orElse(r[0], 0), r[1], r[2], total, nowMs)
	limit, satSince, coolUntil, tripped := nextLimit(cfg, k, rate, orElse(g[2], 0), nowMs, window)
	if tripped {
		log.Warn("disjuntor do estoque aberto", "key", key.String(), "limite", limit)
	}

	pool, err := b.sm.Len(ctx, key)
	if err != nil {
		return fmt.Errorf("falha ao ler o tamanho do pool: %w", err)
	}
	expired, err := b.sm.Expired(ctx, key)
	if err != nil {
		return fmt.Errorf("falha ao ler os vencidos do pool: %w", err)
	}
	made := orElse(g[3], 0)
	cover, waste := nextCover(cfg, k, made, float64(expired))
	coverage := b.coverage(key, orElse(g[0], -1), orElse(g[1], 0), cover)
	target := int(math.Ceil(rate * coverage.Seconds()))
	log.Debug("refill do estoque", "key", key.String(), "rate", rate, "coverage", coverage,
		"target", target, "pool", pool, "limit", limit)
	missing := target - pool

	pipe = b.redis.Pipeline()
	if saveRate {
		pipe.HSet(ctx, rateKey, "rate", rate, "total", total, "at", nowMs)
		pipe.PExpire(ctx, rateKey, metricsTTL)
	}
	pipe.HSet(ctx, brakeKey, "limit", limit, "cover", cover, "satSince", satSince, "coolUntil", coolUntil,
		"made", made, "expired", expired, "waste", waste)
	pipe.PExpire(ctx, brakeKey, metricsTTL)
	prefix := b.id + ":" + strconv.FormatInt(b.seq.Add(1), 10) + ":"
	var reserve *redis.Cmd
	if picks := b.pickStock(key, solvers, states, missing); len(picks) > 0 {
		redisKeys := []string{redisKey(key, "inflight"), brakeKey}
		args := []any{now.Add(genLife).UnixMilli(), now.UnixMilli(), missing, prefix, (2 * genLife).Milliseconds(),
			int(math.Ceil(limit * stockTick.Seconds())), window}
		for _, s := range solvers {
			redisKeys = append(redisKeys, busyKey(key, s.GetName()))
			args = append(args, s.GetLimit())
		}
		for _, i := range picks {
			args = append(args, i+1)
		}
		// Eval, not Run: a pipeline cannot fall back from EVALSHA on NOSCRIPT.
		reserve = reserveScript.Eval(ctx, pipe, redisKeys, args...)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("falha ao reservar a geração: %w", err)
	}
	if reserve == nil {
		return nil
	}
	got, err := reserve.Int64Slice()
	if err != nil {
		return fmt.Errorf("falha ao reservar a geração: %w", err)
	}
	for i, idx := range got {
		go b.generate(key, solvers[idx-1], prefix+strconv.Itoa(i+1))
	}
	return nil
}

// hvals parses the values of an HMGET, NaN where missing.
func hvals(cmd *redis.SliceCmd) []float64 {
	vals := cmd.Val()
	out := make([]float64, len(vals))
	for i, v := range vals {
		out[i] = math.NaN()
		if s, ok := v.(string); ok {
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				out[i] = f
			}
		}
	}
	return out
}

// orElse returns v, or def when v is missing (NaN).
func orElse(v, def float64) float64 {
	if math.IsNaN(v) {
		return def
	}
	return v
}

// sick tells whether a solver fails too often or is slow against its own baseline, per cfg.
func sick(cfg external.StockConfig, mean, base, fail float64) bool {
	return cfg.MaxFailRatio > 0 && fail > cfg.MaxFailRatio ||
		cfg.LatencyFactor > 0 && base > 0 && mean > base*cfg.LatencyFactor
}

// nextRate moves the requests/s EMA toward the rate since the last sample, at least 1s old, with
// a shorter memory on rises. It reports whether the state must be saved.
func nextRate(rate, prev, at, total, now float64) (float64, bool) {
	if math.IsNaN(prev) || total < prev {
		return rate, true // first sample, or the counter was reset
	}
	if now-at < 1000 {
		return rate, false
	}
	sample := (total - prev) * 1000 / (now - at)
	tau := rateTauDown
	if sample > rate {
		tau = rateTauUp
	}
	return rate + (1-math.Exp(-(now-at)/float64(tau.Milliseconds())))*(sample-rate), true
}

// nextLimit moves the stock limit (generations/s) of a window: ×1.5 after a saturated window,
// unless cooling down after a trip; otherwise toward 2× the last use. Never below the rate nor
// above the ceiling. Saturated at the ceiling with failing generation for SaturatedFor trips the
// breaker. k holds the brake fields.
func nextLimit(cfg external.StockConfig, k []float64, rate, fail, now float64, window int64) (limit, satSince, coolUntil float64, tripped bool) {
	maxps := cfg.MaxPerSecond
	limit, satSince, coolUntil = orElse(k[fLimit], maxps), orElse(k[fSatSince], 0), orElse(k[fCoolUntil], 0)
	last := float64(window - 1)
	saturated := k[fLimited] == last
	used := 0.0
	if k[fUsedAt] == last {
		used = k[fUsed] / stockTick.Seconds()
	}
	if saturated {
		if now >= coolUntil {
			limit *= 1.5
		}
	} else {
		limit += 0.1 * (2*used - limit)
	}
	floor := max(1, min(rate, maxps))
	limit = max(floor, min(limit, maxps))

	if cfg.SaturatedFor <= 0 || !saturated || limit < maxps || cfg.MaxFailRatio <= 0 || fail <= cfg.MaxFailRatio {
		return limit, 0, coolUntil, false
	}
	if satSince == 0 {
		satSince = now
	}
	if now-satSince < float64(cfg.SaturatedFor*1000) {
		return limit, satSince, coolUntil, false
	}
	return max(floor, maxps*breakerCut), 0, now + float64(cfg.Cooldown*1000), true
}

// nextCover updates the waste (products expired over products made since the last window, from
// the session manager's counter) and the coverage factor it shrinks. k holds the brake fields.
func nextCover(cfg external.StockConfig, k []float64, made, expired float64) (cover, waste float64) {
	cover, waste = orElse(k[fCover], 1), orElse(k[fWaste], 0)
	if !math.IsNaN(k[fMade]) {
		dexp, dmade := max(0, expired-orElse(k[fExpired], expired)), max(0, made-k[fMade])
		if dexp > 0 || dmade > 0 {
			waste += 0.1 * (min(1, dexp/max(dmade, 1)) - waste)
		}
	}
	if cfg.MaxWaste > 0 && waste > cfg.MaxWaste {
		return max(0.2, cover*0.9), waste
	}
	return min(1, cover*1.05), waste
}

// pickStock picks the solver (index) of up to n stock orders, by preference among the solvers
// with room: healthy ones first, a sick one only when no healthy one has room. Each sick solver
// with room also gets one probe order, so it is measured again.
func (b *BucketHandler) pickStock(key Key, solvers []SolverInterface[any], states []solverState, n int) []int {
	free := make([]int, len(solvers))
	for i, s := range solvers {
		free[i] = n
		if l := s.GetLimit(); l > 0 {
			free[i] = l - states[i].busy
		}
	}
	var picks []int
	for i := range solvers {
		if states[i].sick && free[i] > 0 && len(picks) < n {
			picks = append(picks, i)
			free[i]--
		}
	}
	first := func(order []SolverInterface[any], sickToo bool) int {
		for _, s := range order {
			if i := slices.Index(solvers, s); free[i] > 0 && (sickToo || !states[i].sick) {
				return i
			}
		}
		return -1
	}
	for len(picks) < n {
		order, _ := b.TryGetSolver(key)
		i := first(order, false)
		if i < 0 {
			i = first(order, true)
		}
		if i < 0 {
			break
		}
		picks = append(picks, i)
		free[i]--
	}
	return picks
}

// coverage is mean + 2σ of the generation time plus the tick (defaultCoverage while unmeasured),
// times the waste factor, capped at the product TTL.
func (b *BucketHandler) coverage(key Key, mean, variance, factor float64) time.Duration {
	cover := defaultCoverage
	if mean >= 0 {
		ms := mean + latencySigmas*math.Sqrt(max(variance, 0))
		cover = time.Duration(ms*float64(time.Millisecond)) + stockTick
	}
	cover = time.Duration(float64(cover) * factor)
	if ttl, ok := b.config.TTLs[key]; ok {
		cover = min(cover, ttl)
	}
	return cover
}

// generate solves one product for the stock with solver and saves it. The marks leave only after
// the save, so meanwhile it counts twice: it errs on generating less, not more.
func (b *BucketHandler) generate(key Key, solver SolverInterface[any], member string) {
	ctx := context.Background()
	defer b.redis.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.ZRem(ctx, redisKey(key, "inflight"), member)
		p.ZRem(ctx, busyKey(key, solver.GetName()), member)
		return nil
	})

	prod, err := b.solve(key, solver, nil)
	if err != nil {
		log.Warn("falha ao gerar produto para o estoque", "key", key.String(), "err", err)
		return
	}
	if _, err := b.sm.Save(ctx, key, prod); err != nil {
		log.Warn("falha ao salvar produto no estoque", "key", key.String(), "err", err)
		return
	}
	b.redis.HIncrBy(ctx, redisKey(key, "gen"), "made", 1)
}

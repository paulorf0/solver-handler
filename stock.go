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
	stockTick       = time.Second       // refill period of every task, also the brake window
	ordersTTL       = 3 * stockTick / 2 // queued orders lapse if no leader renews them
	defaultCoverage = 2 * time.Second   // coverage while the generation time is not measured
	genLife         = 30 * time.Second  // an inflight mark lasts this long if its task dies mid generation
	latencySigmas   = 2
	rateTauUp       = 2 * time.Second // rate memory when demand rises: follows spikes fast
	rateTauDown     = 2 * time.Second // rate memory when demand falls: less stock left to expire
	metricsTTL      = 10 * time.Minute
	breakerCut      = 0.25 // share of the ceiling left when the breaker trips

	// The stock follows the demand only after requests arrived in each of the last sustainBins
	// intervals of sustainBin (40s): one burst must not fill a stock that would expire unused.
	sustainBin  = 5 * time.Second
	sustainBins = 8
)

// brakeFields are the fields of the brake hash, in the order stockTick reads them.
var brakeFields = []string{"limit", "cover", "limited", "used", "usedAt", "satSince", "coolUntil", "made", "expired", "waste"}

const (
	fLimit     = iota // stock limit, generations/s
	fCover            // coverage factor, shrunk by waste
	fLimited          // last window cut by the limit
	fUsed             // generations planned in window usedAt
	fUsedAt           //
	fSatSince         // when saturation at the ceiling with failing generation began, ms
	fCoolUntil        // the limit cannot grow until then, ms
	fMade             // made counter at the last window
	fExpired          // session manager's expired counter at the last window
	fWaste            // moving share of the generations that expired
)

// reserveScript plans the stock generations of a window in one step, so two tasks never plan the
// same gap: missing minus what is being generated, capped by the limit. Each planned generation
// takes a free stock slot of its picked solver now; what does not fit is queued in orders, and each
// stock generation that ends takes the next one. A cut by the limit marks the window as saturated.
// Returns the solver index (1-based) of each generation started now.
// KEYS: orders, brake, then busy and stock of each solver (same hash tag).
// ARGV: mark expiry, now, missing, member prefix, mark TTL, limit, window, orders TTL, then limit
// and stock cap of each solver (limit 0 = no limit), then the picked solver index of each order.
var reserveScript = redis.NewScript(`
local ns = (#KEYS - 2) / 2
local inflight, free = 0, {}
for i = 1, ns do
	local busy, stock = KEYS[1 + 2 * i], KEYS[2 + 2 * i]
	redis.call('ZREMRANGEBYSCORE', busy, '-inf', ARGV[2])
	redis.call('ZREMRANGEBYSCORE', stock, '-inf', ARGV[2])
	local s = redis.call('ZCARD', stock)
	inflight = inflight + s
	local lim, cap = tonumber(ARGV[7 + 2 * i]), tonumber(ARGV[8 + 2 * i])
	free[i] = lim > 0 and math.min(lim - redis.call('ZCARD', busy), cap - s) or math.huge
end
local want = tonumber(ARGV[3]) - inflight
local n = math.max(0, math.min(want, tonumber(ARGV[6])))
if n < want then redis.call('HSET', KEYS[2], 'limited', ARGV[7]) end
redis.call('HSET', KEYS[2], 'used', n, 'usedAt', ARGV[7])
local got = {}
for p = 9 + 2 * ns, #ARGV do
	if #got == n then break end
	local i = tonumber(ARGV[p])
	if free[i] > 0 then
		free[i] = free[i] - 1
		local m = ARGV[4] .. (#got + 1)
		redis.call('ZADD', KEYS[1 + 2 * i], ARGV[1], m)
		redis.call('ZADD', KEYS[2 + 2 * i], ARGV[1], m)
		got[#got + 1] = i
	end
end
for k = 3, #KEYS do redis.call('PEXPIRE', KEYS[k], ARGV[5]) end
redis.call('SET', KEYS[1], n - #got, 'PX', ARGV[8])
return got
`)

// nextOrderScript ends a stock generation: it frees its slot and, if an order is queued and the
// slot is still within the solver's limits, takes the order and keeps the slot under a new mark.
// Returns 1 when it took one.
// KEYS: orders, busy and stock of the solver (same hash tag).
// ARGV: old mark, new mark, now, mark expiry, mark TTL, limit (0 = no limit), stock cap.
var nextOrderScript = redis.NewScript(`
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
local lim = tonumber(ARGV[6])
if lim > 0 then
	redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[3])
	redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', ARGV[3])
	if redis.call('ZCARD', KEYS[2]) >= lim or redis.call('ZCARD', KEYS[3]) >= tonumber(ARGV[7]) then return 0 end
end
if tonumber(redis.call('GET', KEYS[1]) or '0') <= 0 then return 0 end
redis.call('DECR', KEYS[1])
redis.call('ZADD', KEYS[2], ARGV[4], ARGV[2])
redis.call('ZADD', KEYS[3], ARGV[4], ARGV[2])
redis.call('PEXPIRE', KEYS[2], ARGV[5])
redis.call('PEXPIRE', KEYS[3], ARGV[5])
return 1
`)

// solverState is the global state of one solver of a key, refreshed by every stock tick.
type solverState struct {
	busy  int  // generations in progress, from every task
	stock int  // of those, stock generations
	sick  bool // failing or slow against its own baseline
}

// solverStates returns the last known state of each solver of key, nil before the first tick.
func (b *BucketHandler) solverStates(key string) []solverState {
	if v, ok := b.states.Load(key); ok {
		return v.([]solverState)
	}
	return nil
}

// stockCap is how many generations of a solver the stock may hold: its limit minus the share kept
// for solving now, at least one slot when share is set.
func stockCap(limit int, share float64) int {
	if limit <= 0 || share <= 0 {
		return limit
	}
	return max(0, limit-max(1, int(math.Round(float64(limit)*share))))
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
				log.Warn("falha no refill do estoque", "key", key, "err", err)
			}
		}
		cancel()
	}
}

// stockTick refreshes the solver states and, when this task leads the window (the first one to
// claim it), plans the stock generations of the window:
//
//	target = ceil(rate × coverage), or 0 until the demand is sustained
//	plan   = target − pool − inflight, capped by the brake; started now on free stock slots,
//	         the rest queued for the stock generations that end
//
// When the demand falls below what the stock can sell before it expires (rate × TTL < pool +
// inflight), the stock stops and waits for sustained demand again.
// Only the leader writes the rate and brake state, once per window, so it needs no script.
func (b *BucketHandler) stockTick(ctx context.Context, key string) error {
	cfg, ok := b.cfg().Stock[key]
	solvers := b.solvers[key]
	if !ok || cfg.MaxPerSecond <= 0 || len(solvers) == 0 || b.sm == nil {
		return nil
	}
	now := time.Now()
	nowMs := float64(now.UnixMilli())
	nowArg := strconv.FormatInt(now.UnixMilli(), 10)
	window := now.UnixMilli() / stockTick.Milliseconds()
	rateKey, brakeKey, genKey := redisKey(key, "rate"), redisKey(key, "brake"), redisKey(key, "gen")
	snapsKey := redisKey(key, "snaps")

	// One round trip for every task: the window lead, the solver states and what the leader needs.
	pipe := b.redis.Pipeline()
	lead := pipe.SetNX(ctx, redisKey(key, "leader:"+strconv.FormatInt(window, 10)), 1, 2*stockTick)
	busy := make([]*redis.IntCmd, len(solvers))
	stock := make([]*redis.IntCmd, len(solvers))
	health := make([]*redis.SliceCmd, len(solvers))
	for i, s := range solvers {
		busy[i] = pipe.ZCount(ctx, busyKey(key, s.GetName()), nowArg, "+inf")
		stock[i] = pipe.ZCount(ctx, stockKey(key, s.GetName()), nowArg, "+inf")
		health[i] = pipe.HMGet(ctx, solverGenKey(key, s.GetName()), "mean", "base", "fail")
	}
	rateCmd := pipe.HMGet(ctx, rateKey, "rate", "total", "at", "bin")
	snapsCmd := pipe.LRange(ctx, snapsKey, 0, sustainBins)
	totalCmd := pipe.Get(ctx, redisKey(key, "requests"))
	brakeCmd := pipe.HMGet(ctx, brakeKey, brakeFields...)
	genCmd := pipe.HMGet(ctx, genKey, "mean", "var", "fail", "made")
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("falha ao ler a janela: %w", err)
	}

	states := make([]solverState, len(solvers))
	inflight := 0
	for i := range states {
		h := hvals(health[i])
		states[i] = solverState{busy: int(busy[i].Val()), stock: int(stock[i].Val()), sick: sick(cfg, h[0], h[1], h[2])}
		inflight += states[i].stock
	}
	b.states.Store(key, states)
	if !lead.Val() {
		return nil
	}

	r, k, g := hvals(rateCmd), hvals(brakeCmd), hvals(genCmd)
	total, _ := totalCmd.Float64()
	rate, saveRate := nextRate(orElse(r[0], 0), r[1], r[2], total, nowMs)

	// Request counter snapshots at the start of each sustainBin interval, newest first. A skipped
	// interval restarts them, since its requests are unknown.
	bin := now.Unix() / int64(sustainBin/time.Second)
	var snaps []float64
	for _, s := range snapsCmd.Val() {
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			snaps = append(snaps, f)
		}
	}
	newBin := r[3] != float64(bin)
	restart := newBin && r[3] != float64(bin-1)
	if restart {
		snaps = nil
	}
	if newBin {
		snaps = append([]float64{total}, snaps...)[:min(len(snaps)+1, sustainBins+1)]
	}

	limit, satSince, coolUntil, tripped := nextLimit(cfg, k, rate, orElse(g[2], 0), nowMs, window)
	if tripped {
		log.Warn("disjuntor do estoque aberto", "key", key, "limite", limit)
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

	sus := sustained(snaps)
	if ttl, ok := b.cfg().TTLs[key]; ok && sus && rate*ttl.Seconds() < float64(pool+inflight) {
		log.Warn("demanda abaixo do que o estoque vende antes de vencer, estoque suspenso", "key", key,
			"rate", rate, "pool", pool, "inflight", inflight)
		snaps, restart, sus = []float64{total}, true, false
	}
	target := 0
	if sus {
		target = int(math.Ceil(rate * coverage.Seconds()))
	}
	log.Debug("refill do estoque", "key", key, "rate", rate, "coverage", coverage,
		"target", target, "pool", pool, "inflight", inflight, "limit", limit, "sustained", sus)

	pipe = b.redis.Pipeline()
	if saveRate {
		pipe.HSet(ctx, rateKey, "rate", rate, "total", total, "at", nowMs)
		pipe.PExpire(ctx, rateKey, metricsTTL)
	}
	if newBin || restart {
		if restart {
			pipe.Del(ctx, snapsKey)
		}
		pipe.LPush(ctx, snapsKey, total)
		pipe.LTrim(ctx, snapsKey, 0, sustainBins)
		pipe.PExpire(ctx, snapsKey, metricsTTL)
		pipe.HSet(ctx, rateKey, "bin", bin)
		pipe.PExpire(ctx, rateKey, metricsTTL)
	}
	pipe.HSet(ctx, brakeKey, "limit", limit, "cover", cover, "satSince", satSince, "coolUntil", coolUntil,
		"made", made, "expired", expired, "waste", waste)
	pipe.PExpire(ctx, brakeKey, metricsTTL)

	// Always runs: with nothing missing it empties the queued orders.
	missing := target - pool
	prefix := b.id + ":" + strconv.FormatInt(b.seq.Add(1), 10) + ":"
	redisKeys := []string{redisKey(key, "orders"), brakeKey}
	args := []any{now.Add(genLife).UnixMilli(), now.UnixMilli(), missing, prefix, (2 * genLife).Milliseconds(),
		int(math.Ceil(limit * stockTick.Seconds())), window, ordersTTL.Milliseconds()}
	for _, s := range solvers {
		redisKeys = append(redisKeys, busyKey(key, s.GetName()), stockKey(key, s.GetName()))
		args = append(args, s.GetLimit(), stockCap(s.GetLimit(), cfg.DirectShare))
	}
	for _, i := range b.pickStock(key, solvers, states, missing-inflight, cfg.DirectShare) {
		args = append(args, i+1)
	}
	// Eval, not Run: a pipeline cannot fall back from EVALSHA on NOSCRIPT.
	reserve := reserveScript.Eval(ctx, pipe, redisKeys, args...)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("falha ao reservar a geração: %w", err)
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

// sustained tells whether requests arrived in each of the last sustainBins intervals, from the
// request counter snapshots taken at the start of each one (newest first).
func sustained(snaps []float64) bool {
	if len(snaps) <= sustainBins {
		return false
	}
	for i := range sustainBins {
		if snaps[i] <= snaps[i+1] {
			return false
		}
	}
	return true
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

// pickStock picks the solver (index) of up to n stock generations, by preference among the
// solvers with a free stock slot: healthy ones first, a sick one only when no healthy one has
// room. Each sick solver with room also gets one probe, so it is measured again.
func (b *BucketHandler) pickStock(key string, solvers []SolverInterface[any], states []solverState, n int, share float64) []int {
	free := make([]int, len(solvers))
	for i, s := range solvers {
		free[i] = n
		if l := s.GetLimit(); l > 0 {
			free[i] = min(l-states[i].busy, stockCap(l, share)-states[i].stock)
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
func (b *BucketHandler) coverage(key string, mean, variance, factor float64) time.Duration {
	cover := defaultCoverage
	if mean >= 0 {
		ms := mean + latencySigmas*math.Sqrt(max(variance, 0))
		cover = time.Duration(ms*float64(time.Millisecond)) + stockTick
	}
	cover = time.Duration(float64(cover) * factor)
	if ttl, ok := b.cfg().TTLs[key]; ok {
		cover = min(cover, ttl)
	}
	return cover
}

// generate runs stock generations with solver on one slot: after each one, it takes the next
// queued order on the same slot right away, without waiting for the next tick. It stops when no
// order is queued, on a failure, or when the solver turns sick; then the slot is freed.
func (b *BucketHandler) generate(key string, solver SolverInterface[any], member string) {
	ctx := context.Background()
	name := solver.GetName()
	defer func() {
		b.redis.Pipelined(ctx, func(p redis.Pipeliner) error {
			p.ZRem(ctx, busyKey(key, name), member)
			p.ZRem(ctx, stockKey(key, name), member)
			return nil
		})
	}()
	for b.stockOne(ctx, key, solver) && !b.isSick(key, solver) {
		next := b.id + ":" + strconv.FormatInt(b.seq.Add(1), 10) + ":c"
		now := time.Now()
		took, err := nextOrderScript.Run(ctx, b.redis,
			[]string{redisKey(key, "orders"), busyKey(key, name), stockKey(key, name)},
			member, next, now.UnixMilli(), now.Add(genLife).UnixMilli(), (2 * genLife).Milliseconds(),
			solver.GetLimit(), stockCap(solver.GetLimit(), b.cfg().Stock[key].DirectShare),
		).Int()
		if err != nil || took == 0 {
			return
		}
		member = next
	}
}

// stockOne solves one product for the stock with solver and saves it. It reports success.
func (b *BucketHandler) stockOne(ctx context.Context, key string, solver SolverInterface[any]) bool {
	var params Params
	if b.proxy != nil {
		proxy, err := b.stockProxy(ctx, key)
		if err != nil {
			log.Warn("falha ao obter proxy para o estoque", "key", key, "err", err)
			return false
		}
		params.Proxy = proxy
	}
	prod, err := b.solve(key, solver, params)
	if err != nil {
		if params.Proxy != "" && errors.Is(err, ErrProxy) {
			b.dropProxy(ctx, key, params.Proxy)
		}
		log.Warn("falha ao gerar produto para o estoque", "key", key, "err", err)
		return false
	}
	if _, err := b.sm.Save(ctx, key, prod); err != nil {
		log.Warn("falha ao salvar produto no estoque", "key", key, "err", err)
		return false
	}
	b.redis.HIncrBy(ctx, redisKey(key, "gen"), "made", 1)
	return true
}

// stockProxy returns the proxy the stock of key uses, asking the provider for one if there is none.
func (b *BucketHandler) stockProxy(ctx context.Context, key string) (string, error) {
	if p, ok := b.proxies.Load(key); ok {
		return p.(string), nil
	}
	info, err := b.proxy.Get(ctx, key)
	if err != nil {
		return "", err
	}
	if info.Proxy == "" {
		return "", fmt.Errorf("provedor devolveu proxy vazia para a chave %s", key)
	}
	p, _ := b.proxies.LoadOrStore(key, info.Proxy)
	return p.(string), nil
}

// dropProxy discards the failed proxy of key so the next solve gets a new one, and reports it.
// Only the first caller for that proxy reports, so concurrent failures report once.
func (b *BucketHandler) dropProxy(ctx context.Context, key string, proxy string) {
	if !b.proxies.CompareAndDelete(key, proxy) {
		return
	}
	if err := b.proxy.Report(ctx, key, proxy, false); err != nil {
		log.Warn("falha ao reportar proxy", "key", key, "err", err)
	}
}

// isSick tells whether solver was sick at the last stock tick.
func (b *BucketHandler) isSick(key string, solver SolverInterface[any]) bool {
	states := b.solverStates(key)
	i := slices.Index(b.solvers[key], solver)
	return i >= 0 && i < len(states) && states[i].sick
}

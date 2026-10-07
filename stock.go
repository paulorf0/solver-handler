package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	signalWindow    = 30               // rpm samples per key; with one Signal per minute, 30 minutes
	signalLockKey   = "signal:lock"    // Redis key of the Signal lock
	signalLease     = 3 * time.Minute  // lock expires if its holder stops calling Signal
	noTTLStockRange = 10 * time.Minute // demand covered by the stock of a key whose products do not expire
	trendSamples    = 5                // recent samples compared with the whole window
)

// signalLock takes the lock, or renews it if this task already holds it. Atomic in Redis.
var signalLock = redis.NewScript(`
local v = redis.call("GET", KEYS[1])
if v == ARGV[1] then
	redis.call("PEXPIRE", KEYS[1], ARGV[2])
	return 1
end
if not v then
	redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
	return 1
end
return 0`)

// Signal returns how many products each key should keep in stock. Call it once per minute.
// Only the task holding the Redis lock decides; the others get nil. The stock of a key is the
// lowest rpm of the last signalWindow samples times its TTL, so it is used before it expires
// and a key whose requests stopped in the window gets no stock. If demand is falling, the stock
// shrinks in proportion (trend).
func (b *BucketHandler) Signal(ctx context.Context) (map[Key]int, error) {
	held, err := signalLock.Run(ctx, b.redis, []string{signalLockKey}, b.id, signalLease.Milliseconds()).Int()
	if err != nil {
		return nil, fmt.Errorf("falha ao obter o lock do signal: %w", err)
	}
	if held == 0 {
		return nil, nil
	}

	targets := map[Key]int{}
	for key, s := range b.stats {
		rps, ok, err := b.RequestRate(ctx, key)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		floor, trend, full := s.addRPM(rps * 60)
		// Se a requisição zerar no período de 30 minutos, o refill para por completo.

		if !full || floor <= 0 {
			continue
		}
		ttl, ok := b.config.TTLs[key]
		if !ok {
			ttl = noTTLStockRange
		}
		// Falling demand shrinks the stock, so less of it is left to expire.
		if n := int(floor * ttl.Minutes() * trend); n > 0 {
			targets[key] = n
		}
	}
	return targets, nil // TODO: Essa conta de quantos estoque gerar pode ser melhor calculado.
}

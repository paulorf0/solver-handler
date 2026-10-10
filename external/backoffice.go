package external

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// BackOffice is the external system that holds the service config.
type BackOffice struct {
	*External[Config]
}

const configPath = "config"

type Config struct {
	Routes         Routes       `json:"routes"`
	TTLs           TTLs         `json:"ttls"`
	Stock          Stocks       `json:"stock"`
	SolverLimit    SolverLimits `json:"solverLimit"`
	AdaptiveChoice bool         `json:"adaptiveChoice"`
}

// SolverLimits maps each key to the concurrent limit of each of its solvers, by solver name.
// It comes from {"key": {"solver": limit}}. A missing solver or a zero limit means no limit.
type SolverLimits map[string]map[string]int32

// TTLs maps each key to how long its product lasts. It comes from {"key": seconds}.
// A key without TTL has products that do not expire.
type TTLs map[string]time.Duration

func (t *TTLs) UnmarshalJSON(data []byte) error {
	var raw map[string]int64
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("json de ttls inválido: %w", err)
	}
	out := make(TTLs, len(raw))
	for key, secs := range raw {
		out[key] = time.Duration(secs) * time.Second
	}
	*t = out
	return nil
}

// StockConfig is the brake of the stock of one key. A key without it has no stock.
// A zero threshold turns its rule off.
type StockConfig struct {
	MaxPerSecond  float64 `json:"maxPerSecond"`  // ceiling of stock generations/s, all tasks
	MaxFailRatio  float64 `json:"maxFailRatio"`  // failure ratio that halves the limit
	LatencyFactor float64 `json:"latencyFactor"` // latency over its baseline that halves the limit
	SaturatedFor  int64   `json:"saturatedFor"`  // seconds at the ceiling that trip the breaker
	Cooldown      int64   `json:"cooldown"`      // seconds the limit cannot grow after a trip
	MaxWaste      float64 `json:"maxWaste"`      // wasted share of the generations that shrinks the coverage
	DirectShare   float64 `json:"directShare"`   // share of each solver's limit kept for solving now (at least 1 slot)
}

// Stocks maps each key to its StockConfig. It comes from {"key": {...}}.
type Stocks map[string]StockConfig

// GetConfig fetches the config and decodes it into Config. RunConfig calls it periodically.
func (b *BackOffice) GetConfig(ctx context.Context) (Config, error) {
	return b.Get(ctx, configPath)
}

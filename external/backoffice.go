package external

import (
	"context"
	"encoding/json"
	"fmt"
	"solver-handler/keys"
	"time"
)

// BackOffice is the external system that holds the service config.
type BackOffice struct {
	*External[Config]
}

const configPath = "config"

type Config struct {
	Routes         Routes `json:"routes"`
	TTLs           TTLs   `json:"ttls"`
	AdaptiveChoice bool   `json:"adaptiveChoice"`
}

// TTLs maps each Key to how long its product lasts. It comes from {"name|client": seconds}.
// A key without TTL has products that do not expire.
type TTLs map[keys.Key]time.Duration

func (t *TTLs) UnmarshalJSON(data []byte) error {
	var raw map[string]int64
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("json de ttls inválido: %w", err)
	}
	out := make(TTLs, len(raw))
	for k, secs := range raw {
		key, err := keys.Parse(k)
		if err != nil {
			return err
		}
		out[key] = time.Duration(secs) * time.Second
	}
	*t = out
	return nil
}

// GetConfig fetches the config and decodes it into Config.
// TODO: call it every 15s or 60s so the service always runs with the latest config. And update all that use the config.
func (b *BackOffice) GetConfig(ctx context.Context) (Config, error) {
	return b.Get(ctx, configPath)
}

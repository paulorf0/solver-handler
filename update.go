package main

import (
	"context"
	log "log/slog"
	"slices"
	"time"

	"solver-handler/external"
)

const configRefresh = 30 * time.Second // how often RunConfig reloads the backoffice config

var emptyConfig external.Config

// cfg returns the current config, empty until the first load.
func (b *BucketHandler) cfg() *external.Config {
	if c := b.config.Load(); c != nil {
		return c
	}
	return &emptyConfig
}

// RunConfig loads the config from the backoffice now and every configRefresh until ctx ends.
// A failed load keeps the current config.
func (b *BucketHandler) RunConfig(ctx context.Context, bo *external.BackOffice) {
	ticker := time.NewTicker(configRefresh)
	defer ticker.Stop()
	for {
		cfg, err := bo.GetConfig(ctx)
		if err != nil {
			log.Error("falha ao carregar a config do backoffice", "err", err)
		} else {
			b.ApplyConfig(cfg)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// ApplyConfig makes cfg the current config and applies its parts that live elsewhere: the
// concurrent limit of every solver and the routes of the session manager. A solver missing from
// cfg gets no limit. What cfg names but would be ignored, or what the stock needs but cfg lacks,
// is logged as an error.
func (b *BucketHandler) ApplyConfig(cfg external.Config) {
	for key, limits := range cfg.SolverLimit {
		for name := range limits {
			if !slices.ContainsFunc(b.solvers[key], func(s SolverInterface[any]) bool { return s.GetName() == name }) {
				log.Error("limite configurado para solver não registrado", "key", key, "solver", name)
			}
		}
	}
	for key, solvers := range b.solvers {
		for _, s := range solvers {
			s.SetLimit(int(cfg.SolverLimit[key][s.GetName()]))
		}
	}

	for key := range cfg.Stock {
		if _, ok := cfg.Routes[key]; !ok {
			log.Error("estoque configurado para chave sem rota no session manager", "key", key)
		}
	}
	if b.sm != nil {
		b.sm.SetRoutes(cfg.Routes)
	}

	b.config.Store(&cfg)
}

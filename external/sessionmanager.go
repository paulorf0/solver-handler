package external

import (
	"context"
	"fmt"
	"sync/atomic"
)

// Routes maps each key to its path in an external system. It comes from a JSON like
// {"key": "path"}, so keys can be added without a deploy.
type Routes map[string]string

// PoolStats reports the state of the pool of key. Each user implements it
// for their own session manager.
type PoolStats interface {
	Len(ctx context.Context, key string) (int, error)     // items in the pool
	Expired(ctx context.Context, key string) (int, error) // items that expired while stored
}

// SessionManager is a External that saves and reads values on the path that Routes gives for each key.
type SessionManager[T any] struct {
	*External[T]
	routes atomic.Pointer[Routes] // replaced whole by SetRoutes
	pool   PoolStats
}

func NewSessionManager[T any](rawURL string, token *string, routes Routes, pool PoolStats) (*SessionManager[T], error) {
	b, err := New[T](rawURL, token)
	if err != nil {
		return nil, err
	}
	s := &SessionManager[T]{External: b, pool: pool}
	s.SetRoutes(routes)
	return s, nil
}

// SetRoutes replaces the routes. Safe while Get and Save run.
func (s *SessionManager[T]) SetRoutes(routes Routes) {
	s.routes.Store(&routes)
}

// Len returns the pool size of key, as the PoolStats given on creation says.
func (s *SessionManager[T]) Len(ctx context.Context, key string) (int, error) {
	return s.pool.Len(ctx, key)
}

// Expired returns how many stored items of key expired, as the PoolStats given on creation says.
func (s *SessionManager[T]) Expired(ctx context.Context, key string) (int, error) {
	return s.pool.Expired(ctx, key)
}

// Save posts value as JSON to the path of key.
func (s *SessionManager[T]) Save(ctx context.Context, key string, value any) (T, error) {
	path, err := s.route(key)
	if err != nil {
		var out T
		return out, err
	}
	return s.Post(ctx, path, value)
}

// Get fetches the value on the path of key. It hides External.Get, which takes a path.
func (s *SessionManager[T]) Get(ctx context.Context, key string) (T, error) {
	path, err := s.route(key)
	if err != nil {
		var out T
		return out, err
	}
	return s.External.Get(ctx, path)
}

func (s *SessionManager[T]) route(key string) (string, error) {
	routes := s.routes.Load()
	if routes == nil {
		return "", fmt.Errorf("nenhuma rota registrada para a chave %s", key)
	}
	path, ok := (*routes)[key]
	if !ok {
		return "", fmt.Errorf("nenhuma rota registrada para a chave %s", key)
	}
	return path, nil
}

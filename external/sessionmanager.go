package external

import (
	"context"
	"encoding/json"
	"fmt"
	"solver-handler/keys"
	"strings"
)

// Routes maps each Key to its path in an external system. It comes from a JSON like
// {"name|client": "path"}, so keys can be added without a deploy.
type Routes map[keys.Key]string

func (r *Routes) UnmarshalJSON(data []byte) error {
	var raw map[string]string
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("json de rotas inválido: %w", err)
	}
	out := make(Routes, len(raw))
	for k, path := range raw {
		// Split on the last "|", since Name is free and may contain it.
		i := strings.LastIndex(k, "|")
		if i < 0 {
			return fmt.Errorf("chave %q fora do padrão chave|cliente", k)
		}
		out[keys.Key{Name: k[:i], Client: k[i+1:]}] = path
	}
	*r = out
	return nil
}

// SessionManager is a BackOffice that saves and reads values on the path that Routes gives for each key.
type SessionManager[T any] struct {
	*BackOffice[T]
	routes Routes
}

func NewSessionManager[T any](rawURL string, token *string, routes Routes) (*SessionManager[T], error) {
	b, err := New[T](rawURL, token)
	if err != nil {
		return nil, err
	}
	return &SessionManager[T]{BackOffice: b, routes: routes}, nil
}

// Save posts value as JSON to the path of key.
func (s *SessionManager[T]) Save(ctx context.Context, key keys.Key, value any) (T, error) {
	path, err := s.route(key)
	if err != nil {
		var out T
		return out, err
	}
	return s.Post(ctx, path, value)
}

// Get fetches the value on the path of key. It hides BackOffice.Get, which takes a path.
func (s *SessionManager[T]) Get(ctx context.Context, key keys.Key) (T, error) {
	path, err := s.route(key)
	if err != nil {
		var out T
		return out, err
	}
	return s.BackOffice.Get(ctx, path)
}

func (s *SessionManager[T]) route(key keys.Key) (string, error) {
	path, ok := s.routes[key]
	if !ok {
		return "", fmt.Errorf("nenhuma rota registrada para a chave %s", key)
	}
	return path, nil
}

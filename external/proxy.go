package external

import (
	"context"
)

// ProxyProvider is an External that hands out proxies. T is the proxy plus whatever
// extra info the provider returns.
type ProxyProvider[T any] struct {
	*External[T]
	path       func(string) string // GET path for a key; may use part or all of it
	reportPath string
}

// ProxyReport tells the provider whether a proxy worked for a key.
type ProxyReport struct {
	Key     string `json:"key"`
	Proxy   string `json:"proxy"`
	Success bool   `json:"success"`
}

func NewProxyProvider[T any](rawURL string, token *string, path func(string) string, reportPath string) (*ProxyProvider[T], error) {
	b, err := New[T](rawURL, token)
	if err != nil {
		return nil, err
	}
	return &ProxyProvider[T]{External: b, path: path, reportPath: reportPath}, nil
}

// Get fetches a proxy for key. It hides External.Get, which takes a path.
func (p *ProxyProvider[T]) Get(ctx context.Context, key string) (T, error) {
	return p.External.Get(ctx, p.path(key))
}

// Report tells the provider whether proxy worked for key.
func (p *ProxyProvider[T]) Report(ctx context.Context, key string, proxy string, success bool) error {
	_, err := p.Post(ctx, p.reportPath, ProxyReport{Key: key, Proxy: proxy, Success: success})
	return err
}

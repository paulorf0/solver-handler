package external

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

// BackOffice talks JSON over HTTP to an external system whose resource is T.
type BackOffice[T any] struct {
	url   url.URL
	token *string
}

func New[T any](rawURL string, token *string) (*BackOffice[T], error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("url do backoffice inválida: %w", err)
	}
	return &BackOffice[T]{url: *u, token: token}, nil
}

// client is shared by every BackOffice so all of them reuse one connection pool.
var client = &http.Client{
	Timeout: 15 * time.Second,
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   64,               // default is 2, which forces reconnects under load
		MaxConnsPerHost:       0,                // no cap, throughput first
		IdleConnTimeout:       50 * time.Second, // below ALB (60s) and NAT (350s) idle timeouts
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		WriteBufferSize:       64 << 10,
		ReadBufferSize:        64 << 10,
	},
}

// Get fetches url/path and decodes the JSON response into T.
func (b *BackOffice[T]) Get(ctx context.Context, path string) (T, error) {
	return b.do(ctx, http.MethodGet, path, nil)
}

// Post sends body as JSON to url/path and decodes the JSON response into T.
func (b *BackOffice[T]) Post(ctx context.Context, path string, body any) (T, error) {
	data, err := json.Marshal(body)
	if err != nil {
		var out T
		return out, fmt.Errorf("falha ao serializar o corpo: %w", err)
	}
	return b.do(ctx, http.MethodPost, path, data)
}

// do sends the request and decodes the JSON response into T. A nil body sends none.
func (b *BackOffice[T]) do(ctx context.Context, method, path string, body []byte) (T, error) {
	var out T

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, b.url.JoinPath(path).String(), reader)
	if err != nil {
		return out, fmt.Errorf("falha ao montar a requisição: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if b.token != nil {
		req.Header.Set("Authorization", "Bearer "+*b.token)
	}

	resp, err := client.Do(req)
	if err != nil {
		return out, fmt.Errorf("falha na requisição %s %s: %w", method, path, err)
	}
	defer func() {
		// Drain leftovers so the connection goes back to the pool.
		io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		resp.Body.Close()
	}()

	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return out, fmt.Errorf("backoffice respondeu %d em %s %s: %s", resp.StatusCode, method, path, msg)
	}
	// Empty body (e.g. 201/204 on POST) is not an error.
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil && err != io.EOF {
		return out, fmt.Errorf("falha ao ler a resposta: %w", err)
	}
	return out, nil
}

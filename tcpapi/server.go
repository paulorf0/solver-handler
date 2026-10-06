// Package tcpapi is a request/response API over TCP with JSON messages and delivery ack.
// Requests are multiplexed: one connection carries many in-flight requests, matched by id.
//
//	client -> {"id": 1, "data": Req}
//	server -> {"id": 1, "data": Resp} or {"id": 1, "error": "..."}
//	client -> {"id": 1, "ack": true}   (only after a "data" response)
//
// A response not acked within AckTimeout, or whose connection drops, is handed to Undelivered.
// Delivery is at-least-once: if the ack itself is lost, the client may get the item twice.
package tcpapi

import (
	"context"
	"encoding/json"
	"fmt"
	log "log/slog"
	"net"
	"sync"
	"time"
)

type message struct {
	ID    uint64          `json:"id"`
	Data  json.RawMessage `json:"data,omitempty"`
	Error string          `json:"error,omitempty"`
	Ack   bool            `json:"ack,omitempty"`
}

type Server[Req, Resp any] struct {
	Handle      func(ctx context.Context, req Req) (Resp, error) // runs in its own goroutine per request
	Undelivered func(resp Resp)                                  // called when the client did not ack

	AckTimeout time.Duration // default 5s, also the write timeout
}

// ListenAndServe serves on addr until ctx is done, then waits for open connections to finish.
func (s *Server[Req, Resp]) ListenAndServe(ctx context.Context, addr string) error {
	lc := net.ListenConfig{KeepAlive: 30 * time.Second}
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("falha ao escutar em %s: %w", addr, err)
	}
	return s.Serve(ctx, ln)
}

func (s *Server[Req, Resp]) Serve(ctx context.Context, ln net.Listener) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	stop := context.AfterFunc(ctx, func() { ln.Close() })
	defer stop()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("falha ao aceitar conexão: %w", err)
		}
		wg.Go(func() { s.serveConn(ctx, conn) })
	}
}

// serverConn holds one connection's write lock and the responses waiting for ack.
type serverConn[Resp any] struct {
	net.Conn
	timeout time.Duration

	wmu sync.Mutex
	enc *json.Encoder

	mu      sync.Mutex
	pending map[uint64]*pending[Resp]
}

type pending[Resp any] struct {
	resp  Resp
	timer *time.Timer
}

func (s *Server[Req, Resp]) serveConn(ctx context.Context, nc net.Conn) {
	c := &serverConn[Resp]{
		Conn:    nc,
		timeout: orDefault(s.AckTimeout, 5*time.Second),
		enc:     json.NewEncoder(nc),
		pending: map[uint64]*pending[Resp]{},
	}
	var wg sync.WaitGroup
	defer func() {
		nc.Close()
		wg.Wait()
		// Connection is gone: nothing still waiting for ack can be confirmed anymore.
		for _, resp := range c.takeAll() {
			s.undelivered(resp, nc)
		}
	}()
	// Closing the conn on shutdown unblocks the reader.
	stop := context.AfterFunc(ctx, func() { nc.Close() })
	defer stop()

	dec := json.NewDecoder(nc)
	for {
		var m message
		if err := dec.Decode(&m); err != nil {
			return
		}
		if m.Ack {
			c.take(m.ID)
			continue
		}
		wg.Go(func() { s.serveRequest(ctx, c, m) })
	}
}

func (s *Server[Req, Resp]) serveRequest(ctx context.Context, c *serverConn[Resp], m message) {
	var req Req
	if err := json.Unmarshal(m.Data, &req); err != nil {
		c.write(message{ID: m.ID, Error: "requisição inválida: " + err.Error()})
		return
	}
	resp, err := s.Handle(ctx, req)
	if err != nil {
		c.write(message{ID: m.ID, Error: err.Error()})
		return
	}
	data, err := json.Marshal(resp)
	if err != nil {
		s.undelivered(resp, c)
		c.write(message{ID: m.ID, Error: "falha ao serializar a resposta: " + err.Error()})
		return
	}

	// Track before writing, so a fast ack always finds it.
	c.track(m.ID, resp, func(r Resp) { s.undelivered(r, c) })
	if err := c.write(message{ID: m.ID, Data: data}); err != nil {
		if r, ok := c.take(m.ID); ok {
			s.undelivered(r, c)
		}
	}
}

func (s *Server[Req, Resp]) undelivered(resp Resp, conn net.Conn) {
	log.Warn("produto não confirmado pelo cliente", "remote", conn.RemoteAddr())
	if s.Undelivered != nil {
		s.Undelivered(resp)
	}
}

func (c *serverConn[Resp]) write(m message) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.SetWriteDeadline(time.Now().Add(c.timeout))
	return c.enc.Encode(m)
}

// track waits for the ack of id; on timeout the response goes to onTimeout.
func (c *serverConn[Resp]) track(id uint64, resp Resp, onTimeout func(Resp)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := &pending[Resp]{resp: resp}
	p.timer = time.AfterFunc(c.timeout, func() {
		if r, ok := c.take(id); ok {
			onTimeout(r)
		}
	})
	c.pending[id] = p
}

// take removes id from pending. Only the first caller gets ok, so a response is handed back once.
func (c *serverConn[Resp]) take(id uint64) (Resp, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.pending[id]
	if !ok {
		var zero Resp
		return zero, false
	}
	p.timer.Stop()
	delete(c.pending, id)
	return p.resp, true
}

func (c *serverConn[Resp]) takeAll() []Resp {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Resp, 0, len(c.pending))
	for id, p := range c.pending {
		p.timer.Stop()
		delete(c.pending, id)
		out = append(out, p.resp)
	}
	return out
}

func orDefault(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

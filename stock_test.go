package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"solver-handler/external"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// funcSolver runs fn on Solve, so a test sees the ctx and params the handler passes.
type funcSolver struct {
	BaseSolver
	fn func(ctx context.Context, params Params) (any, Info)
}

func (s *funcSolver) Solve(ctx context.Context, params Params) (any, Info) { return s.fn(ctx, params) }

// stockEnv is a handler wired to fake external systems: a proxy provider that always hands out
// proxyURL, and a session manager that records what it saves.
type stockEnv struct {
	b       *BucketHandler
	reports chan external.ProxyReport
	saved   chan Product[any]
}

const proxyURL = "http://user:pass@proxy:8080"

func newStockEnv(t *testing.T, solver SolverInterface[any]) *stockEnv {
	t.Helper()
	env := &stockEnv{reports: make(chan external.ProxyReport, 4), saved: make(chan Product[any], 4)}

	proxySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			var rep external.ProxyReport
			body, _ := io.ReadAll(r.Body)
			json.Unmarshal(body, &rep)
			env.reports <- rep
		}
		json.NewEncoder(w).Encode(ProxyInfo{Proxy: proxyURL})
	}))
	t.Cleanup(proxySrv.Close)
	smSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { // estoque vazio
			http.NotFound(w, r)
			return
		}
		var p Product[any]
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &p)
		env.saved <- p
		w.Write(body)
	}))
	t.Cleanup(smSrv.Close)

	proxy, err := external.NewProxyProvider[ProxyInfo](proxySrv.URL, nil, func(k string) string { return "/proxy/" + k }, "/report")
	if err != nil {
		t.Fatal(err)
	}
	sm, err := external.NewSessionManager[Product[any]](smSrv.URL, nil, external.Routes{keyA: "/stock"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Nothing listens on port 1: the Redis metrics fail, and stockOne ignores them.
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 50 * time.Millisecond, MaxRetries: -1})
	t.Cleanup(func() { rdb.Close() })

	env.b = newBuckets(keyA, solver)
	env.b.proxy, env.b.sm, env.b.redis = proxy, sm, rdb
	return env
}

func (env *stockEnv) report(t *testing.T) external.ProxyReport {
	t.Helper()
	select {
	case r := <-env.reports:
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("nenhum report de proxy recebido")
		return external.ProxyReport{}
	}
}

func (env *stockEnv) noSave(t *testing.T) {
	t.Helper()
	select {
	case p := <-env.saved:
		t.Errorf("produto salvo no estoque após falha: %+v", p)
	default:
	}
}

func TestStockOne_ErroDeProxyDescartaEReportaFalha(t *testing.T) {
	solver := &funcSolver{BaseSolver: BaseSolver{name: keyA, key: keyA}}
	solver.fn = func(context.Context, Params) (any, Info) {
		return nil, Info{err: errors.New("407"), errCode: 407}
	}
	env := newStockEnv(t, solver)

	if env.b.stockOne(context.Background(), keyA, solver) {
		t.Fatal("stockOne = true, quer false")
	}
	if r := env.report(t); r.Success || r.Proxy != proxyURL {
		t.Errorf("report = %+v, quer falha da proxy %q", r, proxyURL)
	}
	if _, ok := env.b.proxies.Load(keyA); ok {
		t.Error("proxy com erro continua em uso")
	}
	if c := solver.GetStatistic().Snapshot(time.Now()); c.genFailure != 0 {
		t.Errorf("genFailure = %v, erro de proxy não pune o solver", c.genFailure)
	}
	env.noSave(t)
}

func TestStockOne_ErroDoSolverMantemAProxy(t *testing.T) {
	solver := &funcSolver{BaseSolver: BaseSolver{name: keyA, key: keyA}}
	solver.fn = func(context.Context, Params) (any, Info) {
		return nil, Info{err: errors.New("500"), errCode: 500}
	}
	env := newStockEnv(t, solver)

	if env.b.stockOne(context.Background(), keyA, solver) {
		t.Fatal("stockOne = true, quer false")
	}
	if r := env.report(t); !r.Success {
		t.Errorf("report = %+v, erro que não é de proxy reporta a proxy como boa", r)
	}
	if p, _ := env.b.proxies.Load(keyA); p != proxyURL {
		t.Errorf("proxy em uso = %v, quer %q", p, proxyURL)
	}
	if c := solver.GetStatistic().Snapshot(time.Now()); c.genFailure < 0.99 { // decai um pouco até o Snapshot
		t.Errorf("genFailure = %v, quer 1", c.genFailure)
	}
	env.noSave(t)
}

func TestStockOne_SucessoSalvaComAProxyEReporta(t *testing.T) {
	var deadline time.Duration
	solver := &funcSolver{BaseSolver: BaseSolver{name: keyA, key: keyA}}
	solver.fn = func(ctx context.Context, p Params) (any, Info) {
		if d, ok := ctx.Deadline(); ok {
			deadline = time.Until(d)
		}
		return "token", Info{}
	}
	env := newStockEnv(t, solver)

	if !env.b.stockOne(context.Background(), keyA, solver) {
		t.Fatal("stockOne = false, quer true")
	}
	if r := env.report(t); !r.Success || r.Proxy != proxyURL {
		t.Errorf("report = %+v, quer sucesso da proxy %q", r, proxyURL)
	}
	select {
	case p := <-env.saved:
		if p.Params.Proxy != proxyURL || p.Token != "token" {
			t.Errorf("produto salvo = %+v, quer token com a proxy %q", p, proxyURL)
		}
	default:
		t.Error("produto não foi salvo")
	}
	// Sem amostras, o prazo é o máximo.
	if deadline < (maxSolveTimeout-1)*time.Second || deadline > maxSolveTimeout*time.Second {
		t.Errorf("prazo do solve = %v, quer ~%ds", deadline, maxSolveTimeout)
	}
}

func TestStockOne_CancelamentoDoPaiChegaAoSolve(t *testing.T) {
	solver := &funcSolver{BaseSolver: BaseSolver{name: keyA, key: keyA}}
	solver.fn = func(ctx context.Context, _ Params) (any, Info) {
		<-ctx.Done()
		return nil, Info{err: ctx.Err()}
	}
	env := newStockEnv(t, solver)
	env.b.proxy = nil // sem provedor de proxy: não pode reportar nem entrar em panic

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if env.b.stockOne(ctx, keyA, solver) {
		t.Fatal("stockOne = true, quer false")
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("solve levou %v, o cancelamento do pai não chegou ao ctx do solve", took)
	}
	if c := solver.GetStatistic().Snapshot(time.Now()); c.genFailure != 0 {
		t.Errorf("genFailure = %v, erro sem código não pune o solver", c.genFailure)
	}
}

func TestGetProduct_PrazoDoSolveEODoSolver(t *testing.T) {
	for _, client := range []float64{0.5, 120} {
		var deadline time.Duration
		solver := &funcSolver{BaseSolver: BaseSolver{name: keyA, key: keyA}}
		solver.fn = func(ctx context.Context, _ Params) (any, Info) {
			d, _ := ctx.Deadline()
			deadline = time.Until(d)
			return "ok", Info{}
		}
		b := newBuckets(keyA, solver)

		if _, err := b.GetProduct(Request{Key: keyA, Timeout: &client}); err != nil {
			t.Fatalf("erro inesperado: %v", err)
		}
		if deadline <= (maxSolveTimeout-1)*time.Second || deadline > maxSolveTimeout*time.Second {
			t.Errorf("cliente %vs: prazo do solve = %v, quer ~%ds", client, deadline, maxSolveTimeout)
		}
	}
}

func TestGetProduct_OrcamentoEsgotadoVaiParaOEstoque(t *testing.T) {
	solver := &funcSolver{BaseSolver: BaseSolver{name: keyA, key: keyA}}
	solver.fn = func(context.Context, Params) (any, Info) {
		time.Sleep(300 * time.Millisecond)
		return "tarde", Info{}
	}
	env := newStockEnv(t, solver)
	env.b.proxy, env.b.redis = nil, nil // sem as esperas do Redis inacessível

	budget := 0.05
	start := time.Now()
	_, err := env.b.GetProduct(Request{Key: keyA, Timeout: &budget})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, quer prazo esgotado", err)
	}
	if took := time.Since(start); took > 250*time.Millisecond {
		t.Errorf("GetProduct levou %v, quer voltar no fim do orçamento", took)
	}
	select {
	case p := <-env.saved:
		if p.Token != "tarde" {
			t.Errorf("produto salvo = %+v, quer o da tentativa em andamento", p)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("o produto da tentativa em andamento não foi para o estoque")
	}
}

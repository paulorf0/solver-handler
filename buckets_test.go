package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

var (
	keyA     = "a"
	keyB     = "b"
	keyOutra = "outra"
	keyAlfa  = "alfa"
	keyBeta  = "beta"
)

type fakeSolver struct {
	key   string
	calls int
	token any
	err   error
	code  int
	stats SolverStats
}

func (f *fakeSolver) GetKey() string { return f.key }

func (f *fakeSolver) GetWeight() int { return 0 }

func (f *fakeSolver) GetName() string { return f.key }

func (f *fakeSolver) GetStatistic() *SolverStats { return &f.stats }

func (f *fakeSolver) GetInflights() int { return 0 }
func (f *fakeSolver) AddInflights()     {}
func (f *fakeSolver) SubInflights()     {}
func (f *fakeSolver) GetLimit() int     { return 0 }
func (f *fakeSolver) SetLimit(int)      {}

func (f *fakeSolver) Solve(context.Context, Params) (any, Info) {
	f.calls++
	return f.token, Info{err: f.err, errCode: f.code}
}

func newBuckets(key string, solver SolverInterface[any]) *BucketHandler {
	return &BucketHandler{
		solvers: map[string][]SolverInterface[any]{key: {solver}},
		stats:   map[string]*KeyStats{key: {}},
	}
}

func TestGetProduct_SemEstoqueResolveNaHora(t *testing.T) {
	solver := &fakeSolver{key: keyA, token: "Solver"}
	b := newBuckets(keyA, solver)

	got, err := b.GetProduct(Request{Key: keyA})
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if got.Token != "Solver" {
		t.Errorf("Token = %v, quer %q", got.Token, "Solver")
	}
	if got.Id != GetHash(keyA) || got.Key != keyA {
		t.Errorf("Id = %q, Key = %q; quer %q, %q", got.Id, got.Key, GetHash(keyA), "a")
	}
	if solver.calls != 1 {
		t.Errorf("Solver chamado %d vezes, quer 1", solver.calls)
	}
}

func TestGetProduct_ErroDoSolver(t *testing.T) {
	boom := errors.New("boom")
	b := newBuckets(keyA, &fakeSolver{key: keyA, err: boom})

	_, err := b.GetProduct(Request{Key: keyA})
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, quer %v", err, boom)
	}
}

func TestGetProduct_ChaveInexistente(t *testing.T) {
	b := newBuckets(keyA, &fakeSolver{key: keyA})

	if _, err := b.GetProduct(Request{Key: keyOutra}); err == nil {
		t.Error("quer erro para chave inexistente")
	}
}

func TestGetProduct_SemSolver(t *testing.T) {
	b := newBuckets(keyA, &fakeSolver{key: keyA})
	delete(b.solvers, keyA)

	if _, err := b.GetProduct(Request{Key: keyA}); err == nil {
		t.Error("quer erro para chave sem Solver e sem estoque")
	}
}

func TestTryGetSolver(t *testing.T) {
	solver := &fakeSolver{key: keyA}
	b := newBuckets(keyA, solver)

	got, err := b.TryGetSolver(keyA)
	if err != nil || len(got) != 1 || got[0] != SolverInterface[any](solver) {
		t.Errorf("got = %v, err = %v; quer %v, nil", got, err, solver)
	}
	if _, err := b.TryGetSolver(keyOutra); err == nil {
		t.Error("quer erro para chave inexistente")
	}
}

// --- campos do BucketHandler ---

func TestBucketHandler_Campos(t *testing.T) {
	b := newBuckets(keyAlfa, &fakeSolver{key: keyAlfa})

	if len(b.solvers) != 1 || b.solvers[keyAlfa] == nil {
		t.Errorf("solvers = %v, quer só o Solver de %q", b.solvers, keyAlfa)
	}
	if b.redis != nil {
		t.Error("redis deveria ser nil num handler montado sem conexão")
	}
}

func TestNewBucketHandler(t *testing.T) {
	b := NewBucketHandler()
	if b == nil {
		t.Skip("redis indisponível em localhost:6379")
	}
	if b.redis == nil {
		t.Error("redis nil mesmo com o handler criado")
	}
	if len(b.solvers) != len(registry) {
		t.Errorf("solvers tem %d itens, registry tem %d", len(b.solvers), len(registry))
	}
}

func TestNewBaseSolver(t *testing.T) {
	s := NewBaseSolver("alfa", keyAlfa, 0)

	if s.GetKey() != keyAlfa {
		t.Errorf("GetKey() = %q, quer %q", s.GetKey(), keyAlfa)
	}
}

// --- chave -> Solver ---

func TestChaveParaSolver(t *testing.T) {
	alfa := &fakeSolver{key: keyAlfa, token: "Token-alfa"}
	beta := &fakeSolver{key: keyBeta, token: "Token-beta"}
	b := &BucketHandler{
		solvers: map[string][]SolverInterface[any]{keyAlfa: {alfa}, keyBeta: {beta}},
		stats:   map[string]*KeyStats{keyAlfa: {}, keyBeta: {}},
	}

	got, err := b.GetProduct(Request{Key: keyBeta})
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if got.Token != "Token-beta" || got.Key != keyBeta {
		t.Errorf("Token = %v, Key = %q; quer %q, %q", got.Token, got.Key, "Token-beta", keyBeta)
	}
	if beta.calls != 1 || alfa.calls != 0 {
		t.Errorf("calls beta = %d, alfa = %d; quer 1, 0", beta.calls, alfa.calls)
	}
}

func TestSolverRegistradoTemASuaChave(t *testing.T) {
	b := newBuckets(keyAlfa, &fakeSolver{key: keyAlfa})

	order, err := b.TryGetSolver(keyAlfa)
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if order[0].GetKey() != keyAlfa {
		t.Errorf("GetKey() = %q, quer %q", order[0].GetKey(), keyAlfa)
	}
}

// --- BaseSolver ---

type embedSolver struct {
	BaseSolver
}

func (*embedSolver) Solve(context.Context, Params) (any, Info) { return nil, Info{} }

func TestBaseSolver_Key(t *testing.T) {
	s := &embedSolver{BaseSolver{key: keyAlfa}}

	var _ SolverInterface[any] = s // embutir BaseSolver já satisfaz tudo menos Solve()
	if s.GetKey() != keyAlfa {
		t.Errorf("GetKey() = %q, quer %q", s.GetKey(), keyAlfa)
	}
}

// --- erros e timeout do solve ---

func TestGetProduct_SucessoNaoEntraEmPanico(t *testing.T) {
	b := newBuckets(keyA, &fakeSolver{key: keyA, token: "ok"})

	got, err := b.GetProduct(Request{Key: keyA, Params: Params{Proxy: "http://p"}})
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if got.Params.Proxy != "http://p" {
		t.Errorf("Params.Proxy = %q, quer %q", got.Params.Proxy, "http://p")
	}
}

func TestAddGeneration_ErroQueNaoEDoSolver(t *testing.T) {
	boom := errors.New("boom")
	for _, code := range []int{0, 407, 429, 502} {
		var s SolverStats
		s.AddGeneration(time.Now(), time.Second, Info{err: boom, errCode: code})
		if c := s.Snapshot(time.Now()); c.genFailure != 0 || c.genSuccess != 0 || c.w != 0 {
			t.Errorf("código %d: contadores = %+v, quer zerados", code, c)
		}
	}

	var s SolverStats
	s.AddGeneration(time.Now(), time.Second, Info{err: boom, errCode: 500})
	if c := s.Snapshot(time.Now()); c.genFailure != 1 {
		t.Errorf("código 500: genFailure = %v, quer 1", c.genFailure)
	}
}

func TestTimeoutGen(t *testing.T) {
	var s SolverStats
	if got := s.TimeoutGen(30, 2); got != 30 {
		t.Errorf("sem amostras: TimeoutGen = %v, quer 30", got)
	}

	// média 4s, desvio 1s -> 4 + 4×1 = 8s
	now := time.Now()
	for _, d := range []time.Duration{3 * time.Second, 5 * time.Second} {
		s.AddGeneration(now, d, Info{})
	}
	if got := s.TimeoutGen(30, 2); got < 7.99 || got > 8.01 {
		t.Errorf("TimeoutGen = %v, quer 8", got)
	}
}

func TestAddGeneration_CorteEntraSoNaLatencia(t *testing.T) {
	var s SolverStats
	now := time.Now()
	s.AddGeneration(now, 3*time.Second, Info{})
	s.AddGeneration(now, 5*time.Second, Info{err: context.DeadlineExceeded})

	c := s.Snapshot(now)
	if c.genSuccess != 1 || c.genFailure != 0 {
		t.Errorf("genSuccess = %v, genFailure = %v; quer 1, 0", c.genSuccess, c.genFailure)
	}
	if c.w != 2 || c.meanElapsedGenSucess != 4 {
		t.Errorf("w = %v, média = %v; quer 2, 4 (o corte entra na latência)", c.w, c.meanElapsedGenSucess)
	}
}

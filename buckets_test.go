package main

import (
	"errors"
	"testing"
)

var (
	keyA     = Key{Name: "a", Client: "cli"}
	keyB     = Key{Name: "b", Client: "cli"}
	keyOutra = Key{Name: "outra", Client: "cli"}
	keyAlfa  = Key{Name: "alfa", Client: "cli"}
	keyBeta  = Key{Name: "beta", Client: "cli"}
)

type fakeSolver struct {
	key   Key
	calls int
	token any
	err   error
	stats SolverStats
}

func (f *fakeSolver) GetKey() Key { return f.key }

func (f *fakeSolver) GetWeight() int { return 0 }

func (f *fakeSolver) GetName() string { return f.key.String() }

func (f *fakeSolver) GetStatistic() *SolverStats { return &f.stats }

func (f *fakeSolver) GetInflights() int { return 0 }
func (f *fakeSolver) AddInflights()     {}
func (f *fakeSolver) SubInflights()     {}

func (f *fakeSolver) Solve(Params) (any, error) {
	f.calls++
	return f.token, f.err
}

func newBuckets(key Key, solver SolverInterface[any]) *BucketHandler {
	return &BucketHandler{
		solvers: map[Key][]SolverInterface[any]{key: {solver}},
		stats:   map[Key]*KeyStats{key: {}},
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
	if err != nil || got != SolverInterface[any](solver) {
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
		solvers: map[Key][]SolverInterface[any]{keyAlfa: {alfa}, keyBeta: {beta}},
		stats:   map[Key]*KeyStats{keyAlfa: {}, keyBeta: {}},
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

	solver, err := b.TryGetSolver(keyAlfa)
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if solver.GetKey() != keyAlfa {
		t.Errorf("GetKey() = %q, quer %q", solver.GetKey(), keyAlfa)
	}
}

// --- BaseSolver ---

type embedSolver struct {
	BaseSolver
}

func (*embedSolver) Solve(Params) (any, error) { return nil, nil }

func TestBaseSolver_Key(t *testing.T) {
	s := &embedSolver{BaseSolver{key: keyAlfa}}

	var _ SolverInterface[any] = s // embutir BaseSolver já satisfaz tudo menos Solve()
	if s.GetKey() != keyAlfa {
		t.Errorf("GetKey() = %q, quer %q", s.GetKey(), keyAlfa)
	}
}

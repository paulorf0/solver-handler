package main

import (
	"errors"
	"testing"
)

var (
	keyA     = Key{Name: "a", Client: "cli"}
	keyB     = Key{Name: "b", Client: "cli"}
	keyOutra = Key{Name: "outra", Client: "cli"}
	keyLatam = Key{Name: "latam", Client: "cli"}
	keyGol   = Key{Name: "gol", Client: "cli"}
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

func (f *fakeSolver) Solve() (any, error) {
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
	solver := &fakeSolver{key: keyA, token: "solver"}
	b := newBuckets(keyA, solver)

	got, err := b.GetProduct(keyA)
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if got.token != "solver" {
		t.Errorf("token = %v, quer %q", got.token, "solver")
	}
	if got.id != GetHash(keyA) || got.key != keyA {
		t.Errorf("id = %q, key = %q; quer %q, %q", got.id, got.key, GetHash(keyA), "a")
	}
	if solver.calls != 1 {
		t.Errorf("solver chamado %d vezes, quer 1", solver.calls)
	}
}

func TestGetProduct_ErroDoSolver(t *testing.T) {
	boom := errors.New("boom")
	b := newBuckets(keyA, &fakeSolver{key: keyA, err: boom})

	_, err := b.GetProduct(keyA)
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, quer %v", err, boom)
	}
}

func TestGetProduct_ChaveInexistente(t *testing.T) {
	b := newBuckets(keyA, &fakeSolver{key: keyA})

	if _, err := b.GetProduct(keyOutra); err == nil {
		t.Error("quer erro para chave inexistente")
	}
}

func TestGetProduct_SemSolver(t *testing.T) {
	b := newBuckets(keyA, &fakeSolver{key: keyA})
	delete(b.solvers, keyA)

	if _, err := b.GetProduct(keyA); err == nil {
		t.Error("quer erro para chave sem solver e sem estoque")
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
	b := newBuckets(keyLatam, &fakeSolver{key: keyLatam})

	if len(b.solvers) != 1 || b.solvers[keyLatam] == nil {
		t.Errorf("solvers = %v, quer só o solver de %q", b.solvers, keyLatam)
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
	s := NewBaseSolver("latam", keyLatam, 0)

	if s.GetKey() != keyLatam {
		t.Errorf("GetKey() = %q, quer %q", s.GetKey(), keyLatam)
	}
}

// --- chave -> solver ---

func TestChaveParaSolver(t *testing.T) {
	latam := &fakeSolver{key: keyLatam, token: "token-latam"}
	gol := &fakeSolver{key: keyGol, token: "token-gol"}
	b := &BucketHandler{
		solvers: map[Key][]SolverInterface[any]{keyLatam: {latam}, keyGol: {gol}},
		stats:   map[Key]*KeyStats{keyLatam: {}, keyGol: {}},
	}

	got, err := b.GetProduct(keyGol)
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if got.token != "token-gol" || got.key != keyGol {
		t.Errorf("token = %v, key = %q; quer %q, %q", got.token, got.key, "token-gol", keyGol)
	}
	if gol.calls != 1 || latam.calls != 0 {
		t.Errorf("calls gol = %d, latam = %d; quer 1, 0", gol.calls, latam.calls)
	}
}

func TestSolverRegistradoTemASuaChave(t *testing.T) {
	b := newBuckets(keyLatam, &fakeSolver{key: keyLatam})

	solver, err := b.TryGetSolver(keyLatam)
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if solver.GetKey() != keyLatam {
		t.Errorf("GetKey() = %q, quer %q", solver.GetKey(), keyLatam)
	}
}

// --- BaseSolver ---

type embedSolver struct {
	BaseSolver
}

func (*embedSolver) Solve() (any, error) { return nil, nil }

func TestBaseSolver_Key(t *testing.T) {
	s := &embedSolver{BaseSolver{key: keyLatam}}

	var _ SolverInterface[any] = s // embutir BaseSolver já satisfaz tudo menos Solve()
	if s.GetKey() != keyLatam {
		t.Errorf("GetKey() = %q, quer %q", s.GetKey(), keyLatam)
	}
}

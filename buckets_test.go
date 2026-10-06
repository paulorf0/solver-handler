package main

import (
	"errors"
	"solver-handler/fifo"
	"testing"
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

func (f *fakeSolver) GetName() string { return string(f.key) }

func (f *fakeSolver) GetStatistic() *SolverStats { return &f.stats }

func (f *fakeSolver) GetInflights() int { return 0 }
func (f *fakeSolver) AddInflights()     {}
func (f *fakeSolver) SubInflights()     {}

func (f *fakeSolver) Solve() (any, error) {
	f.calls++
	return f.token, f.err
}

func newBuckets(key Key, solver SolverInterface[any]) (*BucketHandler, *Queue) {
	q := fifo.New[Product[any]]()
	b := &BucketHandler{
		queues:  map[Key]*Queue{key: q},
		solvers: map[Key][]SolverInterface[any]{key: {solver}},
		stats:   map[Key]*KeyStats{key: {}},
	}
	return b, q
}

func TestGetProduct_PegaDaFila(t *testing.T) {
	solver := &fakeSolver{key: "a", token: "solver"}
	b, q := newBuckets("a", solver)
	q.Push(Product[any]{id: "fila"})

	got, err := b.GetProduct("a")
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if got.id != "fila" {
		t.Errorf("id = %q, quer %q", got.id, "fila")
	}
	if solver.calls != 0 {
		t.Errorf("solver chamado %d vezes, quer 0", solver.calls)
	}
}

func TestGetProduct_FilaVaziaResolveNaHora(t *testing.T) {
	solver := &fakeSolver{key: "a", token: "solver"}
	b, _ := newBuckets("a", solver)

	got, err := b.GetProduct("a")
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if got.token != "solver" {
		t.Errorf("token = %v, quer %q", got.token, "solver")
	}
	if got.id != GetHash("a") || got.key != "a" {
		t.Errorf("id = %q, key = %q; quer %q, %q", got.id, got.key, GetHash("a"), "a")
	}
	if solver.calls != 1 {
		t.Errorf("solver chamado %d vezes, quer 1", solver.calls)
	}
}

func TestGetProduct_ErroDoSolver(t *testing.T) {
	boom := errors.New("boom")
	b, _ := newBuckets("a", &fakeSolver{key: "a", err: boom})

	_, err := b.GetProduct("a")
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, quer %v", err, boom)
	}
}

func TestGetProduct_SemQueue(t *testing.T) {
	b, _ := newBuckets("a", &fakeSolver{key: "a"})

	if _, err := b.GetProduct("outra"); err == nil {
		t.Error("quer erro para chave sem queue")
	}
}

func TestGetProduct_SemSolver(t *testing.T) {
	b, _ := newBuckets("a", &fakeSolver{key: "a"})
	delete(b.solvers, "a")

	if _, err := b.GetProduct("a"); err == nil {
		t.Error("quer erro para chave sem solver e fila vazia")
	}
}

func TestTryGetQueue(t *testing.T) {
	b, q := newBuckets("a", &fakeSolver{key: "a"})

	got, err := b.TryGetQueue("a")
	if err != nil || got != q {
		t.Errorf("got = %p, err = %v; quer %p, nil", got, err, q)
	}
	if _, err := b.TryGetQueue("x"); err == nil {
		t.Error("quer erro para chave inexistente")
	}
}

func TestTryGetSolver(t *testing.T) {
	solver := &fakeSolver{key: "a"}
	b, _ := newBuckets("a", solver)

	got, err := b.TryGetSolver("a")
	if err != nil || got != SolverInterface[any](solver) {
		t.Errorf("got = %v, err = %v; quer %v, nil", got, err, solver)
	}
	if _, err := b.TryGetSolver("x"); err == nil {
		t.Error("quer erro para chave inexistente")
	}
}

// --- campos do BucketHandler ---

func TestBucketHandler_Campos(t *testing.T) {
	b, q := newBuckets("latam|cli", &fakeSolver{key: "latam|cli"})

	if len(b.queues) != 1 || b.queues["latam|cli"] != q {
		t.Errorf("queues = %v, quer só a queue de %q", b.queues, "latam|cli")
	}
	if len(b.solvers) != 1 || b.solvers["latam|cli"] == nil {
		t.Errorf("solvers = %v, quer só o solver de %q", b.solvers, "latam|cli")
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
	if b.queues == nil || len(b.queues) != len(registry) {
		t.Errorf("queues = %v, quer uma queue por solver do registry", b.queues)
	}
}

func TestNewBaseSolver(t *testing.T) {
	s := NewBaseSolver("latam", "latam|cli", 0)

	if s.GetKey() != "latam|cli" {
		t.Errorf("GetKey() = %q, quer %q", s.GetKey(), "latam|cli")
	}
}

// --- chave -> solver ---

func TestChaveParaSolver(t *testing.T) {
	latam := &fakeSolver{key: "latam|cli", token: "token-latam"}
	gol := &fakeSolver{key: "gol|cli", token: "token-gol"}
	b := &BucketHandler{
		queues: map[Key]*Queue{
			"latam|cli": fifo.New[Product[any]](),
			"gol|cli":   fifo.New[Product[any]](),
		},
		solvers: map[Key][]SolverInterface[any]{"latam|cli": {latam}, "gol|cli": {gol}},
		stats:   map[Key]*KeyStats{"latam|cli": {}, "gol|cli": {}},
	}

	got, err := b.GetProduct("gol|cli")
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if got.token != "token-gol" || got.key != "gol|cli" {
		t.Errorf("token = %v, key = %q; quer %q, %q", got.token, got.key, "token-gol", "gol|cli")
	}
	if gol.calls != 1 || latam.calls != 0 {
		t.Errorf("calls gol = %d, latam = %d; quer 1, 0", gol.calls, latam.calls)
	}
}

func TestSolverRegistradoTemASuaChave(t *testing.T) {
	b, _ := newBuckets("latam|cli", &fakeSolver{key: "latam|cli"})

	solver, err := b.TryGetSolver("latam|cli")
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if solver.GetKey() != "latam|cli" {
		t.Errorf("GetKey() = %q, quer %q", solver.GetKey(), "latam|cli")
	}
}

func TestFilasIsoladasPorChave(t *testing.T) {
	b, qa := newBuckets("a", &fakeSolver{key: "a"})
	b.queues["b"] = fifo.New[Product[any]]()
	b.solvers["b"] = []SolverInterface[any]{&fakeSolver{key: "b", token: "solver-b"}}
	b.stats["b"] = &KeyStats{}
	qa.Push(Product[any]{id: "da-fila-a"})

	got, err := b.GetProduct("b")
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if got.id == "da-fila-a" {
		t.Error("produto da fila de \"a\" apareceu na chave \"b\"")
	}
	if got.token != "solver-b" {
		t.Errorf("token = %v, quer %q (resolvido na hora)", got.token, "solver-b")
	}
}

// --- BaseSolver ---

type embedSolver struct {
	BaseSolver
}

func (*embedSolver) Solve() (any, error) { return nil, nil }

func TestBaseSolver_Key(t *testing.T) {
	s := &embedSolver{BaseSolver{key: "latam|cli"}}

	var _ SolverInterface[any] = s // embutir BaseSolver já satisfaz tudo menos Solve()
	if s.GetKey() != "latam|cli" {
		t.Errorf("GetKey() = %q, quer %q", s.GetKey(), "latam|cli")
	}
}

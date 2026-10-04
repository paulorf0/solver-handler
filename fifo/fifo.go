// Package fifo implementa uma fila FIFO ilimitada, segura para uso
// concorrente por múltiplos produtores e consumidores.
package fifo

import (
	"context"
	"errors"
	"sync"
)

// ErrClosed é devolvido por Push após Close, e por Pop/TryPop quando a
// fila está fechada e vazia.
var ErrClosed = errors.New("fifo: fechada")

const minCap = 16

// Queue é uma fila FIFO baseada em buffer circular. O valor zero não é
// utilizável; crie com New.
type Queue[T any] struct {
	mu     sync.Mutex
	cond   *sync.Cond
	buf    []T
	head   int // índice do primeiro elemento
	size   int // elementos na fila
	closed bool
}

func New[T any]() *Queue[T] {
	q := &Queue[T]{buf: make([]T, minCap)}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Push enfileira x. Nunca bloqueia: a fila cresce conforme necessário.
func (q *Queue[T]) Push(x T) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrClosed
	}
	if q.size == len(q.buf) {
		q.resize(len(q.buf) * 2)
	}
	q.buf[(q.head+q.size)%len(q.buf)] = x
	q.size++
	q.cond.Signal() // acorda UM consumidor
	return nil
}

// Pop bloqueia até haver item, a fila fechar (e esvaziar) ou o ctx cancelar.
// Se houver item disponível, ele é entregue mesmo com o ctx já cancelado,
// para que um Signal de Push nunca se perca.
func (q *Queue[T]) Pop(ctx context.Context) (T, error) {
	// sync.Cond não conhece context: ao cancelar, acordamos todos para reavaliar.
	stop := context.AfterFunc(ctx, func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		q.cond.Broadcast()
	})
	defer stop()

	q.mu.Lock()
	defer q.mu.Unlock()

	for q.size == 0 {
		if q.closed {
			var zero T
			return zero, ErrClosed
		}
		if err := ctx.Err(); err != nil {
			var zero T
			return zero, err
		}
		q.cond.Wait()
	}
	return q.take(), nil
}

// TryPop tenta remover um item sem bloquear. ok é false se a fila estiver
// vazia; err é ErrClosed se estiver fechada e vazia.
func (q *Queue[T]) TryPop() (x T, ok bool, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.size == 0 {
		if q.closed {
			return x, false, ErrClosed
		}
		return x, false, nil
	}
	return q.take(), true, nil
}

// Peek devolve o primeiro item sem removê-lo.
func (q *Queue[T]) Peek() (x T, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.size == 0 {
		return x, false
	}
	return q.buf[q.head], true
}

// Close impede novos Push. Quem já está na fila ainda é consumido;
// depois disso, Pop devolve ErrClosed. Chamar mais de uma vez é seguro.
func (q *Queue[T]) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast()
}

func (q *Queue[T]) Closed() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.closed
}

func (q *Queue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.size
}

// take remove e devolve o primeiro item. Deve ser chamada com o lock
// adquirido e size > 0.
func (q *Queue[T]) take() T {
	var zero T
	x := q.buf[q.head]
	q.buf[q.head] = zero // deixa o GC liberar o elemento
	q.head = (q.head + 1) % len(q.buf)
	q.size--

	// encolhe quando está muito vazia, para não segurar memória à toa
	if len(q.buf) > minCap && q.size <= len(q.buf)/4 {
		q.resize(len(q.buf) / 2)
	}
	return x
}

// resize deve ser chamada com o lock já adquirido.
func (q *Queue[T]) resize(n int) {
	nb := make([]T, n)
	for i := 0; i < q.size; i++ {
		nb[i] = q.buf[(q.head+i)%len(q.buf)]
	}
	q.buf = nb
	q.head = 0
}

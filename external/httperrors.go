package external

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
)

// Common errors when talking to an external system.
var (
	// Network
	ErrTimeout            = errors.New("tempo esgotado na comunicação com o sistema externo")
	ErrConnRefused        = errors.New("conexão recusada pelo sistema externo")
	ErrConnReset          = errors.New("conexão encerrada pelo sistema externo")
	ErrDNS                = errors.New("falha ao resolver o endereço do sistema externo")
	ErrTLS                = errors.New("falha no handshake TLS com o sistema externo")
	ErrNetworkUnreachable = errors.New("rede do sistema externo inacessível")

	// HTTP 4xx
	ErrBadRequest        = errors.New("requisição inválida (400)")
	ErrUnauthorized      = errors.New("não autenticado (401)")
	ErrForbidden         = errors.New("acesso negado (403)")
	ErrNotFound          = errors.New("recurso não encontrado (404)")
	ErrConflict          = errors.New("conflito com o estado do recurso (409)")
	ErrTooManyRequests   = errors.New("limite de requisições excedido (429)")
	ErrProxyAuthRequired = errors.New("autenticação de proxy necessária (407)")

	// HTTP 5xx
	ErrInternal           = errors.New("erro interno do sistema externo (500)")
	ErrBadGateway         = errors.New("resposta inválida do gateway (502)")
	ErrServiceUnavailable = errors.New("sistema externo indisponível (503)")
	ErrGatewayTimeout     = errors.New("tempo esgotado no gateway (504)")

	// Response
	ErrInvalidResponse = errors.New("resposta inválida do sistema externo")
)

// ErrKind classifies a failure by whose fault it is, which sets how to react to it.
type ErrKind int

const (
	KindUnset       ErrKind = iota // not set: derived from the http code
	KindSolver                     // the solver failed: punish it and try another
	KindProxy                      // the proxy failed: drop it, another solver would fail too
	KindTemporary                  // transient: try another solver, do not punish
	KindTargetLimit                // the target limits the key: slow the key down, do not retry
	KindClient                     // invalid client params: do not retry, do not punish
)

// codeKind maps http codes to their kind. Code 0 (no http code, e.g. network or context
// errors) is temporary; codes not listed are the solver's fault.
var codeKind = map[int]ErrKind{
	0:   KindTemporary,
	400: KindClient,
	407: KindProxy,
	422: KindClient,
	429: KindTargetLimit,
	502: KindTemporary,
	503: KindTemporary,
	504: KindTemporary,
}

// IsProxyErr tells whether err is a connection failure (dial, DNS or TLS). Context errors are not.
func IsProxyErr(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var opErr *net.OpError
	var dnsErr *net.DNSError
	var tlsErr *tls.CertificateVerificationError
	return errors.Is(err, ErrConnRefused) ||
		errors.Is(err, ErrConnReset) ||
		errors.Is(err, ErrDNS) ||
		errors.Is(err, ErrTLS) ||
		errors.Is(err, ErrNetworkUnreachable) ||
		(errors.As(err, &opErr) && opErr.Op == "dial") ||
		errors.As(err, &dnsErr) ||
		errors.As(err, &tlsErr)
}

// KindOf returns the kind of a failure with the given http code.
func KindOf(code int) ErrKind {
	if k, ok := codeKind[code]; ok {
		return k
	}
	return KindSolver
}

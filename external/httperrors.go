package external

import "errors"

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

// notSolverError holds the http codes that are not the solver's fault. Code 0 (no http code,
// e.g. network or context errors) is not the solver's fault either.
var notSolverError = map[int]error{
	// TODO: adicionar todos os erros.
	0:   nil,
	502: ErrBadGateway,
	503: ErrServiceUnavailable,
	504: ErrGatewayTimeout,
	407: ErrProxyAuthRequired,
	429: ErrTooManyRequests,
}

// proxyError holds the http codes that only the proxy returns.
var proxyError = map[int]error{
	407: ErrProxyAuthRequired,
}

func ItsSolverError(code int) bool {
	_, ok := notSolverError[code]
	return !ok
}

func ItsProxyError(code int) bool {
	_, ok := proxyError[code]
	return ok
}

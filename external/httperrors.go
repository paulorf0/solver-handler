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
	ErrBadRequest      = errors.New("requisição inválida (400)")
	ErrUnauthorized    = errors.New("não autenticado (401)")
	ErrForbidden       = errors.New("acesso negado (403)")
	ErrNotFound        = errors.New("recurso não encontrado (404)")
	ErrConflict        = errors.New("conflito com o estado do recurso (409)")
	ErrTooManyRequests = errors.New("limite de requisições excedido (429)")

	// HTTP 5xx
	ErrInternal           = errors.New("erro interno do sistema externo (500)")
	ErrBadGateway         = errors.New("resposta inválida do gateway (502)")
	ErrServiceUnavailable = errors.New("sistema externo indisponível (503)")
	ErrGatewayTimeout     = errors.New("tempo esgotado no gateway (504)")

	// Response
	ErrInvalidResponse = errors.New("resposta inválida do sistema externo")
)

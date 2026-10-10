package main

var Version = "v0.1"

// Params is what a client may send besides the key.
type Params struct {
	Proxy string `json:"proxy,omitempty"` // e.g. http://user:pass@host:port
}

// ProxyInfo is what the proxy provider returns; only Proxy is read here.
type ProxyInfo struct {
	Proxy string `json:"proxy"`
}

// Request is what a client asks for. Key picks the solvers; Labels are free attributes
// (client, pool, region...) that other parts, like the proxy, may use.
type Request struct {
	Key    string            `json:"key"`
	Labels map[string]string `json:"labels,omitempty"`
	Params Params            `json:"params,omitzero"`
}

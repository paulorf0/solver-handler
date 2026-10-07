package main

import (
	"encoding/json"
	"solver-handler/keys"
)

var Version = "v0.1"

type Key = keys.Key

// Params is the client data a Solver may need. It must stay an alias, so it keeps RawMessage's JSON methods.
type Params = json.RawMessage

// Request is what a client asks for. Params is opaque here: only the Solver reads it, if it needs to.
type Request struct {
	Key    Key    `json:"Key"`
	Params Params `json:"params,omitempty"`
}

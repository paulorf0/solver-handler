package main

import (
	"solver-handler/keys"
)

var Version = "v0.1"

type Key = keys.Key

type SolverConfig struct {
	adaptiveChoice bool
}

package main

import "solver-handler/fifo"

var Version = "v0.1"

type Key string
type Queue = fifo.Queue[Product[any]]

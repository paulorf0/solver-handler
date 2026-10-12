package external

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	log "log/slog"
	"os"
	"runtime"
	"sync/atomic"
	"time"
)

// Logs is the external system that stores the metrics of every pod and answers queries on them.
// Add queues a log without blocking; Run sends the queue in batches.
type Logs struct {
	*External[[]Log]
	pod     string // identifies this pod in every log it sends
	queue   chan Log
	dropped atomic.Int64 // logs lost with the queue full since the last pod log
}

const (
	logsPath      = "logs"
	logsQueryPath = "logs/query"
	logsFlush     = 5 * time.Second // send period, also the pod log period
	logsBatch     = 500             // logs per POST
	logsQueue     = 10000           // logs waiting to be sent
)

// LogKind says which metric a Log carries.
type LogKind string

const (
	LogRequest    LogKind = "request"    // one GetProduct
	LogGeneration LogKind = "generation" // one Solve, from the stock or solved now
	LogUsage      LogKind = "usage"      // one ReportUsage: did the product work?
	LogStock      LogKind = "stock"      // state of one key in one stock window, from the leader
	LogProxy      LogKind = "proxy"      // a proxy dropped or reported
	LogPod        LogKind = "pod"        // health of the process, sent by Run
)

// Outcomes of a request besides the error kinds.
const (
	OutcomeOK       = "ok"
	OutcomeFail     = "fail" // usage: the product did not work
	OutcomeCanceled = "canceled"
	OutcomeTimeout  = "timeout"
	OutcomeNoKey    = "no_key"
	OutcomeNoSolver = "no_solver"
)

// Origins of a product.
const (
	OriginStock = "stock"
	OriginNow   = "now"
)

// Log is one metric sent to the logs system. Pod and At are set by Add; the other fields are
// filled per Kind and left empty otherwise. Durations are in milliseconds, so queries can
// aggregate them as numbers.
type Log struct {
	Pod    string    `json:"pod"`
	At     time.Time `json:"at"`
	Kind   LogKind   `json:"kind"`
	Key    string    `json:"key,omitempty"`
	Solver string    `json:"solver,omitempty"`

	// request, generation, usage
	Origin    string  `json:"origin,omitempty"`  // OriginStock | OriginNow
	Outcome   string  `json:"outcome,omitempty"` // OutcomeOK, an ErrKind, or another Outcome
	LatencyMs float64 `json:"latencyMs,omitempty"`
	Attempts  int     `json:"attempts,omitempty"` // request: solves tried
	AgeMs     float64 `json:"ageMs,omitempty"`    // usage: product age when used

	// generation: load of the solver when it started; stock: generations in progress
	Inflight int `json:"inflight,omitempty"`
	Limit    int `json:"limit,omitempty"` // concurrent limit of the solver, 0 = none

	// stock
	Rate       float64 `json:"rate,omitempty"`       // requests/s of the key, all pods
	Target     int     `json:"target,omitempty"`     // products the stock aims at
	Pool       int     `json:"pool,omitempty"`       // products in the stock
	Expired    int     `json:"expired,omitempty"`    // products expired unused, counter
	StockLimit float64 `json:"stockLimit,omitempty"` // brake, generations/s
	Waste      float64 `json:"waste,omitempty"`      // moving share of generations that expired
	Sustained  bool    `json:"sustained,omitempty"`  // demand steady enough for the stock
	Tripped    bool    `json:"tripped,omitempty"`    // breaker opened in this window
	Throttled  bool    `json:"throttled,omitempty"`  // target limit braked the key in this window

	// proxy
	ProxyID string `json:"proxyId,omitempty"` // ProxyID of the proxy, never its URL

	// pod
	Goroutines int     `json:"goroutines,omitempty"`
	HeapMB     float64 `json:"heapMB,omitempty"`
	Dropped    int64   `json:"dropped,omitempty"` // logs lost with the queue full
}

// LogQuery filters the logs of a query. Empty fields match everything.
type LogQuery struct {
	Kind   LogKind   `json:"kind,omitempty"`
	Key    string    `json:"key,omitempty"`
	Solver string    `json:"solver,omitempty"`
	Pod    string    `json:"pod,omitempty"`
	From   time.Time `json:"from"`
	To     time.Time `json:"to"`
	Limit  int       `json:"limit,omitempty"`
}

// NewLogs returns the client of the logs system for this pod; PodName gives the usual pod.
func NewLogs(rawURL string, token *string, pod string) (*Logs, error) {
	b, err := New[[]Log](rawURL, token)
	if err != nil {
		return nil, err
	}
	return &Logs{External: b, pod: pod, queue: make(chan Log, logsQueue)}, nil
}

// PodName identifies this pod: POD_NAME when set, else the hostname, which is the pod name on
// Kubernetes and unique per task on ECS.
func PodName() string {
	if name := os.Getenv("POD_NAME"); name != "" {
		return name
	}
	if name, err := os.Hostname(); err == nil {
		return name
	}
	return "desconhecido"
}

// Add queues l to be sent. It never blocks: with the queue full the log is dropped and counted.
func (l *Logs) Add(lg Log) {
	lg.Pod = l.pod
	if lg.At.IsZero() {
		lg.At = time.Now()
	}
	select {
	case l.queue <- lg:
	default:
		l.dropped.Add(1)
	}
}

// Run sends the queued logs every logsFlush, or sooner when a batch fills, plus one pod log per
// period. On ctx end it sends what is batched and returns.
func (l *Logs) Run(ctx context.Context) {
	ticker := time.NewTicker(logsFlush)
	defer ticker.Stop()
	batch := make([]Log, 0, logsBatch)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		sendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), logsFlush)
		defer cancel()
		if err := l.Send(sendCtx, batch); err != nil {
			log.Warn("falha ao enviar logs", "n", len(batch), "err", err)
		}
		batch = batch[:0]
	}
	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case lg := <-l.queue:
			if batch = append(batch, lg); len(batch) >= logsBatch {
				flush()
			}
		case <-ticker.C:
			batch = append(batch, l.podLog())
			flush()
		}
	}
}

// Send posts logs to the logs system now.
func (l *Logs) Send(ctx context.Context, logs []Log) error {
	_, err := l.Post(ctx, logsPath, logs)
	return err
}

// Query returns the logs that match q.
func (l *Logs) Query(ctx context.Context, q LogQuery) ([]Log, error) {
	return l.Post(ctx, logsQueryPath, q)
}

// podLog is the health of this process.
func (l *Logs) podLog() Log {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return Log{Pod: l.pod, At: time.Now(), Kind: LogPod, Goroutines: runtime.NumGoroutine(),
		HeapMB: float64(m.HeapAlloc) / (1 << 20), Dropped: l.dropped.Swap(0)}
}

// ProxyID is a short hash of proxy, so a log can tell proxies apart without its credentials.
func ProxyID(proxy string) string {
	sum := sha256.Sum256([]byte(proxy))
	return hex.EncodeToString(sum[:4])
}

// String is the name of the error kind in a log Outcome; KindUnset is a success.
func (k ErrKind) String() string {
	switch k {
	case KindSolver:
		return "solver"
	case KindProxy:
		return "proxy"
	case KindTemporary:
		return "temporary"
	case KindTargetLimit:
		return "target_limit"
	case KindClient:
		return "client"
	}
	return OutcomeOK
}

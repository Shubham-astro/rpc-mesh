package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/shubham-astro/rpc-mesh/router"
)

// knownMethods bounds label cardinality on `method`.
//
// The method name comes from the request body, i.e. it is attacker-controlled.
// Without an allowlist, a loop posting {"method":"random-string-N"} mints a new
// time series per request until the TSDB dies. Anything unrecognized becomes
// "other".
var knownMethods = map[string]struct{}{
	"getAccountInfo": {}, "getBalance": {}, "getBlock": {}, "getBlockCommitment": {},
	"getBlockHeight": {}, "getBlockProduction": {}, "getBlockTime": {}, "getBlocks": {},
	"getBlocksWithLimit": {}, "getClusterNodes": {}, "getEpochInfo": {},
	"getEpochSchedule": {}, "getFeeForMessage": {}, "getFirstAvailableBlock": {},
	"getGenesisHash": {}, "getHealth": {}, "getHighestSnapshotSlot": {},
	"getIdentity": {}, "getInflationGovernor": {}, "getInflationRate": {},
	"getInflationReward": {}, "getLargestAccounts": {}, "getLatestBlockhash": {},
	"getLeaderSchedule": {}, "getMaxRetransmitSlot": {}, "getMaxShredInsertSlot": {},
	"getMinimumBalanceForRentExemption": {}, "getMultipleAccounts": {},
	"getProgramAccounts": {}, "getRecentPerformanceSamples": {},
	"getRecentPrioritizationFees": {}, "getSignatureStatuses": {},
	"getSignaturesForAddress": {}, "getSlot": {}, "getSlotLeader": {},
	"getSlotLeaders": {}, "getStakeMinimumDelegation": {}, "getSupply": {},
	"getTokenAccountBalance": {}, "getTokenAccountsByDelegate": {},
	"getTokenAccountsByOwner": {}, "getTokenLargestAccounts": {}, "getTokenSupply": {},
	"getTransaction": {}, "getTransactionCount": {}, "getVersion": {},
	"getVoteAccounts": {}, "isBlockhashValid": {}, "minimumLedgerSlot": {},
	"requestAirdrop": {}, "sendTransaction": {}, "simulateTransaction": {},
	// Sentinels produced by peekMethod itself.
	"batch": {}, "unknown": {},
}

func normalizeMethod(m string) string {
	if _, ok := knownMethods[m]; ok {
		return m
	}
	return "other"
}

// knownRPCErrorCodes bounds cardinality on the `code` label. The code comes
// from the upstream rather than the client, so the exposure is smaller than
// with method names — but a misbehaving or hostile provider could still emit
// arbitrary integers, and the same allowlist discipline costs nothing.
//
// -32700..-32600 are standard JSON-RPC; -32016..-32001 are Solana's.
var knownRPCErrorCodes = map[int]struct{}{
	-32700: {}, // parse error
	-32600: {}, // invalid request
	-32601: {}, // method not found
	-32602: {}, // invalid params
	-32603: {}, // internal error
	-32001: {}, // block cleaned up
	-32002: {}, // send transaction preflight failure
	-32003: {}, // transaction signature verification failure
	-32004: {}, // block not available
	-32005: {}, // node unhealthy
	-32006: {}, // transaction precompile verification failure
	-32007: {}, // slot skipped
	-32008: {}, // no snapshot
	-32009: {}, // long-term storage slot skipped
	-32010: {}, // key excluded from secondary index
	-32011: {}, // transaction history not available
	-32012: {}, // scan error
	-32013: {}, // transaction signature length mismatch
	-32014: {}, // block status not yet available
	-32015: {}, // unsupported transaction version
	-32016: {}, // minimum context slot not reached
}

func codeLabel(code int) string {
	if _, ok := knownRPCErrorCodes[code]; ok {
		return strconv.Itoa(code)
	}
	return "other"
}

// Metrics implements router.ProxyStats.
type Metrics struct {
	registry *prometheus.Registry

	requests  *prometheus.CounterVec
	duration  *prometheus.HistogramVec
	errors    *prometheus.CounterVec
	rpcErrors *prometheus.CounterVec
	degraded  *prometheus.CounterVec
	retries   *prometheus.CounterVec
}

func New(pool *router.Pool) *Metrics {
	// A private registry rather than the default one. The default is package
	// global — any dependency that calls MustRegister at init can panic on a
	// duplicate, and tests that build two Metrics values collide. Explicit
	// registry, explicit collectors, no action at a distance.
	reg := prometheus.NewRegistry()

	m := &Metrics{
		registry: reg,

		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rpcmesh_requests_total",
			Help: "Total proxied JSON-RPC requests by upstream endpoint, method and HTTP response status.",
		}, []string{"endpoint", "method", "status"}),

		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "rpcmesh_request_duration_seconds",
			Help: "End-to-end proxied request latency, including any retry.",
			// Default buckets top out at 10s and are far too coarse at the
			// low end. Solana reads land between 20ms and 300ms, which the
			// defaults compress into two buckets. These resolve the range
			// that matters and still catch the tail.
			Buckets: []float64{
				0.005, 0.010, 0.025, 0.050, 0.075, 0.100,
				0.150, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0,
			},
		}, []string{"endpoint", "method"}),

		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rpcmesh_upstream_errors_total",
			Help: "Transport or HTTP-level upstream failures by reason (timeout, rate_limited, upstream_5xx, transport, no_endpoints).",
		}, []string{"endpoint", "method", "reason"}),

		rpcErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rpcmesh_rpc_errors_total",
			Help: "JSON-RPC application errors returned inside a 2xx response body. These are invisible to rpcmesh_requests_total, which only sees HTTP status.",
		}, []string{"endpoint", "method", "code"}),

		degraded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rpcmesh_degraded_requests_total",
			Help: "Requests served by a slot-lagging endpoint because no current endpoint was available.",
		}, []string{"endpoint", "method"}),

		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rpcmesh_retries_total",
			Help: "Read requests retried on a different endpoint after an upstream failure.",
		}, []string{"from_endpoint", "to_endpoint", "method"}),
	}

	reg.MustRegister(
		m.requests, m.duration, m.errors, m.rpcErrors, m.degraded, m.retries,
		newPoolCollector(pool),
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		// Surface collector bugs during development instead of silently
		// serving a partial scrape.
		ErrorHandling: promhttp.HTTPErrorOnError,
	})
}

// --- router.ProxyStats ---

func (m *Metrics) ObserveRequest(endpoint, method string, status int, d time.Duration) {
	mm := normalizeMethod(method)
	m.requests.WithLabelValues(endpoint, mm, statusLabel(status)).Inc()
	m.duration.WithLabelValues(endpoint, mm).Observe(d.Seconds())
}

func (m *Metrics) ObserveUpstreamError(endpoint, method, reason string) {
	m.errors.WithLabelValues(endpoint, normalizeMethod(method), reason).Inc()
}

func (m *Metrics) ObserveRPCError(endpoint, method string, code int) {
	m.rpcErrors.WithLabelValues(endpoint, normalizeMethod(method), codeLabel(code)).Inc()
}

func (m *Metrics) ObserveDegraded(endpoint, method string) {
	m.degraded.WithLabelValues(endpoint, normalizeMethod(method)).Inc()
}

func (m *Metrics) ObserveRetry(from, to, method string) {
	m.retries.WithLabelValues(from, to, normalizeMethod(method)).Inc()
}

// statusLabel keeps the status label to a handful of values. Exact codes add
// cardinality without adding meaning — nobody alerts on 502 vs 504
// specifically, and the reason label on errors already carries that detail.
func statusLabel(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return "4xx"
	case code >= 200 && code < 300:
		return "2xx"
	default:
		return "other"
	}
}
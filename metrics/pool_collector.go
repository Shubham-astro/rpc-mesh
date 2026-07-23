package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/shubham-astro/rpc-mesh/router"
)

// poolCollector exposes endpoint state as gauges, read at scrape time.
//
// The alternative — a GaugeVec the health checker pushes into every cycle —
// duplicates state that already lives in the pool, and the copy is stale
// between cycles. Reading through Snapshot() on scrape means the gauge is
// always current by construction and there is no second source of truth to
// keep in sync.
//
// The tradeoff: scrape latency now depends on acquiring the pool's read lock.
// That's an RLock over a handful of endpoints, so it's nothing — but it is the
// reason you would not do this if Collect had to touch a database.
type poolCollector struct {
	pool *router.Pool

	healthy   *prometheus.Desc
	slot      *prometheus.Desc
	slotLag   *prometheus.Desc
	latency   *prometheus.Desc
	checkAge  *prometheus.Desc
	maxSlot   *prometheus.Desc
}

func newPoolCollector(pool *router.Pool) *poolCollector {
	return &poolCollector{
		pool: pool,
		healthy: prometheus.NewDesc(
			"rpcmesh_endpoint_healthy",
			"1 if the endpoint is currently in the routable set, 0 if ejected.",
			[]string{"endpoint"}, nil),
		slot: prometheus.NewDesc(
			"rpcmesh_endpoint_slot",
			"Most recent slot reported by the endpoint.",
			[]string{"endpoint"}, nil),
		slotLag: prometheus.NewDesc(
			"rpcmesh_endpoint_slot_lag",
			"Slots behind the highest slot observed across the pool.",
			[]string{"endpoint"}, nil),
		latency: prometheus.NewDesc(
			"rpcmesh_endpoint_probe_latency_seconds",
			"EWMA-smoothed health probe latency used for routing decisions.",
			[]string{"endpoint"}, nil),
		checkAge: prometheus.NewDesc(
			"rpcmesh_endpoint_last_check_age_seconds",
			"Seconds since this endpoint was last probed. Rising steadily means the health checker is stuck.",
			[]string{"endpoint"}, nil),
		maxSlot: prometheus.NewDesc(
			"rpcmesh_pool_max_slot",
			"Highest slot observed across all healthy endpoints; the reference point for slot lag.",
			nil, nil),
	}
}

func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.healthy
	ch <- c.slot
	ch <- c.slotLag
	ch <- c.latency
	ch <- c.checkAge
	ch <- c.maxSlot
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	now := time.Now()

	for _, st := range c.pool.Snapshot() {
		g := func(d *prometheus.Desc, v float64) {
			ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, st.Name)
		}

		healthy := 0.0
		if st.Healthy {
			healthy = 1.0
		}
		g(c.healthy, healthy)
		g(c.slot, float64(st.Slot))
		g(c.slotLag, float64(st.SlotLag))
		g(c.latency, st.EWMALatency.Seconds())

		if !st.LastChecked.IsZero() {
			g(c.checkAge, now.Sub(st.LastChecked).Seconds())
		}
	}

	ch <- prometheus.MustNewConstMetric(c.maxSlot, prometheus.GaugeValue,
		float64(c.pool.MaxSlot()))
}
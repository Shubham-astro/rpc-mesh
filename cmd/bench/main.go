// Command bench measures proxied RPC latency against each upstream endpoint
// individually and through rpc-mesh, so the routing decision can be evaluated
// against the alternatives a developer would otherwise pick.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type config struct {
	meshURL   string
	endpoints []string
	requests  int
	conc      int
	method    string
	warmup    int
	timeout   time.Duration
	markdown  bool
}

// result is one target's measured distribution.
type result struct {
	name      string
	latencies []time.Duration
	errors    int
	rpcErrors int
	wall      time.Duration
}

func main() {
	var (
		meshURL   = flag.String("mesh", "http://localhost:8080", "rpc-mesh URL")
		endpoints = flag.String("endpoints", "", "comma-separated upstream URLs to benchmark individually")
		requests  = flag.Int("n", 300, "requests per target")
		conc      = flag.Int("c", 10, "concurrent workers")
		method    = flag.String("method", "getSlot", "JSON-RPC method to call")
		warmup    = flag.Int("warmup", 20, "warmup requests per target, discarded")
		timeout   = flag.Duration("timeout", 15*time.Second, "per-request timeout")
		markdown  = flag.Bool("markdown", false, "emit a markdown table for the README")
	)
	flag.Parse()

	if *endpoints == "" {
		fmt.Fprintln(os.Stderr, "error: -endpoints is required")
		fmt.Fprintln(os.Stderr, "\nexample:")
		fmt.Fprintln(os.Stderr, `  go run ./cmd/bench -endpoints "https://a.com,https://b.com" -n 300 -c 10`)
		os.Exit(1)
	}

	cfg := config{
		meshURL:  *meshURL,
		requests: *requests,
		conc:     *conc,
		method:   *method,
		warmup:   *warmup,
		timeout:  *timeout,
		markdown: *markdown,
	}
	for _, e := range strings.Split(*endpoints, ",") {
		if e = strings.TrimSpace(e); e != "" {
			cfg.endpoints = append(cfg.endpoints, e)
		}
	}

	var results []result

	// Each upstream on its own — the baseline a developer gets by hardcoding
	// one provider.
	for _, ep := range cfg.endpoints {
		fmt.Fprintf(os.Stderr, "benchmarking %s ...\n", shortName(ep))
		results = append(results, run(cfg, shortName(ep), ep))
	}

	fmt.Fprintf(os.Stderr, "benchmarking rpc-mesh ...\n")
	mesh := run(cfg, "rpc-mesh", cfg.meshURL)
	results = append(results, mesh)

	if cfg.markdown {
		printMarkdown(results, mesh, cfg)
	} else {
		printTable(results, mesh, cfg)
	}
}

// run drives one target and returns its latency distribution.
//
// Closed-loop: each worker waits for a response before issuing the next
// request. This understates tail latency — when the server slows down, the
// offered load drops with it, so the slowest moments are under-sampled. That
// is "coordinated omission", and an open-loop generator issuing at a fixed
// rate regardless of response time is the rigorous fix. Closed-loop is
// acceptable here because every target is measured the same way, so the
// comparison stays valid even though the absolute numbers are optimistic.
func run(cfg config, name, target string) result {
	client := newClient(cfg.timeout)
	// Connections are per-Client, so each target gets a fresh pool. Sharing
	// one client would let an earlier target's warm connections flatter a
	// later one.
	defer client.CloseIdleConnections()

	body := requestBody(cfg.method)

	// Warmup is not optional. The first request to a host pays DNS, TCP, and
	// TLS — 100-200ms to a distant node — and including it would make every
	// target look worse in proportion to how few requests we sent.
	for i := 0; i < cfg.warmup; i++ {
		_, _, _ = doRequest(context.Background(), client, target, body)
	}

	// One pre-sized slice per worker, no shared state, no mutex. Merged after
	// Wait, which establishes the happens-before that makes the writes visible.
	per := cfg.requests / cfg.conc
	buckets := make([][]time.Duration, cfg.conc)
	errCounts := make([]int, cfg.conc)
	rpcErrCounts := make([]int, cfg.conc)

	var wg sync.WaitGroup
	start := time.Now()

	for w := 0; w < cfg.conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lat := make([]time.Duration, 0, per)

			for i := 0; i < per; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
				d, rpcErr, err := doRequest(ctx, client, target, body)
				cancel()

				switch {
				case err != nil:
					errCounts[w]++
				case rpcErr:
					// Counted separately: the request succeeded at the
					// transport layer but the call failed. Including its
					// latency would be misleading — error responses are fast.
					rpcErrCounts[w]++
				default:
					lat = append(lat, d)
				}
			}
			buckets[w] = lat
		}()
	}

	wg.Wait()
	wall := time.Since(start)

	res := result{name: name, wall: wall}
	for w := 0; w < cfg.conc; w++ {
		res.latencies = append(res.latencies, buckets[w]...)
		res.errors += errCounts[w]
		res.rpcErrors += rpcErrCounts[w]
	}
	sort.Slice(res.latencies, func(i, j int) bool {
		return res.latencies[i] < res.latencies[j]
	})
	return res
}

// doRequest issues one call and reports its latency and whether the body
// carried a JSON-RPC error.
func doRequest(ctx context.Context, c *http.Client, target string, body []byte) (time.Duration, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return 0, false, err
	}
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := c.Do(req)
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()

	// The body MUST be read to completion before Close, or the connection
	// cannot be returned to the idle pool — Go has to drain it to find the
	// start of the next response. Skipping this silently disables keep-alive
	// and every subsequent request pays a fresh handshake. It is the single
	// most common way a Go benchmark measures the wrong thing.
	payload, readErr := io.ReadAll(resp.Body)
	elapsed := time.Since(start)

	if readErr != nil {
		return 0, false, readErr
	}
	if resp.StatusCode >= 400 {
		return 0, false, fmt.Errorf("http %d", resp.StatusCode)
	}

	// JSON-RPC reports application failures inside a 200. Without checking,
	// a provider rejecting every call looks like a perfect success rate at a
	// suspiciously low latency.
	var peek struct {
		Error *struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(payload, &peek); err == nil && peek.Error != nil {
		return elapsed, true, nil
	}

	return elapsed, false, nil
}

// newClient mirrors the proxy's transport settings.
//
// This matters for fairness: benchmarking upstreams with the default
// MaxIdleConnsPerHost of 2 while rpc-mesh uses 100 would measure connection
// pooling, not routing, and hand the mesh a win it did not earn.
func newClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 100,
			MaxIdleConns:        200,
			IdleConnTimeout:     90 * time.Second,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout: 5 * time.Second,
			ForceAttemptHTTP2:   true,
		},
	}
}

func requestBody(method string) []byte {
	b, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
	})
	return b
}

func shortName(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return raw
	}
	return u.Hostname()
}

// percentile uses the nearest-rank method: the smallest value at or below
// which p percent of observations fall. No interpolation — with a few hundred
// samples, interpolating between two measurements invents precision that isn't
// in the data.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted))*p/100.0+0.5) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func mean(ds []time.Duration) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	var total time.Duration
	for _, d := range ds {
		total += d
	}
	return total / time.Duration(len(ds))
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// comparison holds the three claims worth making, computed from the raw data.
type comparison struct {
	fastestName    string
	fastestP50     time.Duration
	naiveP50       time.Duration // mean of endpoint p50s: expected value of picking one at random
	meshP50        time.Duration
	vsNaivePercent float64
	vsFastPercent  float64
	naiveP99       time.Duration
	meshP99        time.Duration
	vsNaiveP99     float64
}

func compare(results []result, mesh result) comparison {
	var c comparison
	c.meshP50 = percentile(mesh.latencies, 50)
	c.meshP99 = percentile(mesh.latencies, 99)

	var sumP50, sumP99 time.Duration
	var n int
	best := time.Duration(1<<63 - 1)

	for _, r := range results {
		if r.name == "rpc-mesh" || len(r.latencies) == 0 {
			continue
		}
		p50 := percentile(r.latencies, 50)
		sumP50 += p50
		sumP99 += percentile(r.latencies, 99)
		n++
		if p50 < best {
			best = p50
			c.fastestName = r.name
			c.fastestP50 = p50
		}
	}
	if n == 0 {
		return c
	}

	c.naiveP50 = sumP50 / time.Duration(n)
	c.naiveP99 = sumP99 / time.Duration(n)

	if c.naiveP50 > 0 {
		c.vsNaivePercent = (1 - float64(c.meshP50)/float64(c.naiveP50)) * 100
	}
	if c.fastestP50 > 0 {
		c.vsFastPercent = (1 - float64(c.meshP50)/float64(c.fastestP50)) * 100
	}
	if c.naiveP99 > 0 {
		c.vsNaiveP99 = (1 - float64(c.meshP99)/float64(c.naiveP99)) * 100
	}
	return c
}

func printTable(results []result, mesh result, cfg config) {
	fmt.Printf("\nmethod=%s  requests=%d  concurrency=%d  warmup=%d\n\n",
		cfg.method, cfg.requests, cfg.conc, cfg.warmup)

	fmt.Printf("%-32s %8s %8s %8s %8s %8s %7s %7s\n",
		"target", "p50", "p95", "p99", "max", "mean", "errs", "rpcerr")
	fmt.Println(strings.Repeat("-", 96))

	for _, r := range results {
		if len(r.latencies) == 0 {
			fmt.Printf("%-32s %8s %8s %8s %8s %8s %7d %7d\n",
				r.name, "-", "-", "-", "-", "-", r.errors, r.rpcErrors)
			continue
		}
		fmt.Printf("%-32s %8.1f %8.1f %8.1f %8.1f %8.1f %7d %7d\n",
			r.name,
			ms(percentile(r.latencies, 50)),
			ms(percentile(r.latencies, 95)),
			ms(percentile(r.latencies, 99)),
			ms(r.latencies[len(r.latencies)-1]),
			ms(mean(r.latencies)),
			r.errors, r.rpcErrors)
	}

	c := compare(results, mesh)
	fmt.Println()
	fmt.Println("all latencies in milliseconds")
	fmt.Println()

	fmt.Printf("vs. a randomly chosen endpoint (p50 %.1fms):  %+.1f%%\n",
		ms(c.naiveP50), c.vsNaivePercent)
	fmt.Printf("vs. the fastest endpoint, %s (p50 %.1fms):  %+.1f%%\n",
		c.fastestName, ms(c.fastestP50), c.vsFastPercent)
	fmt.Printf("p99 vs. a randomly chosen endpoint (%.1fms):  %+.1f%%\n",
		ms(c.naiveP99), c.vsNaiveP99)
	fmt.Println()
	fmt.Println("rpc-mesh adds a network hop, so it cannot beat the fastest endpoint —")
	fmt.Println("the second line should be slightly negative. The first is the real")
	fmt.Println("comparison: what a developer gets by hardcoding one provider.")
}

func printMarkdown(results []result, mesh result, cfg config) {
	fmt.Printf("Benchmarked with `%s`, %d requests at concurrency %d, %d discarded warmup requests.\n\n",
		cfg.method, cfg.requests, cfg.conc, cfg.warmup)

	fmt.Println("| Target | p50 | p95 | p99 | Errors |")
	fmt.Println("|---|---|---|---|---|")
	for _, r := range results {
		if len(r.latencies) == 0 {
			fmt.Printf("| %s | — | — | — | %d |\n", r.name, r.errors+r.rpcErrors)
			continue
		}
		fmt.Printf("| %s | %.0fms | %.0fms | %.0fms | %d |\n",
			r.name,
			ms(percentile(r.latencies, 50)),
			ms(percentile(r.latencies, 95)),
			ms(percentile(r.latencies, 99)),
			r.errors+r.rpcErrors)
	}

	c := compare(results, mesh)
	fmt.Printf("\nAgainst a randomly chosen endpoint, rpc-mesh reduces p50 by **%.0f%%** "+
		"and p99 by **%.0f%%**. It does not beat the single fastest endpoint (%s) — "+
		"it adds a hop — but it finds that endpoint without you knowing which one it is, "+
		"and keeps working when it degrades.\n",
		c.vsNaivePercent, c.vsNaiveP99, c.fastestName)
}
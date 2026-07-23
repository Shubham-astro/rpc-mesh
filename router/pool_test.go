package router

import (
	"errors"
	"testing"
	"time"
)

func testPool(t *testing.T, eps ...*Endpoint) *Pool {
	t.Helper()
	return &Pool{
		endpoints:  eps,
		maxSlotLag: 50,
		ewmaAlpha:  0.2,
		failThresh: 3,
		okThresh:   2,
	}
}

func ep(name string, healthy bool, slot uint64, latency time.Duration) *Endpoint {
	return &Endpoint{
		URL:         "https://" + name + ".example.com",
		Name:        name,
		healthy:     healthy,
		slot:        slot,
		ewmaLatency: latency,
	}
}

// Every case here filters down to one or two candidates, where power-of-two-
// choices degenerates to a direct comparison and selection stays deterministic.
// Behavior with three or more candidates is covered in
// TestPowerOfTwoChoicesSpreadsLoad.
func TestSelectExcluding(t *testing.T) {
	fast := ep("fast", true, 1000, 20*time.Millisecond)
	slow := ep("slow", true, 1000, 200*time.Millisecond)
	down := ep("down", false, 1000, 5*time.Millisecond)
	lagging := ep("lagging", true, 900, 5*time.Millisecond)

	tests := []struct {
		name         string
		endpoints    []*Endpoint
		maxSlot      uint64
		exclude      *Endpoint
		wantName     string
		wantDegraded bool
		wantErr      error
	}{
		{
			name:      "all healthy picks fastest",
			endpoints: []*Endpoint{slow, fast},
			maxSlot:   1000,
			wantName:  "fast",
		},
		{
			name:      "unhealthy skipped even if fastest",
			endpoints: []*Endpoint{down, fast, slow},
			maxSlot:   1000,
			wantName:  "fast",
		},
		{
			name:      "excluded endpoint skipped on retry",
			endpoints: []*Endpoint{fast, slow},
			maxSlot:   1000,
			exclude:   fast,
			wantName:  "slow",
		},
		{
			name:      "lagging endpoint skipped when a current one exists",
			endpoints: []*Endpoint{lagging, slow},
			maxSlot:   1000,
			wantName:  "slow",
		},
		{
			name:         "all lagging degrades rather than failing",
			endpoints:    []*Endpoint{lagging},
			maxSlot:      1000,
			wantName:     "lagging",
			wantDegraded: true,
		},
		{
			name:      "no healthy endpoints returns error",
			endpoints: []*Endpoint{down},
			maxSlot:   1000,
			wantErr:   ErrNoEndpoints,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := testPool(t, tt.endpoints...)
			p.maxSlot = tt.maxSlot

			got, degraded, err := p.SelectExcluding("getSlot", tt.exclude)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Name != tt.wantName {
				t.Errorf("selected %q, want %q", got.Name, tt.wantName)
			}
			if degraded != tt.wantDegraded {
				t.Errorf("degraded = %v, want %v", degraded, tt.wantDegraded)
			}
		})
	}
}

func TestUnprobedEndpointDoesNotBeatMeasuredOne(t *testing.T) {
	// Regression guard: ewmaLatency == 0 must mean "unknown", not "instant".
	measured := ep("measured", true, 1000, 50*time.Millisecond)
	unprobed := ep("unprobed", true, 1000, 0)

	p := testPool(t, unprobed, measured)
	p.maxSlot = 1000

	got, _, err := p.SelectExcluding("getSlot", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Name != "measured" {
		t.Errorf("selected %q, want measured", got.Name)
	}
}

func TestPowerOfTwoChoicesSpreadsLoad(t *testing.T) {
	// With three candidates, P2C samples two distinct endpoints and routes to
	// the faster. The three unordered pairs are equally likely, so the
	// outcome distribution is exact rather than merely probabilistic:
	//
	//   (fast, mid)  -> fast
	//   (fast, slow) -> fast
	//   (mid,  slow) -> mid
	//
	// fast wins 2/3, mid wins 1/3, slow never wins. That is the point: load
	// spreads instead of one endpoint absorbing everything until it rate
	// limits, while the tail stays close to always-pick-the-fastest.
	fast := ep("fast", true, 1000, 10*time.Millisecond)
	mid := ep("mid", true, 1000, 100*time.Millisecond)
	slow := ep("slow", true, 1000, 500*time.Millisecond)

	p := testPool(t, fast, mid, slow)
	p.maxSlot = 1000

	const n = 3000
	counts := map[string]int{}
	for i := 0; i < n; i++ {
		got, _, err := p.Select("getSlot")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		counts[got.Name]++
	}

	// Expected 2000; a wide band keeps this from flaking on randomness.
	if counts["fast"] < n*55/100 || counts["fast"] > n*78/100 {
		t.Errorf("fast selected %d/%d, expected roughly two thirds", counts["fast"], n)
	}
	if counts["mid"] == 0 {
		t.Error("mid never selected — load is not spreading across the pool")
	}
	if counts["slow"] != 0 {
		t.Errorf("slowest selected %d times; it loses every pairing it can appear in", counts["slow"])
	}
}

func TestColdStartSpreadsAcrossUnprobedEndpoints(t *testing.T) {
	// At startup every endpoint has ewma == 0, so all tie at maxDuration.
	// The tie-break must be random or the first endpoint in the config takes
	// all traffic until the first health cycle completes.
	a := ep("a", true, 1000, 0)
	b := ep("b", true, 1000, 0)
	c := ep("c", true, 1000, 0)

	p := testPool(t, a, b, c)
	p.maxSlot = 1000

	counts := map[string]int{}
	for i := 0; i < 900; i++ {
		got, _, err := p.Select("getSlot")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		counts[got.Name]++
	}

	for _, name := range []string{"a", "b", "c"} {
		if counts[name] < 150 {
			t.Errorf("%s selected only %d/900 times; cold-start load is not spreading",
				name, counts[name])
		}
	}
}

func TestHysteresis(t *testing.T) {
	e := ep("flaky", true, 1000, 10*time.Millisecond)
	p := testPool(t, e)
	boom := errors.New("timeout")

	for i := 1; i <= 2; i++ {
		p.RecordProbe(ProbeResult{Endpoint: e, Err: boom})
		if !e.healthy {
			t.Fatalf("ejected after %d failures, threshold is 3", i)
		}
	}

	p.RecordProbe(ProbeResult{Endpoint: e, Err: boom})
	if e.healthy {
		t.Fatal("should be ejected after 3 consecutive failures")
	}

	p.RecordProbe(ProbeResult{Endpoint: e, Slot: 1001, Latency: 10 * time.Millisecond})
	if e.healthy {
		t.Fatal("readmitted after 1 success, threshold is 2")
	}

	p.RecordProbe(ProbeResult{Endpoint: e, Slot: 1002, Latency: 10 * time.Millisecond})
	if !e.healthy {
		t.Fatal("should be readmitted after 2 consecutive successes")
	}
}

func TestEWMASeeding(t *testing.T) {
	e := ep("new", true, 0, 0)
	p := testPool(t, e)

	p.RecordProbe(ProbeResult{Endpoint: e, Slot: 100, Latency: 100 * time.Millisecond})
	if e.ewmaLatency != 100*time.Millisecond {
		t.Fatalf("first sample should seed directly, got %v", e.ewmaLatency)
	}

	// 0.2*200 + 0.8*100 = 120ms
	p.RecordProbe(ProbeResult{Endpoint: e, Slot: 101, Latency: 200 * time.Millisecond})
	if e.ewmaLatency != 120*time.Millisecond {
		t.Fatalf("ewma = %v, want 120ms", e.ewmaLatency)
	}
}
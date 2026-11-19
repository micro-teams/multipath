package multipath

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"
)

// waitFor polls cond until it holds or the deadline passes, failing the test with msg on timeout.
func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

// TestObserveLinkStateAndStats checks that link up/down transitions are surfaced through OnLinkState
// and Stats: all links come up, a cut link is reported down with a reason while the stream keeps
// carrying bytes on the survivors, and a healed link comes back up.
func TestObserveLinkStateAndStats(t *testing.T) {
	f := newFabric(3)
	var mu sync.Mutex
	var events []LinkState
	ctx := context.Background()

	copts := fastOpts(3, f.clientDial)
	copts.OnLinkState = func(e LinkState) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	}

	var cli, srv *RedundantStream
	var cerr, serr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); srv, serr = DialRedundant(ctx, fastOpts(3, f.serverDial)) }()
	go func() { defer wg.Done(); cli, cerr = DialRedundant(ctx, copts) }()
	wg.Wait()
	if cerr != nil || serr != nil {
		t.Fatalf("dial: cli=%v srv=%v", cerr, serr)
	}
	defer cli.Close()
	defer srv.Close()

	// All three links should reach "up", and each should have produced an up event.
	waitFor(t, func() bool {
		up := 0
		for _, s := range cli.Stats() {
			if s.State == "up" {
				up++
			}
		}
		return up == 3
	}, "all three links up")

	mu.Lock()
	upSeen := map[int]bool{}
	for _, e := range events {
		if e.Up {
			upSeen[e.Index] = true
		}
	}
	mu.Unlock()
	if !(upSeen[0] && upSeen[1] && upSeen[2]) {
		t.Fatalf("missing up events, saw %v", upSeen)
	}

	// Cut link 0. It must be reported down with a non-empty reason, and the stream must keep working
	// on the two survivors.
	f.pol[0].blackhole.Store(true)
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range events {
			if e.Index == 0 && !e.Up && e.Reason != "" {
				return true
			}
		}
		return false
	}, "link 0 reported down with a reason")

	payload := randBytes(4000)
	go func() { _, _ = cli.Write(payload) }()
	got := recvN(t, srv, len(payload))
	if !bytes.Equal(got, payload) {
		t.Fatal("stream corrupted while a link was down")
	}

	// Heal link 0. It should reconnect and return to "up" (Reconnects advances as it recovers).
	f.pol[0].blackhole.Store(false)
	waitFor(t, func() bool {
		for _, s := range cli.Stats() {
			if s.Index == 0 && s.State == "up" && s.Reconnects >= 1 {
				return true
			}
		}
		return false
	}, "link 0 recovered")
}

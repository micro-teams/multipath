// Conformance scenarios: the real client and server over fault-injecting lines, with production
// timeouts. Each scenario states what it reproduces and what it requires. The three stalls of the
// version-2 implementation (a replay overtaken by a newer frame, a line whose reader stopped, a
// server restart under a live session) must recover, either without the application noticing or
// by ending the session within about two seconds and starting a new one. A jittery line must keep
// its link, and a switch of every line at once must not cost the session or its streams.
package conformance

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/micro-teams/multipath/conformance/faultproxy"
)

// Requests of every shape over three healthy lines arrive intact, and nothing is left behind.
func TestBaseline(t *testing.T) {
	s := newStack(t, 3)
	base := s.mustServer().Goroutines
	for range 200 {
		if string(s.mustGet("/small")) != "ok" {
			t.Fatal("wrong answer")
		}
	}
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.big(256<<10, 30*time.Second); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	start := time.Now()
	if err := s.big(64<<20, time.Minute); err != nil {
		t.Fatal(err)
	}
	down := time.Since(start)
	start = time.Now()
	if err := s.echo(make([]byte, 16<<20), time.Minute); err != nil {
		t.Fatal(err)
	}
	up := time.Since(start)
	st := s.waitIdle(base, 4)
	measure(t, "64 MiB down in %v (%.0f MiB/s), 16 MiB up in %v; idle server: %+v", down.Round(time.Millisecond),
		64/down.Seconds(), up.Round(time.Millisecond), st)
	if n := s.clientState().SessionsStarted; n != 1 {
		t.Fatalf("%d sessions started", n)
	}
}

// Slow, jittery, capped and lossy lines together still carry every byte.
func TestImpairedLines(t *testing.T) {
	s := newStack(t, 3)
	s.lines[0].Set(faultproxy.Fault{Mode: faultproxy.Pass, Delay: 100 * time.Millisecond, Jitter: 50 * time.Millisecond}, faultproxy.ScopeAll)
	s.lines[1].Set(faultproxy.Fault{Mode: faultproxy.Pass, BPS: 1 << 20}, faultproxy.ScopeAll)
	s.lines[2].Set(faultproxy.Fault{Mode: faultproxy.Loss, LossRate: 0.05, LossPause: 300 * time.Millisecond}, faultproxy.ScopeAll)
	start := time.Now()
	if err := s.big(16<<20, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.echo(make([]byte, 4<<20), time.Minute); err != nil {
		t.Fatal(err)
	}
	var worst time.Duration
	for range 50 {
		t0 := time.Now()
		if string(s.mustGet("/small")) != "ok" {
			t.Fatal("wrong answer")
		}
		worst = max(worst, time.Since(t0))
	}
	st := s.clientState()
	measure(t, "16 MiB down + 4 MiB up in %v; slowest of 50 small requests %v; reconnects per line %d/%d/%d; sessions %d",
		time.Since(start).Round(time.Millisecond), worst.Round(time.Millisecond),
		st.Lines[0].Reconnects, st.Lines[1].Reconnects, st.Lines[2].Reconnects, st.SessionsStarted)
	if st.SessionsStarted != 1 {
		t.Fatalf("%d sessions started", st.SessionsStarted)
	}
}

// streamGaps reads /stream for d and returns the longest pause between two reads.
func streamGaps(s *stack, d time.Duration) (time.Duration, int64, error) {
	hc := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: d + 10*time.Second}
	resp, err := hc.Get(fmt.Sprintf("http://%s/stream?ms=%d", s.clientAddr, d.Milliseconds()))
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	r := bufio.NewReaderSize(resp.Body, 64<<10)
	buf := make([]byte, 64<<10)
	var worst time.Duration
	var total int64
	last := time.Now()
	for {
		n, err := r.Read(buf)
		if n > 0 {
			worst = max(worst, time.Since(last))
			last = time.Now()
			total += int64(n)
		}
		if err == io.EOF {
			return worst, total, nil
		}
		if err != nil {
			return worst, total, err
		}
	}
}

// One line stops reading in both directions while another stays healthy (the version-2 stall in
// which a blocked write froze the whole session). The stream must keep flowing, and the stuck link
// must be dropped. With ScopeAll the reconnects of that line are stalled too.
func TestStalledReaderOnOneLine(t *testing.T) {
	for name, scope := range map[string]faultproxy.Scope{"existing": faultproxy.ScopeExisting, "all": faultproxy.ScopeAll} {
		t.Run(name, func(t *testing.T) {
			s := newStack(t, 2)
			go func() {
				time.Sleep(2 * time.Second)
				s.lines[0].Set(faultproxy.Fault{Mode: faultproxy.Stall}, scope)
			}()
			worst, total, err := streamGaps(s, 8*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			st := s.clientState()
			measure(t, "longest pause %v over %d MiB; line0: %s, %d reconnects, last reason %q", worst.Round(time.Millisecond),
				total>>20, st.Lines[0].State, st.Lines[0].Reconnects, st.Lines[0].LastReason)
			if worst > 1500*time.Millisecond {
				t.Fatalf("the stream paused for %v", worst)
			}
			if st.Lines[0].LastReason == "" || st.SessionsStarted != 1 {
				t.Fatalf("the stalled link was not dropped, or the session restarted: %+v", st)
			}
		})
	}
}

// Links are cut over and over while requests run (the version-2 stall in which a reattached link
// carried a newer frame ahead of its replay). Every request must succeed, quickly, in one session.
func TestReplayUnderManyReconnects(t *testing.T) {
	s := newStack(t, 2)
	stop := make(chan struct{})
	var drops atomic.Int64
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(150 * time.Millisecond):
			}
			// Only cut a line while the other is up, so that every cut is one link's.
			st := s.clientState()
			if len(st.Lines) == 2 && st.Lines[0].State == "up" && st.Lines[1].State == "up" {
				drops.Add(int64(s.lines[i%2].Drop()))
			}
		}
	}()
	var worst time.Duration
	var n int
	for end := time.Now().Add(20 * time.Second); time.Now().Before(end); n++ {
		t0 := time.Now()
		if err := s.echo(make([]byte, 256<<10), 10*time.Second); err != nil {
			t.Fatal(err)
		}
		if err := s.big(1<<20, 10*time.Second); err != nil {
			t.Fatal(err)
		}
		worst = max(worst, time.Since(t0))
	}
	close(stop)
	st := s.clientState()
	measure(t, "%d request pairs during %d link cuts; slowest pair %v; sessions %d", n, drops.Load(), worst.Round(time.Millisecond), st.SessionsStarted)
	if st.SessionsStarted != 1 || worst > 2*time.Second {
		t.Fatalf("not seamless: %d sessions, slowest pair %v", st.SessionsStarted, worst)
	}
}

// firstSuccess polls /small until it succeeds and returns how long that took.
func firstSuccess(s *stack, from time.Time, limit time.Duration) (time.Duration, error) {
	for time.Since(from) < limit {
		if b, err := s.request("GET", "/small", nil, 3*time.Second); err == nil && string(b) == "ok" {
			return time.Since(from), nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return 0, fmt.Errorf("no request succeeded within %v", limit)
}

// The server process is killed or stopped under a live session and comes back on the same port
// (the version-2 stall in which a reconnect was silently taken for a new session at offset 0). A
// request in flight must fail promptly rather than hang, and new requests must succeed within about
// two seconds of the server being back.
func TestServerRestart(t *testing.T) {
	for name, sig := range map[string]syscall.Signal{"kill": syscall.SIGKILL, "graceful": syscall.SIGTERM} {
		t.Run(name, func(t *testing.T) {
			s := newStack(t, 2)
			s.mustGet("/small")
			inflight := make(chan time.Duration, 1)
			go func() {
				t0 := time.Now()
				_, err := s.request("GET", "/hold?ms=5000", nil, 20*time.Second)
				if err == nil {
					t.Error("a request held across the restart succeeded; its stream should have failed")
				}
				inflight <- time.Since(t0)
			}()
			time.Sleep(300 * time.Millisecond)
			s.server.stop(sig)
			stopped := time.Now()
			s.startServer()
			back := time.Since(stopped)
			recovery, err := firstSuccess(s, stopped, 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			failed := <-inflight
			st := s.clientState()
			measure(t, "server back after %v; first request after %v; the in-flight request (started 300 ms before the stop) failed after %v; sessions %d",
				back.Round(time.Millisecond), recovery.Round(time.Millisecond), failed.Round(time.Millisecond), st.SessionsStarted)
			if recovery-back > 2500*time.Millisecond {
				t.Fatalf("recovery took %v after the server was back", recovery-back)
			}
			if failed > 2500*time.Millisecond {
				t.Fatalf("the in-flight request hung for %v", failed)
			}
		})
	}
}

// Every line goes silent at once while new connections work, as when a phone moves from Wi-Fi to
// cellular in the middle of a download. The client must notice within about a second and a half
// that no line answers, dial every line again into the same session, and carry on: the download
// keeps going after a pause, no session ends, and the server keeps one session.
func TestNetworkSwitch(t *testing.T) {
	s := newStack(t, 3)
	s.mustGet("/small")
	before := s.clientState().SessionsStarted
	var cut time.Time
	go func() {
		time.Sleep(2 * time.Second)
		cut = time.Now()
		for _, l := range s.lines {
			l.Set(faultproxy.Fault{Mode: faultproxy.Blackhole}, faultproxy.ScopeExisting)
		}
	}()
	worst, total, err := streamGaps(s, 8*time.Second)
	if err != nil {
		t.Fatalf("the stream across the switch failed: %v", err)
	}
	recovery, err := firstSuccess(s, time.Now(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	st := s.clientState()
	srv := s.mustServer()
	measure(t, "longest pause of the stream across the switch %v over %d MiB; a new request after %v; sessions %d -> %d, server sessions %d; switched %v ago",
		worst.Round(time.Millisecond), total>>20, recovery.Round(time.Millisecond), before, st.SessionsStarted, srv.Sessions, time.Since(cut).Round(time.Millisecond))
	if st.SessionsStarted != before || srv.Sessions != 1 {
		t.Fatalf("the switch cost the session: sessions %d -> %d, server sessions %d, last end %q", before, st.SessionsStarted, srv.Sessions, st.LastSessionEnd)
	}
	if worst > 4*time.Second {
		t.Fatalf("the stream paused for %v", worst)
	}
}

// One line pauses now and then for up to 1.5 s, as a mobile line does when TCP retransmits, while
// another stays healthy. The jittery line must keep its link (it is only soft down while it is late),
// and requests must not wait for it.
func TestJitteryLine(t *testing.T) {
	s := newStack(t, 2)
	s.lines[0].Set(faultproxy.Fault{Mode: faultproxy.Loss, LossRate: 0.2, LossPause: 1500 * time.Millisecond}, faultproxy.ScopeAll)
	var worst time.Duration
	for end := time.Now().Add(15 * time.Second); time.Now().Before(end); time.Sleep(100 * time.Millisecond) {
		t0 := time.Now()
		if string(s.mustGet("/small")) != "ok" {
			t.Fatal("wrong answer")
		}
		worst = max(worst, time.Since(t0))
	}
	st := s.clientState()
	measure(t, "slowest of the small requests %v; jittery line: %d reconnects, last reason %q, rtt %d ms; sessions %d",
		worst.Round(time.Millisecond), st.Lines[0].Reconnects, st.Lines[0].LastReason, st.Lines[0].RTTMs, st.SessionsStarted)
	if st.Lines[0].Reconnects != 0 || st.SessionsStarted != 1 {
		t.Fatalf("the jittery line lost its link: %+v, %d sessions", st.Lines[0], st.SessionsStarted)
	}
	if worst > time.Second {
		t.Fatalf("a request took %v", worst)
	}
}

// Thousands of requests in one session leave no goroutine, stream or target connection behind.
func TestLongSessionReturnsToBaseline(t *testing.T) {
	s := newStack(t, 3)
	s.mustGet("/small")
	base := s.mustServer().Goroutines
	start := time.Now()
	var wg sync.WaitGroup
	var failures atomic.Int64
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				if b, err := s.request("GET", "/small", nil, 10*time.Second); err != nil || string(b) != "ok" {
					failures.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	took := time.Since(start)
	peak := s.mustServer()
	st := s.waitIdle(base, 4)
	measure(t, "4000 requests in %v (%d failed); server goroutines %d before, %d right after; idle %+v; target connections open %d",
		took.Round(time.Millisecond), failures.Load(), base, peak.Goroutines, st, s.target.open.Load())
	if failures.Load() != 0 || s.clientState().SessionsStarted != 1 {
		t.Fatalf("%d failures, %d sessions", failures.Load(), s.clientState().SessionsStarted)
	}
}

// One slow line alone, as a CDN line from far away: 300 ms one way plus up to 150 ms of jitter (a
// round trip of 600 to 900 ms) and 1 MB/s, carrying a first load of 2.6 MB twice and small requests.
// Every byte must arrive in one session: the acknowledgement-progress deadline has to cover an ACK
// queued behind a download on such a line, and the stream credit decides how long the load takes.
func TestSlowLine(t *testing.T) {
	s := newStack(t, 1)
	s.lines[0].Set(faultproxy.Fault{Mode: faultproxy.Pass, Delay: 300 * time.Millisecond, Jitter: 150 * time.Millisecond, BPS: 1 << 20}, faultproxy.ScopeAll)
	var loads []string
	for range 2 {
		t0 := time.Now()
		if err := s.big(2600<<10, time.Minute); err != nil {
			t.Fatal(err)
		}
		loads = append(loads, time.Since(t0).Round(time.Millisecond).String())
	}
	var worst time.Duration
	for range 10 {
		t0 := time.Now()
		if string(s.mustGet("/small")) != "ok" {
			t.Fatal("wrong answer")
		}
		worst = max(worst, time.Since(t0))
	}
	st := s.clientState()
	measure(t, "2.6 MB in %s; slowest of 10 small requests %v; reconnects %d, last reason %q; sessions %d",
		strings.Join(loads, " and "), worst.Round(time.Millisecond), st.Lines[0].Reconnects, st.Lines[0].LastReason, st.SessionsStarted)
	if st.SessionsStarted != 1 {
		t.Fatalf("%d sessions started", st.SessionsStarted)
	}
}

// The entry stops answering while every line works, and the client restarts meanwhile, as when the
// origin that serves the line list is down for minutes and the machine reboots. A client that kept
// the entry's list in its cache file must start a session from it and serve requests within a few
// seconds, whether the entry refuses connections or swallows them until the fetch times out.
func TestEntryUnreachable(t *testing.T) {
	for name, mode := range map[string]faultproxy.Mode{"refused": faultproxy.Refuse, "silent": faultproxy.Blackhole} {
		t.Run(name, func(t *testing.T) {
			s := newServerOnly(t, 2)
			entry, err := faultproxy.Listen(s.serverAddr)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(entry.Close)
			cache := filepath.Join(t.TempDir(), "lines.json")
			s.startClient("http://"+entry.Addr(), "-lines-cache", cache)
			s.waitLinesUp(2, 10*time.Second)
			s.mustGet("/small")
			s.waitFor("the line list cache", 5*time.Second, func() bool { _, err := os.Stat(cache); return err == nil })
			s.client.stop(syscall.SIGTERM)
			entry.Set(faultproxy.Fault{Mode: mode}, faultproxy.ScopeAll)
			restarted := time.Now()
			s.startClient("http://"+entry.Addr(), "-lines-cache", cache)
			recovery, err := firstSuccess(s, restarted, 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			s.waitLinesUp(2, 5*time.Second)
			measure(t, "entry %s: first request %v after the client restarted", name, recovery.Round(time.Millisecond))
			if recovery > 3*time.Second {
				t.Fatalf("the first request took %v with the entry %s", recovery, name)
			}
		})
	}
}

// The conformance harness: it runs the real multipath-server and multipath-client binaries (built by
// run.sh into $MP_BIN) with their production timeouts, puts a fault proxy (package faultproxy) on
// every line, and gives each scenario a target HTTP server with known answers. Everything listens on
// fresh loopback ports and is stopped when the scenario ends; a failing scenario prints the tail of
// both processes' logs.
//
// The target answers:
//
//	GET  /small            "ok"
//	GET  /big?n=N          N bytes of a fixed pattern (checkPattern verifies them)
//	GET  /stream?ms=T      64 KiB every 10 ms for T milliseconds
//	GET  /hold?ms=T        "ok" after T milliseconds
//	POST /echo             the hex SHA-256 of the request body
//
// Scenarios report numbers with measure(), as lines starting with "MEASURE", which run.sh collects.
package conformance

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/micro-teams/multipath/conformance/faultproxy"
	"github.com/micro-teams/multipath/go/client"
	"github.com/micro-teams/multipath/go/server"
)

func bin(t *testing.T, name string) string {
	dir := os.Getenv("MP_BIN")
	if dir == "" {
		t.Fatal("MP_BIN is not set: run the suite with conformance/run.sh, which builds the binaries")
	}
	return filepath.Join(dir, name)
}

func measure(t *testing.T, format string, args ...any) {
	t.Logf("MEASURE %s: %s", t.Name(), fmt.Sprintf(format, args...))
}

func freeAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// pattern is the byte at offset i of a /big answer.
func pattern(i int) byte { return byte(i*31 + i>>13) }

func checkPattern(b []byte) bool {
	for i, v := range b {
		if v != pattern(i) {
			return false
		}
	}
	return true
}

type target struct {
	addr string
	open atomic.Int64 // open connections
}

func startTarget(t *testing.T) *target {
	tg := &target{}
	h := http.NewServeMux()
	h.HandleFunc("/small", func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") })
	h.HandleFunc("/big", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.URL.Query().Get("n"))
		w.Header().Set("Content-Length", strconv.Itoa(n))
		buf := make([]byte, 64<<10)
		for off := 0; off < n; off += len(buf) {
			m := min(len(buf), n-off)
			for i := range m {
				buf[i] = pattern(off + i)
			}
			if _, err := w.Write(buf[:m]); err != nil {
				return
			}
		}
	})
	h.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		ms, _ := strconv.Atoi(r.URL.Query().Get("ms"))
		chunk := bytes.Repeat([]byte{'s'}, 64<<10)
		for end := time.Now().Add(time.Duration(ms) * time.Millisecond); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
			if _, err := w.Write(chunk); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
	})
	h.HandleFunc("/hold", func(w http.ResponseWriter, r *http.Request) {
		ms, _ := strconv.Atoi(r.URL.Query().Get("ms"))
		time.Sleep(time.Duration(ms) * time.Millisecond)
		_, _ = io.WriteString(w, "ok")
	})
	h.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		sum := sha256.New()
		if _, err := io.Copy(sum, r.Body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_, _ = io.WriteString(w, hex.EncodeToString(sum.Sum(nil)))
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tg.addr = ln.Addr().String()
	hs := &http.Server{Handler: h, ConnState: func(_ net.Conn, s http.ConnState) {
		switch s {
		case http.StateNew:
			tg.open.Add(1)
		case http.StateClosed, http.StateHijacked:
			tg.open.Add(-1)
		}
	}}
	go func() { _ = hs.Serve(ln) }() // ends with Close below
	t.Cleanup(func() { _ = hs.Close() })
	return tg
}

type proc struct {
	cmd  *exec.Cmd
	log  string
	done chan struct{}
}

func startProc(t *testing.T, logName string, path string, args ...string) *proc {
	logPath := filepath.Join(t.TempDir(), logName)
	f, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(path, args...)
	cmd.Stdout, cmd.Stderr = f, f
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &proc{cmd: cmd, log: logPath, done: make(chan struct{})}
	go func() { _ = cmd.Wait(); _ = f.Close(); close(p.done) }() // the exit status is not the point; the log is
	t.Cleanup(func() {
		p.stop(syscall.SIGKILL)
		if t.Failed() {
			data, _ := os.ReadFile(logPath)
			lines := strings.Split(strings.TrimSpace(string(data)), "\n")
			t.Logf("--- last lines of %s ---\n%s", logName, strings.Join(lines[max(0, len(lines)-40):], "\n"))
		}
	})
	return p
}

func (p *proc) stop(sig syscall.Signal) {
	select {
	case <-p.done:
		return
	default:
	}
	_ = p.cmd.Process.Signal(sig) // the process may have exited in between
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill() // it ignored the signal; the scenario fails on its own terms
		<-p.done
	}
}

// rssKiB returns the resident memory of a process.
func (p *proc) rssKiB() int {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", p.cmd.Process.Pid))
	if err != nil {
		return -1
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			n, _ := strconv.Atoi(strings.Fields(line)[1])
			return n
		}
	}
	return -1
}

type stack struct {
	t            *testing.T
	target       *target
	lines        []*faultproxy.Line
	linesFile    string
	serverAddr   string
	serverStatus string
	server       *proc
	clientAddr   string
	clientStatus string
	client       *proc
}

// newServerOnly runs a target and a server whose line list names n fault proxies in front of it.
func newServerOnly(t *testing.T, n int) *stack {
	s := &stack{t: t, target: startTarget(t), serverAddr: freeAddr(t), serverStatus: freeAddr(t)}
	var entries []string
	for i := range n {
		l, err := faultproxy.Listen(s.serverAddr)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(l.Close)
		s.lines = append(s.lines, l)
		entries = append(entries, fmt.Sprintf(`{"id":"line%d","url":"http://%s"}`, i, l.Addr()))
	}
	s.linesFile = filepath.Join(t.TempDir(), "lines.json")
	if err := os.WriteFile(s.linesFile, []byte(`{"lines":[`+strings.Join(entries, ",")+`]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	s.startServer()
	return s
}

// newStack adds a client to newServerOnly and waits until all n lines are up.
func newStack(t *testing.T, n int) *stack {
	s := newServerOnly(t, n)
	s.clientAddr, s.clientStatus = freeAddr(t), freeAddr(t)
	s.client = startProc(t, "client.log", bin(t, "multipath-client"), "-listen", s.clientAddr,
		"-entry", "http://"+s.serverAddr, "-status", s.clientStatus)
	s.waitLinesUp(n, 10*time.Second)
	return s
}

func (s *stack) startServer() {
	s.server = startProc(s.t, "server.log", bin(s.t, "multipath-server"), "-listen", s.serverAddr, "-target", s.target.addr,
		"-lines-file", s.linesFile, "-status", s.serverStatus, "-status-every", "0")
	s.waitFor("the server to answer", 10*time.Second, func() bool { _, err := s.serverState(); return err == nil })
}

func getJSON(addr string, v any) error {
	hc := http.Client{Timeout: time.Second}
	resp, err := hc.Get("http://" + addr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(v)
}

func (s *stack) serverState() (server.Status, error) {
	var st server.Status
	return st, getJSON(s.serverStatus+"/", &st)
}

func (s *stack) mustServer() server.Status {
	st, err := s.serverState()
	if err != nil {
		s.t.Fatalf("server status: %v", err)
	}
	return st
}

func (s *stack) clientState() client.Status {
	var st client.Status
	if err := getJSON(s.clientStatus+"/status", &st); err != nil {
		s.t.Fatalf("client status: %v", err)
	}
	return st
}

// linesUp counts the client's lines that are up; 0 while its status port is not answering yet.
func (s *stack) linesUp() int {
	var st client.Status
	if getJSON(s.clientStatus+"/status", &st) != nil {
		return 0
	}
	up := 0
	for _, l := range st.Lines {
		if l.State == "up" {
			up++
		}
	}
	return up
}

func (s *stack) waitFor(what string, timeout time.Duration, cond func() bool) time.Duration {
	start := time.Now()
	for time.Since(start) < timeout {
		if cond() {
			return time.Since(start)
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.t.Fatalf("timed out after %v waiting for %s", timeout, what)
	return 0
}

func (s *stack) waitLinesUp(n int, timeout time.Duration) {
	s.waitFor(fmt.Sprintf("%d lines up", n), timeout, func() bool { return s.linesUp() == n })
}

// waitIdle waits until the server holds no streams or target connections and its goroutines are
// back within slack of base, and returns the final status.
func (s *stack) waitIdle(base, slack int) server.Status {
	var st server.Status
	s.waitFor("the server to return to its baseline", 15*time.Second, func() bool {
		st = s.mustServer()
		return st.Streams == 0 && st.Targets == 0 && st.Goroutines <= base+slack && s.target.open.Load() == 0
	})
	return st
}

// request performs one HTTP request through the client's local port, on a connection of its own.
func (s *stack) request(method, path string, body []byte, timeout time.Duration) ([]byte, error) {
	hc := &http.Client{Timeout: timeout, Transport: &http.Transport{DisableKeepAlives: true}}
	req, err := http.NewRequestWithContext(context.Background(), method, "http://"+s.clientAddr+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err == nil && resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("%s: %s", resp.Status, b)
	}
	return b, err
}

func (s *stack) mustGet(path string) []byte {
	b, err := s.request("GET", path, nil, 30*time.Second)
	if err != nil {
		s.t.Fatalf("GET %s: %v", path, err)
	}
	return b
}

// echo uploads body and checks the target saw exactly it.
func (s *stack) echo(body []byte, timeout time.Duration) error {
	got, err := s.request("POST", "/echo", body, timeout)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(body)
	if string(got) != hex.EncodeToString(sum[:]) {
		return fmt.Errorf("upload of %d bytes arrived altered", len(body))
	}
	return nil
}

// big downloads n bytes and checks them.
func (s *stack) big(n int, timeout time.Duration) error {
	b, err := s.request("GET", fmt.Sprintf("/big?n=%d", n), nil, timeout)
	if err != nil {
		return err
	}
	if len(b) != n || !checkPattern(b) {
		return fmt.Errorf("download of %d bytes arrived as %d bytes, intact=%v", n, len(b), checkPattern(b))
	}
	return nil
}

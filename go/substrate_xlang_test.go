package multipath

import (
	"bufio"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestXLangSubstrateGoClientJavaOrigin is the cross-language proof for the whole substrate: the Go
// Client dials a real JVM Origin over several links and drives it through the header-routed paths —
// a tunnel spliced to an echo and a normal stream spliced to an HTTP greeter — so the redundant
// frames, the mux, and the L5 header all have to agree on the wire between the two implementations.
//
// Skipped unless MP_JVM_CP is set to the JVM classpath (see testbed/redundant-xlang/run.sh).
func TestXLangSubstrateGoClientJavaOrigin(t *testing.T) {
	cp := os.Getenv("MP_JVM_CP")
	if cp == "" {
		t.Skip("set MP_JVM_CP to run the cross-language substrate e2e (see testbed/redundant-xlang/run.sh)")
	}
	const n = 3

	cmd := exec.Command("java", "-cp", cp, "app.microteams.multipath.OriginMain", strconv.Itoa(n))
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start java: %v", err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })

	port := ""
	lines := bufio.NewScanner(stdout)
	deadline := time.Now().Add(30 * time.Second)
	for lines.Scan() && time.Now().Before(deadline) {
		if rest, ok := strings.CutPrefix(lines.Text(), "LISTENING "); ok {
			port = strings.TrimSpace(rest)
			break
		}
	}
	if port == "" {
		t.Fatal("java origin did not report LISTENING")
	}

	origin := "127.0.0.1:" + port
	c := dialClientOverN(t, origin, n)

	t.Run("tunnel", func(t *testing.T) {
		st, err := c.OpenTunnel("echo:0", []byte("ticket"))
		if err != nil {
			t.Fatalf("OpenTunnel: %v", err)
		}
		msg := []byte("bytes across the language boundary, deduped by offset")
		if _, err := st.Write(msg); err != nil {
			t.Fatalf("write: %v", err)
		}
		got := make([]byte, len(msg))
		if _, err := io.ReadFull(st, got); err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(got) != string(msg) {
			t.Fatalf("tunnel echo mismatch: %q", got)
		}
	})

	t.Run("normal-http", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, "http://origin/xlang", nil)
		resp, err := c.RoundTrip(req)
		if err != nil {
			t.Fatalf("RoundTrip: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 || string(body) != "hello /xlang" {
			t.Fatalf("unexpected response %d %q", resp.StatusCode, body)
		}
	})
}

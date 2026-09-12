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

// TestXLangSubstrateGoClientJavaOrigin is the cross-language proof for the whole substrate, over an
// adverse network: the Go Client dials a real JVM Origin over several links, each fronted by its own
// fault middlebox (black-hole / one-directional / disconnect), and drives it through both routed
// paths — a tunnel spliced to an echo and a normal stream spliced to an HTTP greeter. So the
// redundant frames, the mux, and the L5 header all have to agree on the wire between the two
// implementations AND survive links being cut underneath them.
//
// Skipped unless MP_JVM_CP is set to the JVM classpath (see testbed/run.sh).
func TestXLangSubstrateGoClientJavaOrigin(t *testing.T) {
	cp := os.Getenv("MP_JVM_CP")
	if cp == "" {
		t.Skip("set MP_JVM_CP to run the cross-language substrate e2e (see testbed/run.sh)")
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

	originPort := ""
	scan := bufio.NewScanner(stdout)
	deadline := time.Now().Add(30 * time.Second)
	for scan.Scan() && time.Now().Before(deadline) {
		if rest, ok := strings.CutPrefix(scan.Text(), "LISTENING "); ok {
			originPort = strings.TrimSpace(rest)
			break
		}
	}
	if originPort == "" {
		t.Fatal("java origin did not report LISTENING")
	}

	// One fault middlebox per link, each fronting the Java origin; the client reaches the origin
	// only through them, so every byte crosses a link that is being cut on a random schedule.
	// Mixed encapsulation into one redundant stream: even links plaintext, odd links WebSocket, so
	// one run exercises both of the origin's decapsulation paths and proves a redundant stream can
	// aggregate links of different transports.
	lines := make([]Line, n)
	for i := 0; i < n; i++ {
		box := newMiddlebox(t, "127.0.0.1:"+originPort, int64(6000+i))
		t.Cleanup(box.close)
		transport := TransportTCP
		if i%2 == 1 {
			transport = TransportWS
		}
		lines[i] = Line{ID: "l" + strconv.Itoa(i), URL: "http://" + box.addr(), Transport: string(transport)}
	}

	c, err := Dial(t.Context(), ClientOptions{
		Lines: lines,
		Redundant: RedundantOptions{
			Window: 2 << 20, PingInterval: 20 * time.Millisecond, DeadAfter: 100 * time.Millisecond,
			AckInterval: 8 * time.Millisecond, ReconnectDelay: 5 * time.Millisecond, MaxDelay: 40 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	t.Run("tunnel", func(t *testing.T) {
		st, err := c.Open("echo", []byte("ticket"))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		msg := randBytes(48 << 10)
		go func() { _, _ = st.Write(msg); _ = st.Close() }()
		got, err := io.ReadAll(st)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(got) != string(msg) {
			t.Fatalf("tunnel echo mismatch: %d of %d bytes", len(got), len(msg))
		}
	})

	t.Run("normal-http", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, "http://origin/xlang", nil)
		resp, err := c.RoundTrip("greeter", nil, req)
		if err != nil {
			t.Fatalf("RoundTrip: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 || string(body) != "hello /xlang" {
			t.Fatalf("unexpected response %d %q", resp.StatusCode, body)
		}
	})

	t.Run("websocket", func(t *testing.T) {
		ws, err := c.OpenWebSocket("ws-echo", []byte("ticket"), "/echo", nil)
		if err != nil {
			t.Fatalf("OpenWebSocket: %v", err)
		}
		defer ws.Close()
		for i := 0; i < 3; i++ {
			msg := randBytes(1024 * (i + 1))
			if err := ws.WriteMessage(WSBinary, msg); err != nil {
				t.Fatalf("WriteMessage: %v", err)
			}
			typ, got, err := ws.ReadMessage()
			if err != nil {
				t.Fatalf("ReadMessage: %v", err)
			}
			if typ != WSBinary {
				t.Fatalf("message %d: type = %v, want WSBinary", i, typ)
			}
			if string(got) != string(msg) {
				t.Fatalf("message %d: echo mismatch: %d of %d bytes", i, len(got), len(msg))
			}
		}
	})
}

package multipath

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestXLangJavaServerGoClient is the true cross-language end-to-end: a real JVM server process (the
// Kotlin RedundantServer, run via EchoServerMain) and a Go client, with a per-link fault middlebox
// between them doing black-hole / one-directional / disconnect cuts. The Go client sends a payload,
// the Java server echoes every byte back over the same redundant stream, and the round-trip must be
// byte-exact — proving the two implementations interoperate on the wire under adverse networks.
//
// Skipped unless MP_JVM_CP is set to the JVM classpath (see testbed/redundant-xlang/run.sh). This
// keeps the default `go test` free of a Java dependency.
func TestXLangJavaServerGoClient(t *testing.T) {
	cp := os.Getenv("MP_JVM_CP")
	if cp == "" {
		t.Skip("set MP_JVM_CP to the JVM classpath to run the cross-language e2e (see testbed/redundant-xlang/run.sh)")
	}
	const n = 3

	// Start the Java echo server on an ephemeral port; it prints "LISTENING <port>" when ready.
	cmd := exec.Command("java", "-cp", cp, "app.microteams.multipath.redundant.EchoServerMain", "0", strconv.Itoa(n))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start java: %v", err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()

	br := bufio.NewReader(stdout)
	var srvPort int
	{
		line := make(chan string, 1)
		go func() {
			s, _ := br.ReadString('\n')
			line <- s
		}()
		select {
		case s := <-line:
			s = strings.TrimSpace(s)
			if !strings.HasPrefix(s, "LISTENING ") {
				t.Fatalf("unexpected server line: %q", s)
			}
			srvPort, err = strconv.Atoi(strings.TrimPrefix(s, "LISTENING "))
			if err != nil {
				t.Fatalf("parse port: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("java server did not report LISTENING")
		}
	}

	// One fault middlebox per link, each fronting the Java server.
	boxes := make([]*middlebox, n)
	for i := 0; i < n; i++ {
		boxes[i] = newMiddlebox(t, "127.0.0.1:"+strconv.Itoa(srvPort), int64(2000+i))
		defer boxes[i].close()
	}

	cli, err := DialRedundant(context.Background(), RedundantOptions{
		N: n, Window: 2 << 20,
		PingInterval: 20 * time.Millisecond, DeadAfter: 100 * time.Millisecond,
		AckInterval: 8 * time.Millisecond, ReconnectDelay: 5 * time.Millisecond, MaxDelay: 40 * time.Millisecond,
		Dial: func(ctx context.Context, i int) (io.ReadWriteCloser, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", boxes[i].addr())
		},
	})
	if err != nil {
		t.Fatalf("client dial: %v", err)
	}
	defer cli.Close()

	msg := randBytes(256 << 10)
	go func() { _, _ = cli.Write(msg) }()

	got := make([]byte, 0, len(msg))
	buf := make([]byte, 64*1024)
	deadline := time.Now().Add(60 * time.Second)
	for len(got) < len(msg) {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: echoed %d/%d bytes under fault injection", len(got), len(msg))
		}
		m, rerr := cli.Read(buf)
		if m > 0 {
			got = append(got, buf[:m]...)
		}
		if rerr != nil && len(got) < len(msg) {
			t.Fatalf("read err at %d/%d: %v", len(got), len(msg), rerr)
		}
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("cross-language echo mismatch")
	}
}

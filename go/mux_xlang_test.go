package multipath

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestXLangMuxJavaServerGoClient is the cross-language proof for the mux layer: a real JVM server
// process (RedundantServer + MuxSession in "mux" mode) and a Go client that opens many concurrent
// mux streams over ONE redundant stream, through a per-link fault middlebox. Every stream's echo
// must be byte-exact. Skipped unless MP_JVM_CP is set (see testbed/redundant-xlang/run.sh).
func TestXLangMuxJavaServerGoClient(t *testing.T) {
	cp := os.Getenv("MP_JVM_CP")
	if cp == "" {
		t.Skip("set MP_JVM_CP to run the cross-language mux e2e (see testbed/redundant-xlang/run.sh)")
	}
	const nLinks = 3
	cmd := exec.Command("java", "-cp", cp, "app.microteams.multipath.redundant.EchoServerMain", "0", strconv.Itoa(nLinks), "mux")
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
	line := make(chan string, 1)
	go func() { s, _ := br.ReadString('\n'); line <- s }()
	var srvPort int
	select {
	case s := <-line:
		s = strings.TrimSpace(s)
		if !strings.HasPrefix(s, "LISTENING ") {
			t.Fatalf("unexpected server line: %q", s)
		}
		if srvPort, err = strconv.Atoi(strings.TrimPrefix(s, "LISTENING ")); err != nil {
			t.Fatalf("parse port: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("java server did not report LISTENING")
	}

	boxes := make([]*middlebox, nLinks)
	for i := 0; i < nLinks; i++ {
		boxes[i] = newMiddlebox(t, "127.0.0.1:"+strconv.Itoa(srvPort), int64(6000+i))
		defer boxes[i].close()
	}

	cliRS, err := DialRedundant(context.Background(), RedundantOptions{
		N: nLinks, Window: 2 << 20,
		PingInterval: 20 * time.Millisecond, DeadAfter: 100 * time.Millisecond, AckInterval: 8 * time.Millisecond,
		ReconnectDelay: 5 * time.Millisecond, MaxDelay: 40 * time.Millisecond,
		Dial: func(ctx context.Context, i int) (io.ReadWriteCloser, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", boxes[i].addr())
		},
	})
	if err != nil {
		t.Fatalf("client dial: %v", err)
	}
	defer cliRS.Close()

	sess := NewClientSession(cliRS)
	defer sess.Close()

	const nStreams = 16
	var wg sync.WaitGroup
	errs := make(chan error, nStreams)
	for i := 0; i < nStreams; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st, err := sess.OpenStream()
			if err != nil {
				errs <- err
				return
			}
			payload := append([]byte(fmt.Sprintf("s%02d:", i)), randBytes(48*1024)...)
			go func() { st.Write(payload); st.Close() }()
			got, _ := io.ReadAll(st)
			if !bytes.Equal(got, payload) {
				errs <- fmt.Errorf("stream %d mismatch: %d/%d bytes", i, len(got), len(payload))
			}
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(90 * time.Second):
		t.Fatalf("cross-language mux under faults timed out")
	}
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

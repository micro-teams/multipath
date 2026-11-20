// The browser leg: the JavaScript client in headless Chromium (js/conformance/browser.mjs) against the
// real server over two faulty lines, one of which is cut every 400 ms while the page runs requests.
// It needs Node, the built js/ package and Playwright's Chromium, so it runs only with MP_BROWSER=1
// (run.sh sets it when asked, and CI always); otherwise it is skipped, and says so.
package conformance

import (
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBrowserSmoke(t *testing.T) {
	if os.Getenv("MP_BROWSER") != "1" {
		t.Skip("MP_BROWSER is not 1: the Chromium leg is skipped")
	}
	s := newServerOnly(t, 2)
	stop := make(chan struct{})
	var cuts atomic.Int64
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(400 * time.Millisecond):
				cuts.Add(int64(s.lines[0].Drop()))
			}
		}
	}()
	out, err := exec.Command("node", "../js/conformance/browser.mjs", "-entry", "http://"+s.serverAddr, "-seconds", "6").CombinedOutput()
	close(stop)
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if strings.HasPrefix(line, "MEASURE ") {
			t.Logf("MEASURE chromium client, %s: %s; %d link cuts", t.Name(), strings.TrimPrefix(line, "MEASURE browser: "), cuts.Load())
		}
	}
	if err != nil {
		t.Fatalf("browser leg: %v\n%s", err, out)
	}
}

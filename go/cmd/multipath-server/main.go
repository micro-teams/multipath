// multipath-server: the MultiPath server as a process. It listens on one plain HTTP port for links
// and for the line list, and splices every stream to one TCP target; TLS is terminated in front of
// it (nginx, a CDN). See README.md for deployment and PROTOCOL.md for the wire.
//
// Configuration is by flags only (run with -h for the list):
//
//	multipath-server -listen :8080 -target 127.0.0.1:80 -lines-file /etc/multipath/lines.json \
//	    -proxy-protocol -status 127.0.0.1:9101
//
// It logs one line per event, and every -status-every a line with the live counters (sessions,
// links, streams, target connections, goroutines); -status serves the same counters as JSON on a
// separate, local address. SIGINT or SIGTERM sends GOODBYE to every session before exiting, so
// clients move to the next server process at once.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/micro-teams/multipath/go/protocol"
	"github.com/micro-teams/multipath/go/server"
)

func main() {
	d := protocol.DefaultLimits()
	listen := flag.String("listen", ":8080", "address for links and the line list (plain HTTP)")
	linkPath := flag.String("link-path", "/multipath/v3/link", "path of the WebSocket link endpoint")
	linesPath := flag.String("lines-path", "/multipath/v3/lines", "path of the line list")
	linesFile := flag.String("lines-file", "", "JSON line list served at -lines-path, read on every request (required)")
	target := flag.String("target", "", "host:port every stream is spliced to (required)")
	proxyProtocol := flag.Bool("proxy-protocol", false, "send a PROXY protocol v2 header with the client address to the target")
	status := flag.String("status", "", "local address serving the counters as JSON (off when empty)")
	statusEvery := flag.Duration("status-every", time.Minute, "interval of the counters log line (0 turns it off)")
	window := flag.Int("window", d.Window, "session window in bytes")
	streamWindow := flag.Int("stream-window", d.StreamWindow, "per-stream credit window in bytes")
	maxStreams := flag.Int("max-streams", d.MaxStreams, "open streams per session")
	maxLinks := flag.Int("max-links", d.MaxLinks, "links per session")
	flag.Parse()

	lim := d
	lim.Window, lim.StreamWindow, lim.MaxStreams, lim.MaxLinks = *window, *streamWindow, *maxStreams, *maxLinks
	srv, err := server.New(server.Config{LinkPath: *linkPath, LinesPath: *linesPath, LinesFile: *linesFile,
		Target: *target, ProxyProtocol: *proxyProtocol, Limits: lim, Logf: log.Printf})
	if err != nil {
		log.Fatalf("configuration: %v", err)
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	hs := &http.Server{Handler: srv.Handler(), ReadHeaderTimeout: protocol.HandshakeTimeout}
	if *status != "" {
		go func() { log.Fatalf("status: %v", http.ListenAndServe(*status, srv.StatusHandler())) }()
	}
	if *statusEvery > 0 {
		go func() {
			for range time.Tick(*statusEvery) {
				st := srv.Status()
				log.Printf("status: %d sessions, %d links, %d streams, %d target connections, %d goroutines",
					st.Sessions, st.Links, st.Streams, st.Targets, st.Goroutines)
			}
		}()
	}
	log.Printf("%s listening on %s, links at %s, lines at %s, target %s", protocol.Implementation, ln.Addr(), *linkPath, *linesPath, *target)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Printf("shutting down: saying goodbye to every session")
		srv.Shutdown()
		if err := hs.Close(); err != nil {
			log.Printf("close: %v", err)
		}
	}()
	if err := hs.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

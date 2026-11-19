// multipath-client: the MultiPath client as a process. Every TCP connection to its local port becomes
// one stream to the server's target, carried over all of the entry's lines at once. See README.md.
//
// Configuration is by flags only (run with -h for the list):
//
//	multipath-client -listen 127.0.0.1:7000 -entry https://example.app -status 127.0.0.1:7001
//
// The proxy for lines and the line list comes from HTTPS_PROXY, HTTP_PROXY and NO_PROXY unless
// -proxy names one. -status serves the client's state as JSON at /status on a local address.
// SIGINT or SIGTERM sends GOODBYE before exiting, so the server releases the session at once.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/micro-teams/multipath/go/client"
	"github.com/micro-teams/multipath/go/protocol"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:7000", "local address programs connect to")
	entry := flag.String("entry", "", "origin the line list is fetched from, such as https://example.app (required)")
	linkPath := flag.String("link-path", client.DefaultLinkPath, "path of the link endpoint on every line")
	linesPath := flag.String("lines-path", client.DefaultLinesPath, "path of the line list on the entry")
	proxy := flag.String("proxy", "", "http:// proxy for lines and the line list (default: from HTTPS_PROXY and HTTP_PROXY)")
	status := flag.String("status", "", "local address serving GET /status as JSON (off when empty)")
	refresh := flag.Duration("refresh", 10*time.Minute, "interval of line list refreshes")
	wait := flag.Duration("wait", 10*time.Second, "how long a local connection waits for a live session")
	flag.Parse()

	proxyFunc := http.ProxyFromEnvironment
	if *proxy != "" {
		u, err := url.Parse(*proxy)
		if err != nil {
			log.Fatalf("proxy: %v", err)
		}
		proxyFunc = http.ProxyURL(u)
	}
	c, err := client.New(client.Config{Listen: *listen, Entry: *entry, LinkPath: *linkPath, LinesPath: *linesPath,
		Proxy: proxyFunc, Refresh: *refresh, Wait: *wait, Logf: log.Printf})
	if err != nil {
		log.Fatalf("configuration: %v", err)
	}
	if *status != "" {
		h := http.NewServeMux()
		h.Handle("/status", c.StatusHandler())
		go func() { log.Fatalf("status: %v", http.ListenAndServe(*status, h)) }()
	}
	log.Printf("%s listening on %s, entry %s", protocol.Implementation, c.Addr(), *entry)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := c.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

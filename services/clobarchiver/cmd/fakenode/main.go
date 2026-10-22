// Command fakenode serves synthetic CLOB archive data on the node's clob
// JSON-RPC namespace, for running clobarchiver locally without a node.
//
//	go run ./cmd/fakenode -addr 127.0.0.1:18545 -oldest 1 -latest 400 -gaps 150,151,152,301
//	go run ./cmd/fakenode -latest 100 -grow-ms 500   # tip advances every 500ms
package main

import (
	"flag"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"clobarchiver/synth"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18545", "listen address")
	oldest := flag.Uint64("oldest", 1, "oldest height held")
	latest := flag.Uint64("latest", 400, "latest height")
	gaps := flag.String("gaps", "", "comma-separated heights returned as null")
	grow := flag.Int("grow-ms", 0, "advance latest by one every N ms (0 = fixed)")
	flag.Parse()

	var gs []uint64
	for _, s := range strings.Split(*gaps, ",") {
		if s = strings.TrimSpace(s); s == "" {
			continue
		}
		g, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			log.Fatalf("bad gap %q: %v", s, err)
		}
		gs = append(gs, g)
	}
	n := synth.NewNode(*oldest, *latest, gs...)
	if *grow > 0 {
		go func() {
			for range time.Tick(time.Duration(*grow) * time.Millisecond) {
				_, l := n.Range()
				n.SetLatest(l + 1)
			}
		}()
	}
	log.Printf("fakenode on http://%s oldest=%d latest=%d gaps=%v", *addr, *oldest, *latest, gs)
	log.Fatal(http.ListenAndServe(*addr, n))
}

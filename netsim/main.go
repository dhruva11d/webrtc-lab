package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"time"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:3479", "UDP address browsers connect to (as their TURN server)")
	target := flag.String("target", "192.168.29.19:3478", "real TURN server (coturn) to forward to")
	httpAddr := flag.String("http", "127.0.0.1:3480", "control API address (local only)")
	loss := flag.Float64("loss", 0, "packet loss percent per direction")
	delay := flag.Float64("delay", 0, "one-way delay in ms per direction")
	jitter := flag.Float64("jitter", 0, "+/- delay variation in ms")
	rate := flag.Float64("rate", 0, "bottleneck rate in kbps per direction (0 = unlimited)")
	queue := flag.Float64("queue", 300, "bottleneck buffer in ms (only with -rate)")
	flag.Parse()

	p, err := NewProxy(*listen, *target, Settings{LossPct: *loss, DelayMs: *delay, JitterMs: *jitter, RateKbps: *rate, MaxQueueMs: *queue})
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	go p.Run(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		s := p.Settings()
		fields := map[string]*float64{"loss": &s.LossPct, "delay": &s.DelayMs, "jitter": &s.JitterMs, "rate": &s.RateKbps, "queue": &s.MaxQueueMs}
		for key, dst := range fields {
			if v := r.URL.Query().Get(key); v != "" {
				f, err := strconv.ParseFloat(v, 64)
				if err != nil || f < 0 || (key == "loss" && f > 100) {
					http.Error(w, "bad value for "+key, http.StatusBadRequest)
					return
				}
				*dst = f
			}
		}
		p.SetSettings(s)
		log.Printf("settings changed: %+v", s)
		writeStatus(w, p)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { writeStatus(w, p) })
	go func() { log.Println("control API:", http.ListenAndServe(*httpAddr, mux)) }()

	fmt.Printf("netsim: browsers -> udp://%s -> %s\n", p.Addr(), *target)
	fmt.Printf("control: POST http://%s/set?loss=5&delay=100&jitter=20&rate=1000   (GET / shows status)\n", *httpAddr)
	fmt.Printf("initial: %+v\n", p.Settings())

	last := p.Counters()
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c := p.Counters()
			if c != last {
				fmt.Printf("up fwd=%d drop=%d | down fwd=%d drop=%d\n", c.UpForwarded, c.UpDropped, c.DownForwarded, c.DownDropped)
				last = c
			}
		}
	}
}

func writeStatus(w http.ResponseWriter, p *Proxy) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"settings": p.Settings(), "counters": p.Counters()})
}

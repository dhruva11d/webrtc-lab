package main

import (
	"flag"
	"log"
	"net/http"

	"sfu-lab/internal/echo"
	"sfu-lab/internal/relay"
	"sfu-lab/internal/room"
	"sfu-lab/internal/rtc"
	"sfu-lab/internal/simulcast"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8090", "HTTP address (pages and signaling)")
	web := flag.String("web", "web", "directory with the browser pages")
	flag.Parse()

	mux := http.NewServeMux()
	files := http.FileServer(http.Dir(*web))
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		files.ServeHTTP(w, r)
	}))

	api := rtc.NewAPI()
	mux.HandleFunc("/offer", echo.OfferHandler(api))
	mux.HandleFunc("/publish", relay.PublishHandler(api))
	mux.HandleFunc("/subscribe", relay.SubscribeHandler(api))
	mux.HandleFunc("/ws", room.WSHandler(api))
	mux.HandleFunc("/simulcast", simulcast.PublishHandler(api))
	mux.HandleFunc("/simulcast-sub", simulcast.SubscribeHandler())
	mux.HandleFunc("/simulcast-select", simulcast.SelectHandler)
	mux.HandleFunc("/simulcast-impair", simulcast.ImpairHandler)
	mux.HandleFunc("/simulcast-sub-stats", simulcast.StatsHandler())
	mux.HandleFunc("/simulcast-sub-series", simulcast.SeriesHandler)
	log.Printf("sfu-lab: open http://%s/echo.html", *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}

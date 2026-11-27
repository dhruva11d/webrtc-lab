package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
)

// used to define the MediaEngine instance and assign the interceptors that will be used in the RTP amd RTCP
// and use the webrtc.NewAPI to use the available media engine capabilities as well as the interceptors
func newAPI() *webrtc.API {
	m := &webrtc.MediaEngine{}
	if err := m.RegisterDefaultCodecs(); err != nil {
		log.Fatal(err)
	}
	ir := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(m, ir); err != nil {
		log.Fatal(err)
	}
	return webrtc.NewAPI(webrtc.WithMediaEngine(m), webrtc.WithInterceptorRegistry(ir))
}

func offerHandler(api *webrtc.API) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var offer webrtc.SessionDescription
		if err := json.NewDecoder(r.Body).Decode(&offer); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		pc, err := api.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
			log.Println("connection state:", s)
			if s == webrtc.PeerConnectionStateFailed || s == webrtc.PeerConnectionStateClosed {
				pc.Close()
			}
		})

		// The track we send back. It must exist before the answer is created.
		local, err := webrtc.NewTrackLocalStaticRTP(
			webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}, "video", "echo")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		sender, err := pc.AddTrack(local)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var inSSRC atomic.Uint32
		sendPLI := func() {
			if ssrc := inSSRC.Load(); ssrc != 0 {
				pc.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: ssrc}})
			}
		}

		// RTCP coming back from the browser about the video we echo. A PLI here means the
		// browser cannot decode: relay the request to whoever produces the video (the browser itself).
		go func() {
			for {
				pkts, _, err := sender.ReadRTCP()
				if err != nil {
					return
				}
				for _, p := range pkts {
					switch p.(type) {
					case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
						log.Println("browser sent PLI/FIR -> forwarding PLI to the source")
						sendPLI()
					}
				}
			}
		}()

		pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
			log.Printf("incoming track: kind=%s codec=%s ssrc=%d pt=%d",
				remote.Kind(), remote.Codec().MimeType, remote.SSRC(), remote.PayloadType())
			if remote.Kind() != webrtc.RTPCodecTypeVideo {
				return
			}
			inSSRC.Store(uint32(remote.SSRC()))
			sendPLI()

			var (
				first     = true
				lastSeq   uint16
				received  uint64
				lost      uint64
				reordered uint64
				bytes     uint64
				winStart  = time.Now()
				winRecv   uint64
				winLost   uint64
				winBytes  uint64
			)
			for {
				pkt, _, err := remote.ReadRTP()
				if err != nil {
					log.Println("track ended:", err)
					return
				}
				received++
				winRecv++
				size := uint64(len(pkt.Payload))
				bytes += size
				winBytes += size

				if first {
					first = false
				} else {
					gap := pkt.SequenceNumber - lastSeq
					switch {
					case gap == 1:
					case gap > 1 && gap < 0x8000:
						lost += uint64(gap - 1)
						winLost += uint64(gap - 1)
					default: // old / duplicate packet (reordering or a NACK retransmission)
						reordered++
						continue
					}
				}
				lastSeq = pkt.SequenceNumber

				if err := local.WriteRTP(pkt); err != nil {
					log.Println("write error:", err)
					return
				}

				if time.Since(winStart) >= time.Second {
					secs := time.Since(winStart).Seconds()
					pct := 0.0
					if winRecv+winLost > 0 {
						pct = 100 * float64(winLost) / float64(winRecv+winLost)
					}
					log.Printf("in: %.0f pkt/s %.0f kbps(payload) | lost this s=%d (%.1f%%) | totals recv=%d lost=%d late/dup=%d | seq=%d ts=%d",
						float64(winRecv)/secs, float64(winBytes)*8/secs/1000, winLost, pct, received, lost, reordered,
						pkt.SequenceNumber, pkt.Timestamp)
					winStart, winRecv, winLost, winBytes = time.Now(), 0, 0, 0
				}
			}
		})

		if err := pc.SetRemoteDescription(offer); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		answer, err := pc.CreateAnswer(nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		gathered := webrtc.GatheringCompletePromise(pc)
		if err := pc.SetLocalDescription(answer); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		<-gathered
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(pc.LocalDescription())
	}
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8090", "HTTP address (page + /offer)")
	flag.Parse()
	mux := http.NewServeMux()
	files := http.FileServer(http.Dir("web"))
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		files.ServeHTTP(w, r)
	}))
	api := newAPI()
	mux.HandleFunc("/offer", offerHandler(api))
	mux.HandleFunc("/publish", publishHandler(api))
	mux.HandleFunc("/subscribe", subscribeHandler(api))
	mux.HandleFunc("/ws", wsHandler(api))
	mux.HandleFunc("/simulcast", simulcastPublishHandler(api))
	mux.HandleFunc("/simulcast-sub", simSubscribeHandler())
	mux.HandleFunc("/simulcast-select", simSelectHandler)
	mux.HandleFunc("/simulcast-impair", simImpairHandler)
	mux.HandleFunc("/simulcast-sub-stats", simStatsHandler())
	mux.HandleFunc("/simulcast-sub-series", simSeriesHandler)
	log.Printf("sfu-lab echo: open http://%s/echo.html", *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}

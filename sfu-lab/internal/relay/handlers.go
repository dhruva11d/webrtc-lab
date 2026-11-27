package relay

import (
	"encoding/json"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"

	"sfu-lab/internal/sdp"
)

func PublishHandler(api *webrtc.API) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		offer, ok := sdp.DecodeOffer(w, r)
		if !ok {
			return
		}
		pc, err := api.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		pub := &publisher{pc: pc}

		pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
			log.Println("publisher connection:", s)
			if s == webrtc.PeerConnectionStateFailed || s == webrtc.PeerConnectionStateClosed {
				pubMu.Lock()
				if current == pub {
					current = nil
				}
				pubMu.Unlock()
				pc.Close()
			}
		})

		pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
			if remote.Kind() != webrtc.RTPCodecTypeVideo {
				return
			}
			if _, err := shared.get(remote.Codec().RTPCodecCapability); err != nil {
				log.Println("rejecting publisher:", err)
				pc.Close()
				return
			}
			pub.ssrc.Store(uint32(remote.SSRC()))
			pubMu.Lock()
			current = pub
			pubMu.Unlock()
			log.Printf("publisher live: codec=%s ssrc=%d", remote.Codec().MimeType, remote.SSRC())
			pub.requestKeyframe()

			var n, bytes uint64
			start := time.Now()
			for {
				pkt, _, err := remote.ReadRTP()
				if err != nil {
					log.Println("publisher track ended:", err)
					return
				}
				n++
				bytes += uint64(len(pkt.Payload))
				if err := shared.write(pkt); err != nil {
					log.Println("fan-out write:", err)
					return
				}
				if time.Since(start) >= 2*time.Second {
					s := time.Since(start).Seconds()
					log.Printf("publisher in: %.0f pkt/s %.0f kbps | subscribers=%d", float64(n)/s, float64(bytes)*8/s/1000, subs.Load())
					n, bytes, start = 0, 0, time.Now()
				}
			}
		})

		answer, err := sdp.Negotiate(pc, offer)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(answer)
	}
}

func SubscribeHandler(api *webrtc.API) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		track := shared.current()
		if track == nil {
			http.Error(w, "no publisher has ever connected", http.StatusConflict)
			return
		}
		offer, ok := sdp.DecodeOffer(w, r)
		if !ok {
			return
		}
		pc, err := api.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		sender, err := pc.AddTrack(track)
		if err != nil {
			pc.Close()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var counted atomic.Bool
		pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
			log.Println("subscriber connection:", s)
			switch s {
			case webrtc.PeerConnectionStateConnected:
				if counted.CompareAndSwap(false, true) {
					subs.Add(1)
				}
				requestKeyframe()
			case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed, webrtc.PeerConnectionStateDisconnected:
				if counted.CompareAndSwap(true, false) {
					subs.Add(-1)
				}
				if s != webrtc.PeerConnectionStateDisconnected {
					pc.Close()
				}
			}
		})

		go func() {
			for {
				pkts, _, err := sender.ReadRTCP()
				if err != nil {
					return
				}
				for _, p := range pkts {
					switch p.(type) {
					case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
						log.Println("subscriber PLI/FIR -> forwarding to publisher")
						requestKeyframe()
					}
				}
			}
		}()

		answer, err := sdp.Negotiate(pc, offer)
		if err != nil {
			pc.Close()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(answer)
	}
}

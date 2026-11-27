package simulcast

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"

	"sfu-lab/internal/adapt"
	"sfu-lab/internal/bwe"
	"sfu-lab/internal/sdp"
)

func SubscribeHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		layer := r.URL.Query().Get("layer")
		if layer == "" {
			layer = "h"
		}
		if !adapt.Valid(layer) {
			http.Error(w, "layer must be q, h or f", http.StatusBadRequest)
			return
		}
		offer, ok := sdp.DecodeOffer(w, r)
		if !ok {
			return
		}
		rig, err := bwe.NewRig()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		pc, err := rig.API.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			rig.Mux.Close()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		est, err := bwe.TakeEstimator(rig.Estimator)
		if err != nil {
			pc.Close()
			rig.Mux.Close()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}, "video", "sim-sub")
		if err != nil {
			pc.Close()
			rig.Mux.Close()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		sender, err := pc.AddTrack(track)
		if err != nil {
			pc.Close()
			rig.Mux.Close()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		buf := make([]byte, 4)
		rand.Read(buf)
		sub := &simSub{id: hex.EncodeToString(buf), est: est, shaper: rig.Shaper, mux: rig.Mux, done: make(chan struct{})}
		sub.sel = &selector{
			target:      layer,
			waitingFrom: time.Now(),
			write:       track.WriteRTP,
			askKeyframe: sim.askKeyframe,
			onSwitch: func(from, to string, waited time.Duration) {
				log.Printf("[sub %s] switched %q -> %q after waiting %v for a keyframe", sub.id, from, to, waited.Round(time.Millisecond))
			},
		}

		pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
			log.Printf("[sub %s] connection: %s", sub.id, s)
			if s == webrtc.PeerConnectionStateConnected {
				go sub.logLoop()
			}
			if s == webrtc.PeerConnectionStateFailed || s == webrtc.PeerConnectionStateClosed {
				sub.stop()
				sim.mu.Lock()
				delete(sim.subs, sub.id)
				sim.mu.Unlock()
				pc.Close()
			}
		})

		go func() {
			for {
				pkts, _, err := sender.ReadRTCP()
				if err != nil {
					return
				}
				for _, p := range pkts {
					switch rp := p.(type) {
					case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
						if cur := sub.sel.currentLayer(); cur != "" {
							sim.askKeyframe(cur)
						}
					case *rtcp.ReceiverReport:
						for _, rep := range rp.Reports {
							sub.onLoss(float64(rep.FractionLost) / 256 * 100)
						}
					}
				}
			}
		}()

		sim.mu.Lock()
		sim.subs[sub.id] = sub
		sim.mu.Unlock()

		answer, err := sdp.Negotiate(pc, offer)
		if err != nil {
			pc.Close()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		sim.mu.RLock()
		live := len(sim.layers)
		sim.mu.RUnlock()
		log.Printf("[sub %s] subscribed, wants layer %q, live publisher layers: %d", sub.id, layer, live)
		if live == 0 {
			log.Printf("[sub %s] no publisher is live: it stays black until simulcast.html publishes", sub.id)
		}
		w.Header().Set("X-Publisher-Live", strconv.Itoa(live))
		w.Header().Set("X-Sub-Id", sub.id)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(answer)
	}
}

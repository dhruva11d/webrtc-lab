package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/ice/v4"
	"github.com/pion/interceptor/pkg/cc"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

type simLayer struct {
	pc   *webrtc.PeerConnection
	ssrc uint32
}

func (l *simLayer) requestKeyframe() {
	l.pc.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: l.ssrc}})
}

type simSub struct {
	id   string
	sel  *selector
	auto atomic.Bool
	adMu sync.Mutex
	ad   *adapter

	bweMode atomic.Bool
	bweAd   *bweAdapter

	serMu   sync.Mutex
	series  []seriesPoint
	started time.Time

	est       cc.BandwidthEstimator
	shaper    *shapedConn
	mux       ice.UDPMux
	done      chan struct{}
	closeOnce sync.Once
}

func (s *simSub) policyName() string {
	if s.bweMode.Load() {
		return "bwe"
	}
	return "loss"
}

func (s *simSub) onEstimate(estKbps float64, now time.Time) {
	if !s.auto.Load() || !s.bweMode.Load() {
		return
	}
	s.adMu.Lock()
	layer, changed := s.bweAd.observe(estKbps, now)
	s.adMu.Unlock()
	if changed {
		log.Printf("[sub %s] auto(bwe): estimate %.0f kbps, moving to layer %q", s.id, estKbps, layer)
		s.sel.setTarget(layer)
	}
}

func (s *simSub) onLoss(lossPct float64) {
	if !s.auto.Load() || s.bweMode.Load() {
		return
	}
	s.adMu.Lock()
	layer, changed := s.ad.observe(lossPct, time.Now())
	s.adMu.Unlock()
	if lossPct >= lossUpPct || changed {
		log.Printf("[sub %s] auto: reported loss %.1f%% on %q", s.id, lossPct, s.sel.currentLayer())
	}
	if changed {
		log.Printf("[sub %s] auto: moving to layer %q", s.id, layer)
		s.sel.setTarget(layer)
	}
}

func (s *simSub) setAuto(on, bwe bool) {
	if on {
		s.adMu.Lock()
		s.ad = newAdapter(s.sel.targetLayer())
		s.bweAd = newBWEAdapter(s.sel.targetLayer())
		s.adMu.Unlock()
		s.bweMode.Store(bwe)
	}
	s.auto.Store(on)
}

type simRegistry struct {
	mu     sync.RWMutex
	layers map[string]*simLayer
	subs   map[string]*simSub
}

var sim = &simRegistry{layers: map[string]*simLayer{}, subs: map[string]*simSub{}}

func (r *simRegistry) newPublisher() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.layers = map[string]*simLayer{}
	for _, s := range r.subs {
		s.sel.reset()
	}
}

func (r *simRegistry) setLayer(rid string, l *simLayer) {
	r.mu.Lock()
	r.layers[rid] = l
	r.mu.Unlock()
}

func (r *simRegistry) askKeyframe(rid string) {
	r.mu.RLock()
	l := r.layers[rid]
	r.mu.RUnlock()
	if l != nil {
		l.requestKeyframe()
	}
}

func (r *simRegistry) dispatch(rid string, pkt *rtp.Packet) {
	r.mu.RLock()
	subs := make([]*simSub, 0, len(r.subs))
	for _, s := range r.subs {
		subs = append(subs, s)
	}
	r.mu.RUnlock()
	for _, s := range subs {
		s.sel.handle(rid, pkt)
	}
}

func validLayer(rid string) bool { return rid == "q" || rid == "h" || rid == "f" }

func simSubscribeHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		layer := r.URL.Query().Get("layer")
		if layer == "" {
			layer = "h"
		}
		if !validLayer(layer) {
			http.Error(w, "layer must be q, h or f", http.StatusBadRequest)
			return
		}
		offer, ok := decodeOffer(w, r)
		if !ok {
			return
		}
		rig, err := newBWERig()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		pc, err := rig.api.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			rig.mux.Close()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		est, err := takeEstimator(rig.est)
		if err != nil {
			pc.Close()
			rig.mux.Close()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}, "video", "sim-sub")
		if err != nil {
			pc.Close()
			rig.mux.Close()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		sender, err := pc.AddTrack(track)
		if err != nil {
			pc.Close()
			rig.mux.Close()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		buf := make([]byte, 4)
		rand.Read(buf)
		sub := &simSub{id: hex.EncodeToString(buf), est: est, shaper: rig.shaper, mux: rig.mux, done: make(chan struct{})}
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

		answer, err := negotiate(pc, offer)
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

func simSelectHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "use POST", http.StatusMethodNotAllowed)
		return
	}
	layer := r.URL.Query().Get("layer")
	if layer != "auto" && !validLayer(layer) {
		http.Error(w, "layer must be q, h, f or auto", http.StatusBadRequest)
		return
	}
	sub := findSub(r)
	if sub == nil {
		http.Error(w, "unknown subscriber", http.StatusNotFound)
		return
	}
	if layer == "auto" {
		sub.setAuto(true, r.URL.Query().Get("policy") == "bwe")
		log.Printf("[sub %s] auto mode on (policy %s), starting from %q", sub.id, sub.policyName(), sub.sel.targetLayer())
	} else {
		sub.setAuto(false, false)
		log.Printf("[sub %s] manual: asked for layer %q (currently %q)", sub.id, layer, sub.sel.currentLayer())
		sub.sel.setTarget(layer)
	}
	w.WriteHeader(http.StatusNoContent)
}

func findSub(r *http.Request) *simSub {
	sim.mu.RLock()
	defer sim.mu.RUnlock()
	return sim.subs[r.URL.Query().Get("id")]
}

func simImpairHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "use POST", http.StatusMethodNotAllowed)
		return
	}
	sub := findSub(r)
	if sub == nil {
		http.Error(w, "unknown subscriber", http.StatusNotFound)
		return
	}
	loss, err1 := strconv.ParseFloat(r.URL.Query().Get("loss"), 64)
	capKbps, err2 := strconv.ParseFloat(r.URL.Query().Get("cap"), 64)
	if (r.URL.Query().Get("loss") != "" && err1 != nil) || (r.URL.Query().Get("cap") != "" && err2 != nil) || loss < 0 || loss > 100 || capKbps < 0 {
		http.Error(w, "loss must be 0-100 and cap >= 0", http.StatusBadRequest)
		return
	}
	sub.shaper.set(loss, capKbps)
	log.Printf("[sub %s] impairment: loss=%.0f%% cap=%.0f kbps", sub.id, loss, capKbps)
	w.WriteHeader(http.StatusNoContent)
}

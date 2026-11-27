package main

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/pion/ice/v4"
	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/cc"
	"github.com/pion/interceptor/pkg/gcc"
	"github.com/pion/webrtc/v4"
)

const (
	bweInitial = 500_000
	bweMin     = 100_000
	bweMax     = 5_000_000
)

type bweRig struct {
	api    *webrtc.API
	est    <-chan cc.BandwidthEstimator
	shaper *shapedConn
	mux    ice.UDPMux
}

func newBWERig() (*bweRig, error) {
	m := &webrtc.MediaEngine{}
	if err := m.RegisterDefaultCodecs(); err != nil {
		return nil, err
	}
	ir := &interceptor.Registry{}
	controller, err := cc.NewInterceptor(func() (cc.BandwidthEstimator, error) {
		return gcc.NewSendSideBWE(
			gcc.SendSideBWEInitialBitrate(bweInitial),
			gcc.SendSideBWEMinBitrate(bweMin),
			gcc.SendSideBWEMaxBitrate(bweMax),
		)
	})
	if err != nil {
		return nil, err
	}
	ch := make(chan cc.BandwidthEstimator, 1)
	controller.OnNewPeerConnection(func(_ string, e cc.BandwidthEstimator) {
		select {
		case ch <- e:
		default:
		}
	})
	ir.Add(controller)
	if err := webrtc.ConfigureTWCCHeaderExtensionSender(m, ir); err != nil {
		return nil, err
	}
	if err := webrtc.RegisterDefaultInterceptors(m, ir); err != nil {
		return nil, err
	}
	raw, err := net.ListenPacket("udp4", "0.0.0.0:0")
	if err != nil {
		return nil, err
	}
	shaper := newShapedConn(raw)
	mux := ice.NewUDPMuxDefault(ice.UDPMuxParams{UDPConn: shaper})
	se := webrtc.SettingEngine{}
	se.SetICEUDPMux(mux)
	se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	api := webrtc.NewAPI(webrtc.WithMediaEngine(m), webrtc.WithInterceptorRegistry(ir), webrtc.WithSettingEngine(se))
	return &bweRig{api: api, est: ch, shaper: shaper, mux: mux}, nil
}

func takeEstimator(ch <-chan cc.BandwidthEstimator) (cc.BandwidthEstimator, error) {
	select {
	case e := <-ch:
		return e, nil
	case <-time.After(time.Second):
		return nil, errors.New("bandwidth estimator was not created")
	}
}

type sampler struct {
	bytes uint64
	at    time.Time
}

func (s *sampler) kbps(b uint64, now time.Time) float64 {
	defer func() { s.bytes, s.at = b, now }()
	if s.at.IsZero() || !now.After(s.at) {
		return 0
	}
	return float64(b-s.bytes) * 8 / now.Sub(s.at).Seconds() / 1000
}

type subStats struct {
	EstimateKbps float64 `json:"estimate_kbps"`
	LossKbps     float64 `json:"loss_based_kbps"`
	DelayKbps    float64 `json:"delay_based_kbps"`
	Usage        string  `json:"usage"`
	State        string  `json:"state"`
	Layer        string  `json:"layer"`
	Target       string  `json:"target"`
	SendKbps     float64 `json:"send_kbps"`
	Dropped      uint64  `json:"dropped"`
	Auto         bool    `json:"auto"`
	Policy       string  `json:"policy"`
}

func num(v any) float64 {
	switch x := v.(type) {
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case float64:
		return x
	}
	return 0
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func (s *simSub) stats(sp *sampler, now time.Time) subStats {
	g := s.est.GetStats()
	layer, target, bytes := s.sel.snapshot()
	return subStats{
		EstimateKbps: float64(s.est.GetTargetBitrate()) / 1000,
		LossKbps:     num(g["lossTargetBitrate"]) / 1000,
		DelayKbps:    num(g["delayTargetBitrate"]) / 1000,
		Usage:        str(g["usage"]),
		State:        str(g["state"]),
		Layer:        layer,
		Target:       target,
		SendKbps:     sp.kbps(bytes, now),
		Dropped:      s.shaper.dropped.Load(),
		Auto:         s.auto.Load(),
		Policy:       s.policyName(),
	}
}

type seriesPoint struct {
	T       float64 `json:"t"`
	Est     float64 `json:"est"`
	Loss    float64 `json:"loss_based"`
	Delay   float64 `json:"delay_based"`
	Send    float64 `json:"send"`
	Layer   string  `json:"layer"`
	Cap     float64 `json:"cap"`
	LossPct float64 `json:"loss_pct"`
	Policy  string  `json:"policy"`
	Auto    bool    `json:"auto"`
}

const (
	seriesMax  = 3600
	seriesTrim = 600
)

func trimSeries(p []seriesPoint) []seriesPoint {
	if len(p) <= seriesMax {
		return p
	}
	return append([]seriesPoint(nil), p[seriesTrim:]...)
}

func (s *simSub) record(sp *sampler, now time.Time) {
	g := s.est.GetStats()
	layer, _, bytes := s.sel.snapshot()
	lossPct, rate := s.shaper.get()
	p := seriesPoint{
		T:       now.Sub(s.started).Seconds(),
		Est:     float64(s.est.GetTargetBitrate()) / 1000,
		Loss:    num(g["lossTargetBitrate"]) / 1000,
		Delay:   num(g["delayTargetBitrate"]) / 1000,
		Send:    sp.kbps(bytes, now),
		Layer:   layer,
		Cap:     rate,
		LossPct: lossPct,
		Policy:  s.policyName(),
		Auto:    s.auto.Load(),
	}
	s.serMu.Lock()
	s.series = trimSeries(append(s.series, p))
	s.serMu.Unlock()
}

func simSeriesHandler(w http.ResponseWriter, r *http.Request) {
	sub := findSub(r)
	if sub == nil {
		http.Error(w, "unknown subscriber", http.StatusNotFound)
		return
	}
	sub.serMu.Lock()
	out := append([]seriesPoint(nil), sub.series...)
	sub.serMu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

func (s *simSub) logLoop() {
	var sp, sp1 sampler
	s.started = time.Now()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	ticks := 0
	for {
		select {
		case <-s.done:
			return
		case now := <-t.C:
			s.onEstimate(float64(s.est.GetTargetBitrate())/1000, now)
			s.record(&sp1, now)
			ticks++
			if ticks%2 != 0 {
				continue
			}
			st := s.stats(&sp, now)
			log.Printf("[sub %s] bwe: estimate %.0f kbps (loss-based %.0f, delay-based %.0f; usage=%s state=%s) | sending %.0f kbps on %q, dropped=%d",
				s.id, st.EstimateKbps, st.LossKbps, st.DelayKbps, st.Usage, st.State, st.SendKbps, st.Layer, st.Dropped)
		}
	}
}

func (s *simSub) stop() {
	s.closeOnce.Do(func() {
		close(s.done)
		s.est.Close()
		s.mux.Close()
	})
}

func simStatsHandler() http.HandlerFunc {
	var mu sync.Mutex
	samplers := map[string]*sampler{}
	return func(w http.ResponseWriter, r *http.Request) {
		sub := findSub(r)
		if sub == nil {
			http.Error(w, "unknown subscriber", http.StatusNotFound)
			return
		}
		mu.Lock()
		sp := samplers[sub.id]
		if sp == nil {
			sp = &sampler{}
			samplers[sub.id] = sp
		}
		st := sub.stats(sp, time.Now())
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(st)
	}
}

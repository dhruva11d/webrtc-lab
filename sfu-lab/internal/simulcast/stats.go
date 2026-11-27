package simulcast

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"sfu-lab/internal/bwe"
)

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

func (s *simSub) stats(sp *bwe.Sampler, now time.Time) subStats {
	e := bwe.Read(s.est)
	layer, target, bytes := s.sel.snapshot()
	return subStats{
		EstimateKbps: e.Kbps,
		LossKbps:     e.LossKbps,
		DelayKbps:    e.DelayKbps,
		Usage:        e.Usage,
		State:        e.State,
		Layer:        layer,
		Target:       target,
		SendKbps:     sp.Kbps(bytes, now),
		Dropped:      s.shaper.Dropped(),
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

func (s *simSub) record(sp *bwe.Sampler, now time.Time) {
	e := bwe.Read(s.est)
	layer, _, bytes := s.sel.snapshot()
	lossPct, rate := s.shaper.Get()
	p := seriesPoint{
		T:       now.Sub(s.started).Seconds(),
		Est:     e.Kbps,
		Loss:    e.LossKbps,
		Delay:   e.DelayKbps,
		Send:    sp.Kbps(bytes, now),
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

func SeriesHandler(w http.ResponseWriter, r *http.Request) {
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
	var sp, sp1 bwe.Sampler
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

func StatsHandler() http.HandlerFunc {
	var mu sync.Mutex
	samplers := map[string]*bwe.Sampler{}
	return func(w http.ResponseWriter, r *http.Request) {
		sub := findSub(r)
		if sub == nil {
			http.Error(w, "unknown subscriber", http.StatusNotFound)
			return
		}
		mu.Lock()
		sp := samplers[sub.id]
		if sp == nil {
			sp = &bwe.Sampler{}
			samplers[sub.id] = sp
		}
		st := sub.stats(sp, time.Now())
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(st)
	}
}

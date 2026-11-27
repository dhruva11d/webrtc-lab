package simulcast

import (
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/ice/v4"
	"github.com/pion/interceptor/pkg/cc"

	"sfu-lab/internal/adapt"
	"sfu-lab/internal/netem"
)

type simSub struct {
	id   string
	sel  *selector
	auto atomic.Bool
	adMu sync.Mutex
	ad   *adapt.LossAdapter

	bweMode atomic.Bool
	bweAd   *adapt.BWEAdapter

	serMu   sync.Mutex
	series  []seriesPoint
	started time.Time

	est       cc.BandwidthEstimator
	shaper    *netem.ShapedConn
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
	layer, changed := s.bweAd.Observe(estKbps, now)
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
	layer, changed := s.ad.Observe(lossPct, time.Now())
	s.adMu.Unlock()
	if lossPct >= adapt.LossUpPct || changed {
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
		s.ad = adapt.NewLossAdapter(s.sel.targetLayer())
		s.bweAd = adapt.NewBWEAdapter(s.sel.targetLayer())
		s.adMu.Unlock()
		s.bweMode.Store(bwe)
	}
	s.auto.Store(on)
}

func findSub(r *http.Request) *simSub {
	sim.mu.RLock()
	defer sim.mu.RUnlock()
	return sim.subs[r.URL.Query().Get("id")]
}

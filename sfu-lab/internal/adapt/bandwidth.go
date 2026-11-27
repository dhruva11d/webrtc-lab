package adapt

import "time"

var layerNeedKbps = map[string]float64{"q": 150, "h": 500, "f": 1500}

const (
	bweHeadroom    = 0.85
	bweDownNeeded  = 2
	bweUpNeeded    = 5
	bweLayerMargin = 1.0
)

type BWEAdapter struct {
	idx        int
	low, high  int
	lastChange time.Time
	lastUp     time.Time
	upWait     time.Duration
}

func NewBWEAdapter(layer string) *BWEAdapter {
	a := &BWEAdapter{idx: 1, upWait: baseUpWait}
	for i, l := range layerOrder {
		if l == layer {
			a.idx = i
		}
	}
	return a
}

func fittingLayer(estKbps float64) int {
	best := 0
	for i, l := range layerOrder {
		if estKbps*bweHeadroom >= layerNeedKbps[l]*bweLayerMargin {
			best = i
		}
	}
	return best
}

func (a *BWEAdapter) Observe(estKbps float64, now time.Time) (string, bool) {
	fit := fittingLayer(estKbps)
	switch {
	case fit < a.idx:
		a.low++
		a.high = 0
	case fit > a.idx:
		a.high++
		a.low = 0
		if !a.lastUp.IsZero() && now.Sub(a.lastUp) > stableToBase {
			a.upWait = baseUpWait
		}
	default:
		a.low, a.high = 0, 0
	}

	if a.low >= bweDownNeeded && now.Sub(a.lastChange) >= minHold {
		if !a.lastUp.IsZero() && now.Sub(a.lastUp) < failedProbe {
			a.upWait = min(a.upWait*2, maxUpWait)
		}
		a.idx = fit
		a.low, a.high = 0, 0
		a.lastChange = now
		return layerOrder[a.idx], true
	}
	if a.high >= bweUpNeeded && now.Sub(a.lastChange) >= a.upWait {
		a.idx++
		a.low, a.high = 0, 0
		a.lastChange = now
		a.lastUp = now
		return layerOrder[a.idx], true
	}
	return "", false
}

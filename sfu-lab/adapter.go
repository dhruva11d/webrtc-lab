package main

import "time"

var layerOrder = []string{"q", "h", "f"}

const (
	lossDownPct  = 5.0
	lossUpPct    = 1.0
	downNeeded   = 2
	upNeeded     = 8
	minHold      = 2 * time.Second
	baseUpWait   = 10 * time.Second
	maxUpWait    = 60 * time.Second
	failedProbe  = 15 * time.Second
	stableToBase = 30 * time.Second
)

type adapter struct {
	idx        int
	bad, good  int
	lastChange time.Time
	lastUp     time.Time
	upWait     time.Duration
}

func newAdapter(layer string) *adapter {
	a := &adapter{idx: 1, upWait: baseUpWait}
	for i, l := range layerOrder {
		if l == layer {
			a.idx = i
		}
	}
	return a
}

func (a *adapter) observe(lossPct float64, now time.Time) (string, bool) {
	switch {
	case lossPct >= lossDownPct:
		a.bad++
		a.good = 0
	case lossPct < lossUpPct:
		a.good++
		a.bad = 0
		if !a.lastUp.IsZero() && now.Sub(a.lastUp) > stableToBase {
			a.upWait = baseUpWait
		}
	default:
		a.bad, a.good = 0, 0
	}

	if a.bad >= downNeeded && a.idx > 0 && now.Sub(a.lastChange) >= minHold {
		if !a.lastUp.IsZero() && now.Sub(a.lastUp) < failedProbe {
			a.upWait = min(a.upWait*2, maxUpWait)
		}
		a.idx--
		a.bad, a.good = 0, 0
		a.lastChange = now
		return layerOrder[a.idx], true
	}
	if a.good >= upNeeded && a.idx < len(layerOrder)-1 && now.Sub(a.lastChange) >= a.upWait {
		a.idx++
		a.bad, a.good = 0, 0
		a.lastChange = now
		a.lastUp = now
		return layerOrder[a.idx], true
	}
	return "", false
}

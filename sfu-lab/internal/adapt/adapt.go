package adapt

import "time"

var layerOrder = []string{"q", "h", "f"}

const (
	lossDownPct  = 5.0
	LossUpPct    = 1.0
	downNeeded   = 2
	upNeeded     = 8
	minHold      = 2 * time.Second
	baseUpWait   = 10 * time.Second
	maxUpWait    = 60 * time.Second
	failedProbe  = 15 * time.Second
	stableToBase = 30 * time.Second
)

func Valid(rid string) bool {
	for _, l := range layerOrder {
		if l == rid {
			return true
		}
	}
	return false
}

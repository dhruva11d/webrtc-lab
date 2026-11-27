package bwe

import "github.com/pion/interceptor/pkg/cc"

type Estimate struct {
	Kbps      float64
	LossKbps  float64
	DelayKbps float64
	Usage     string
	State     string
}

func Read(e cc.BandwidthEstimator) Estimate {
	g := e.GetStats()
	return Estimate{
		Kbps:      float64(e.GetTargetBitrate()) / 1000,
		LossKbps:  num(g["lossTargetBitrate"]) / 1000,
		DelayKbps: num(g["delayTargetBitrate"]) / 1000,
		Usage:     str(g["usage"]),
		State:     str(g["state"]),
	}
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

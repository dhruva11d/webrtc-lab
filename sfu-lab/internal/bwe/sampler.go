package bwe

import "time"

type Sampler struct {
	bytes uint64
	at    time.Time
}

func (s *Sampler) Kbps(b uint64, now time.Time) float64 {
	defer func() { s.bytes, s.at = b, now }()
	if s.at.IsZero() || !now.After(s.at) {
		return 0
	}
	return float64(b-s.bytes) * 8 / now.Sub(s.at).Seconds() / 1000
}

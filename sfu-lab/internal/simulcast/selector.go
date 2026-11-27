package simulcast

import (
	"sync"
	"time"

	"github.com/pion/rtp"

	"sfu-lab/internal/rtprewrite"
	"sfu-lab/internal/vp8"
)

type selector struct {
	mu          sync.Mutex
	current     string
	target      string
	waitingFrom time.Time
	lastAsk     time.Time
	rw          rtprewrite.Rewriter
	write       func(*rtp.Packet) error
	askKeyframe func(rid string)
	onSwitch    func(from, to string, waited time.Duration)

	fwdBytes uint64
}

func (s *selector) snapshot() (current, target string, bytes uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current, s.target, s.fwdBytes
}

func (s *selector) targetLayer() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.target
}

func (s *selector) setTarget(rid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rid != s.target {
		s.target = rid
		s.waitingFrom = time.Now()
		s.lastAsk = time.Time{}
	}
}

func (s *selector) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.current = ""
	s.waitingFrom = time.Now()
	s.lastAsk = time.Time{}
}

func (s *selector) currentLayer() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current
}

func (s *selector) handle(rid string, pkt *rtp.Packet) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()

	if rid == s.target && rid != s.current {
		if !vp8.IsKeyframe(pkt.Payload) {
			if now.Sub(s.lastAsk) > 500*time.Millisecond {
				s.lastAsk = now
				if s.askKeyframe != nil {
					s.askKeyframe(rid)
				}
			}
			return
		}
		from := s.current
		s.current = rid
		s.rw.BeginSource()
		if s.onSwitch != nil {
			s.onSwitch(from, rid, now.Sub(s.waitingFrom))
		}
	}
	if rid != s.current {
		return
	}
	out := pkt.Clone()
	s.rw.Rewrite(out, now)
	s.fwdBytes += uint64(len(out.Payload) + 12)
	s.write(out)
}

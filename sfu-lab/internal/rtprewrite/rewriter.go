package rtprewrite

import (
	"time"

	"github.com/pion/rtp"
)

const videoClockRate = 90000

type Rewriter struct {
	started bool
	fresh   bool
	lastSeq uint16
	lastTS  uint32
	lastAt  time.Time
	seqOff  uint16
	tsOff   uint32
}

func (r *Rewriter) BeginSource() { r.fresh = true }

func (r *Rewriter) Rewrite(p *rtp.Packet, now time.Time) {
	if r.fresh {
		if r.started {
			r.seqOff = r.lastSeq + 1 - p.SequenceNumber
			elapsed := uint32(now.Sub(r.lastAt).Seconds() * videoClockRate)
			if elapsed == 0 {
				elapsed = 1
			}
			r.tsOff = r.lastTS + elapsed - p.Timestamp
		}
		r.fresh = false
	}
	p.SequenceNumber += r.seqOff
	p.Timestamp += r.tsOff
	r.lastSeq = p.SequenceNumber
	r.lastTS = p.Timestamp
	r.lastAt = now
	r.started = true
}

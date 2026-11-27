package rtprewrite

import (
	"testing"
	"time"

	"github.com/pion/rtp"
)

func pkt(seq uint16, ts uint32) *rtp.Packet {
	return &rtp.Packet{Header: rtp.Header{SequenceNumber: seq, Timestamp: ts}}
}

func TestFirstSourcePassesThrough(t *testing.T) {
	var r Rewriter
	r.BeginSource()
	p := pkt(100, 5000)
	r.Rewrite(p, time.Now())
	if p.SequenceNumber != 100 || p.Timestamp != 5000 {
		t.Fatalf("first source must be unchanged, got seq=%d ts=%d", p.SequenceNumber, p.Timestamp)
	}
}

func TestSwitchKeepsSequenceContinuousAndTimestampForward(t *testing.T) {
	var r Rewriter
	now := time.Now()
	r.BeginSource()
	for i := uint16(0); i < 3; i++ {
		r.Rewrite(pkt(100+i, 1000+uint32(i)*3000), now.Add(time.Duration(i)*33*time.Millisecond))
	}
	lastTS := r.lastTS

	r.BeginSource()
	p := pkt(40000, 7)
	r.Rewrite(p, now.Add(time.Second))
	if p.SequenceNumber != 103 {
		t.Fatalf("sequence must continue at 103, got %d", p.SequenceNumber)
	}
	if p.Timestamp <= lastTS {
		t.Fatalf("timestamp must move forward: last %d, new %d", lastTS, p.Timestamp)
	}
	if gap := p.Timestamp - lastTS; gap < 80000 || gap > 100000 {
		t.Fatalf("a ~1 s pause should be ~90000 ticks, got %d", gap)
	}

	next := pkt(40001, 7+3000)
	r.Rewrite(next, now.Add(time.Second+33*time.Millisecond))
	if next.SequenceNumber != 104 || next.Timestamp-p.Timestamp != 3000 {
		t.Fatalf("spacing inside one source must be preserved, got seq=%d dts=%d", next.SequenceNumber, next.Timestamp-p.Timestamp)
	}
}

func TestSequenceWrapAround(t *testing.T) {
	var r Rewriter
	now := time.Now()
	r.BeginSource()
	r.Rewrite(pkt(65535, 10), now)
	r.BeginSource()
	p := pkt(500, 20)
	r.Rewrite(p, now.Add(time.Second))
	if p.SequenceNumber != 0 {
		t.Fatalf("sequence after 65535 must wrap to 0, got %d", p.SequenceNumber)
	}
}

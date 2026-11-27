package main

import (
	"testing"
	"time"

	"github.com/pion/rtp"
)

var (
	keyPayload   = []byte{0x10, 0x10, 0x00, 0x00}
	deltaPayload = []byte{0x10, 0x11, 0x00, 0x00}
)

func mk(seq uint16, ts uint32, payload []byte) *rtp.Packet {
	return &rtp.Packet{Header: rtp.Header{SequenceNumber: seq, Timestamp: ts}, Payload: payload}
}

type harness struct {
	s     *selector
	out   []*rtp.Packet
	asks  []string
	moves []string
}

func newHarness(target string) *harness {
	h := &harness{}
	h.s = &selector{
		target:      target,
		write:       func(p *rtp.Packet) error { h.out = append(h.out, p); return nil },
		askKeyframe: func(rid string) { h.asks = append(h.asks, rid) },
		onSwitch:    func(from, to string, _ time.Duration) { h.moves = append(h.moves, from+">"+to) },
	}
	return h
}

func TestWaitsForKeyframeOnTarget(t *testing.T) {
	h := newHarness("h")
	h.s.handle("q", mk(1, 100, keyPayload))
	h.s.handle("h", mk(500, 100, deltaPayload))
	if len(h.out) != 0 {
		t.Fatalf("nothing may be forwarded before a keyframe on the target layer, got %d", len(h.out))
	}
	if len(h.asks) != 1 || h.asks[0] != "h" {
		t.Fatalf("must ask the target layer for a keyframe once, got %v", h.asks)
	}
	h.s.handle("h", mk(501, 103, keyPayload))
	h.s.handle("h", mk(502, 106, deltaPayload))
	if len(h.out) != 2 {
		t.Fatalf("keyframe and the delta after it must be forwarded, got %d", len(h.out))
	}
	if h.s.currentLayer() != "h" {
		t.Fatalf("current layer must be h, got %q", h.s.currentLayer())
	}
}

func TestSwitchKeepsOldLayerUntilKeyframeAndStaysContinuous(t *testing.T) {
	h := newHarness("h")
	h.s.handle("h", mk(10, 1000, keyPayload))
	h.s.handle("h", mk(11, 1003, deltaPayload))
	h.s.setTarget("f")

	h.s.handle("h", mk(12, 1006, deltaPayload))
	h.s.handle("f", mk(900, 5000, deltaPayload))
	if len(h.out) != 3 || h.s.currentLayer() != "h" {
		t.Fatalf("old layer must keep flowing until the new keyframe; out=%d current=%q", len(h.out), h.s.currentLayer())
	}

	h.s.handle("f", mk(901, 5003, keyPayload))
	h.s.handle("h", mk(13, 1009, deltaPayload))
	h.s.handle("f", mk(902, 5006, deltaPayload))
	if h.s.currentLayer() != "f" {
		t.Fatalf("must have switched to f, got %q", h.s.currentLayer())
	}
	if len(h.out) != 5 {
		t.Fatalf("after the switch only f may flow: want 5 packets, got %d", len(h.out))
	}
	for i := 1; i < len(h.out); i++ {
		if h.out[i].SequenceNumber != h.out[i-1].SequenceNumber+1 {
			t.Fatalf("output sequence must be continuous at index %d: %d then %d", i, h.out[i-1].SequenceNumber, h.out[i].SequenceNumber)
		}
		if h.out[i].Timestamp < h.out[i-1].Timestamp {
			t.Fatalf("timestamp went backwards at index %d", i)
		}
	}
	if len(h.moves) != 2 || h.moves[1] != "h>f" {
		t.Fatalf("expected switches [>h h>f], got %v", h.moves)
	}
}

func TestPacketsAreClonedPerSubscriber(t *testing.T) {
	a, b := newHarness("h"), newHarness("h")
	shared := mk(7, 100, keyPayload)
	a.s.handle("h", shared)
	b.s.handle("h", shared)
	if shared.SequenceNumber != 7 || shared.Timestamp != 100 {
		t.Fatalf("the shared source packet must not be modified, got seq=%d ts=%d", shared.SequenceNumber, shared.Timestamp)
	}
}

func TestResetWaitsForANewKeyframe(t *testing.T) {
	h := newHarness("h")
	h.s.handle("h", mk(1, 0, keyPayload))
	h.s.reset()
	h.s.handle("h", mk(2, 3, deltaPayload))
	if len(h.out) != 1 {
		t.Fatalf("after reset a delta frame must not be forwarded, got %d packets", len(h.out))
	}
	h.s.handle("h", mk(3, 6, keyPayload))
	if len(h.out) != 2 {
		t.Fatalf("a keyframe must resume forwarding, got %d packets", len(h.out))
	}
}

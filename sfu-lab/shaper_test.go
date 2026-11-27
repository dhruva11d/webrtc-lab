package main

import (
	"net"
	"testing"
	"time"
)

func pair(t *testing.T) (*shapedConn, net.PacketConn) {
	t.Helper()
	rx, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := newShapedConn(tx)
	t.Cleanup(func() { s.Close(); rx.Close() })
	return s, rx
}

func count(rx net.PacketConn, wait time.Duration) int {
	buf := make([]byte, 2000)
	n := 0
	rx.SetReadDeadline(time.Now().Add(wait))
	for {
		if _, _, err := rx.ReadFrom(buf); err != nil {
			return n
		}
		n++
	}
}

func TestTotalLossDeliversNothing(t *testing.T) {
	s, rx := pair(t)
	s.set(100, 0)
	for i := 0; i < 20; i++ {
		s.WriteTo(make([]byte, 100), rx.LocalAddr())
	}
	if got := count(rx, 200*time.Millisecond); got != 0 {
		t.Fatalf("want 0 delivered, got %d", got)
	}
	if s.dropped.Load() != 20 {
		t.Fatalf("want 20 dropped, got %d", s.dropped.Load())
	}
}

func TestRateCapDelaysAndTailDrops(t *testing.T) {
	s, rx := pair(t)
	s.set(0, 800)
	for i := 0; i < 100; i++ {
		s.WriteTo(make([]byte, 1000), rx.LocalAddr())
	}
	got := count(rx, 600*time.Millisecond)
	if got < 15 || got > 40 {
		t.Fatalf("800 kbps with a 200 ms queue should pass about 20 packets of 1000 bytes, got %d", got)
	}
	if s.dropped.Load() == 0 {
		t.Fatal("expected tail drops")
	}
}

func TestNoImpairmentPassesEverything(t *testing.T) {
	s, rx := pair(t)
	for i := 0; i < 50; i++ {
		s.WriteTo(make([]byte, 100), rx.LocalAddr())
	}
	if got := count(rx, 300*time.Millisecond); got != 50 {
		t.Fatalf("want 50, got %d", got)
	}
}

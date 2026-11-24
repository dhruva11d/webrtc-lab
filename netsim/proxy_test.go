package main

import (
	"context"
	"net"
	"testing"
	"time"
)

func startEcho(t *testing.T) string {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	go func() {
		buf := make([]byte, 65535)
		for {
			n, a, err := c.ReadFromUDP(buf)
			if err != nil {
				return
			}
			c.WriteToUDP(buf[:n], a)
		}
	}()
	return c.LocalAddr().String()
}

func startProxy(t *testing.T, s Settings) (*Proxy, *net.UDPConn) {
	t.Helper()
	p, err := NewProxy("127.0.0.1:0", startEcho(t), s)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go p.Run(ctx)
	cl, err := net.DialUDP("udp", nil, p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	return p, cl
}

func countEchoes(t *testing.T, cl *net.UDPConn, n, size int, wait time.Duration) int {
	t.Helper()
	msg := make([]byte, size)
	for i := 0; i < n; i++ {
		cl.Write(msg)
	}
	got := 0
	buf := make([]byte, 65535)
	deadline := time.Now().Add(wait)
	for {
		cl.SetReadDeadline(deadline)
		if _, err := cl.Read(buf); err != nil {
			return got
		}
		got++
	}
}

func TestPassThrough(t *testing.T) {
	_, cl := startProxy(t, Settings{})
	if got := countEchoes(t, cl, 200, 100, time.Second); got != 200 {
		t.Fatalf("no impairment: want 200 echoes, got %d", got)
	}
}

func TestLossAppliesBothDirections(t *testing.T) {
	_, cl := startProxy(t, Settings{LossPct: 20})
	got := countEchoes(t, cl, 600, 100, time.Second)
	if got < 300 || got > 470 {
		t.Fatalf("20%% loss each way: expected about 384 of 600 echoes, got %d", got)
	}
}

func TestDelay(t *testing.T) {
	_, cl := startProxy(t, Settings{DelayMs: 50})
	start := time.Now()
	cl.Write([]byte("x"))
	buf := make([]byte, 100)
	cl.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := cl.Read(buf); err != nil {
		t.Fatal(err)
	}
	rtt := time.Since(start)
	if rtt < 90*time.Millisecond || rtt > 400*time.Millisecond {
		t.Fatalf("50 ms each way: expected RTT about 100 ms, got %v", rtt)
	}
}

func TestBottleneckDropsBurst(t *testing.T) {
	p, cl := startProxy(t, Settings{RateKbps: 800, MaxQueueMs: 50})
	got := countEchoes(t, cl, 200, 1000, 2*time.Second)
	if got < 1 || got > 20 {
		t.Fatalf("expected a handful of survivors, got %d (counters %+v)", got, p.Counters())
	}
	if p.Counters().UpDropped == 0 {
		t.Fatal("expected tail drops on the upstream queue")
	}
}

func TestSettingsChangeLive(t *testing.T) {
	p, cl := startProxy(t, Settings{})
	if got := countEchoes(t, cl, 100, 100, time.Second); got != 100 {
		t.Fatalf("before: want 100, got %d", got)
	}
	p.SetSettings(Settings{LossPct: 100})
	if got := countEchoes(t, cl, 100, 100, 500*time.Millisecond); got != 0 {
		t.Fatalf("after 100%% loss: want 0, got %d", got)
	}
}

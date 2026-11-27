package bwe

import (
	"testing"
	"time"
)

func TestSamplerReportsRateBetweenCalls(t *testing.T) {
	var sp Sampler
	t0 := time.Now()
	if got := sp.Kbps(0, t0); got != 0 {
		t.Fatalf("first call has no interval, got %v", got)
	}
	got := sp.Kbps(125_000, t0.Add(time.Second))
	if got < 999 || got > 1001 {
		t.Fatalf("125000 bytes in 1 s is 1000 kbps, got %v", got)
	}
	if got := sp.Kbps(125_000, t0.Add(2*time.Second)); got != 0 {
		t.Fatalf("no new bytes means 0 kbps, got %v", got)
	}
}

func TestNumHandlesTheTypesTheEstimatorReturns(t *testing.T) {
	if num(5) != 5 || num(int64(6)) != 6 || num(7.5) != 7.5 || num("x") != 0 || num(nil) != 0 {
		t.Fatal("num must convert int, int64 and float64 and ignore anything else")
	}
}

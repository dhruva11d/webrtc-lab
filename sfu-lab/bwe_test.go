package main

import (
	"testing"
	"time"
)

func TestSamplerReportsRateBetweenCalls(t *testing.T) {
	var sp sampler
	t0 := time.Now()
	if got := sp.kbps(0, t0); got != 0 {
		t.Fatalf("first call has no interval, got %v", got)
	}
	got := sp.kbps(125_000, t0.Add(time.Second))
	if got < 999 || got > 1001 {
		t.Fatalf("125000 bytes in 1 s is 1000 kbps, got %v", got)
	}
	if got := sp.kbps(125_000, t0.Add(2*time.Second)); got != 0 {
		t.Fatalf("no new bytes means 0 kbps, got %v", got)
	}
}

func TestNumHandlesTheTypesTheEstimatorReturns(t *testing.T) {
	if num(5) != 5 || num(int64(6)) != 6 || num(7.5) != 7.5 || num("x") != 0 || num(nil) != 0 {
		t.Fatal("num must convert int, int64 and float64 and ignore anything else")
	}
}

func TestSeriesIsBounded(t *testing.T) {
	var p []seriesPoint
	for i := 0; i < seriesMax+10; i++ {
		p = trimSeries(append(p, seriesPoint{T: float64(i)}))
	}
	if len(p) > seriesMax {
		t.Fatalf("series must stay bounded, got %d", len(p))
	}
	if p[len(p)-1].T != float64(seriesMax+9) {
		t.Fatal("newest point must be kept")
	}
}

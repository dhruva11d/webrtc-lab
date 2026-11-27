package adapt

import (
	"testing"
	"time"
)

func feedBWE(a *BWEAdapter, est float64, secs int, t0 time.Time, from int) (string, int) {
	for i := 0; i < secs; i++ {
		if l, ch := a.Observe(est, t0.Add(time.Duration(from+i)*time.Second)); ch {
			return l, from + i
		}
	}
	return "", -1
}

func TestFittingLayerUsesHeadroom(t *testing.T) {
	cases := map[float64]int{100: 0, 200: 0, 600: 1, 800: 1, 1800: 2, 5000: 2}
	for est, want := range cases {
		if got := fittingLayer(est); got != want {
			t.Errorf("estimate %.0f: want index %d, got %d", est, want, got)
		}
	}
}

func TestDropsToTheFittingLayerAfterTwoLowReadings(t *testing.T) {
	a := NewBWEAdapter("f")
	t0 := time.Now()
	if l, at := feedBWE(a, 300, 5, t0, 10); l != "q" || at != 11 {
		t.Fatalf("want q after two readings, got %q at %d", l, at)
	}
}

func TestStepsUpOneLayerAfterSustainedHeadroom(t *testing.T) {
	a := NewBWEAdapter("q")
	t0 := time.Now()
	if l, _ := feedBWE(a, 5000, 4, t0, 20); l != "" {
		t.Fatalf("four good readings are not enough, got %q", l)
	}
	if l, _ := feedBWE(a, 5000, 20, t0, 24); l != "h" {
		t.Fatalf("want one step up to h, got %q", l)
	}
}

func TestFailedProbeLengthensTheWait(t *testing.T) {
	a := NewBWEAdapter("h")
	t0 := time.Now()
	_, at := feedBWE(a, 5000, 30, t0, 20)
	if at < 0 {
		t.Fatal("expected a step up")
	}
	if _, at2 := feedBWE(a, 300, 10, t0, at+1); at2 < 0 {
		t.Fatal("expected a step down")
	}
	if a.upWait <= baseUpWait {
		t.Fatalf("a failed probe must lengthen the wait, got %v", a.upWait)
	}
}

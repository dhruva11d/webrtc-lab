package adapt

import (
	"testing"
	"time"
)

func feed(a *LossAdapter, loss float64, n int, now *time.Time) (last string, changes int) {
	for i := 0; i < n; i++ {
		*now = now.Add(time.Second)
		if l, ok := a.Observe(loss, *now); ok {
			last = l
			changes++
		}
	}
	return
}

func TestSingleBadReportDoesNotDowngrade(t *testing.T) {
	a, now := NewLossAdapter("f"), time.Now()
	if _, changes := feed(a, 20, 1, &now); changes != 0 {
		t.Fatal("one bad report must not change the layer")
	}
}

func TestSustainedLossStepsDownOneLayerAtATimeAndStopsAtLowest(t *testing.T) {
	a, now := NewLossAdapter("f"), time.Now()
	var got []string
	for i := 0; i < 20; i++ {
		now = now.Add(time.Second)
		if l, ok := a.Observe(30, now); ok {
			got = append(got, l)
		}
	}
	if len(got) != 2 || got[0] != "h" || got[1] != "q" {
		t.Fatalf("want [h q], got %v", got)
	}
}

func TestMiddleLossHolds(t *testing.T) {
	a, now := NewLossAdapter("h"), time.Now()
	if _, changes := feed(a, 3, 60, &now); changes != 0 {
		t.Fatal("loss between 1% and 5% must hold the layer")
	}
}

func TestStepsUpOnlyAfterEnoughCleanReportsAndWait(t *testing.T) {
	a, now := NewLossAdapter("q"), time.Now()
	a.lastChange = now
	if _, changes := feed(a, 0, 8, &now); changes != 0 {
		t.Fatal("must not step up before the wait has passed")
	}
	if l, changes := feed(a, 0, 5, &now); changes != 1 || l != "h" {
		t.Fatalf("expected one step up to h, got %q after %d changes", l, changes)
	}
}

func TestFailedProbeDoublesTheWait(t *testing.T) {
	a, now := NewLossAdapter("h"), time.Now()
	a.lastChange = now.Add(-time.Minute)
	feed(a, 0, 8, &now)
	if a.idx != 2 {
		t.Fatalf("setup: expected a step up to f, idx=%d", a.idx)
	}
	feed(a, 25, 3, &now)
	if a.idx != 1 {
		t.Fatalf("expected a step back down to h, idx=%d", a.idx)
	}
	if a.upWait != 2*baseUpWait {
		t.Fatalf("a failed probe must double the wait: got %v", a.upWait)
	}
}

func TestWaitIsCappedAndResetsAfterStability(t *testing.T) {
	a := NewLossAdapter("f")
	a.upWait = 50 * time.Second
	now := time.Now()
	a.lastUp = now
	a.Observe(30, now.Add(time.Second))
	a.Observe(30, now.Add(3*time.Second))
	if a.upWait != maxUpWait {
		t.Fatalf("wait must be capped at %v, got %v", maxUpWait, a.upWait)
	}
	a.Observe(0, now.Add(2*time.Minute))
	if a.upWait != baseUpWait {
		t.Fatalf("a long stable period must reset the wait, got %v", a.upWait)
	}
}

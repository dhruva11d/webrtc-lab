package simulcast

import "testing"

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

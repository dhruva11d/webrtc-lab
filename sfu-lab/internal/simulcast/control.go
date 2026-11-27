package simulcast

import (
	"log"
	"net/http"
	"strconv"

	"sfu-lab/internal/adapt"
)

func SelectHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "use POST", http.StatusMethodNotAllowed)
		return
	}
	layer := r.URL.Query().Get("layer")
	if layer != "auto" && !adapt.Valid(layer) {
		http.Error(w, "layer must be q, h, f or auto", http.StatusBadRequest)
		return
	}
	sub := findSub(r)
	if sub == nil {
		http.Error(w, "unknown subscriber", http.StatusNotFound)
		return
	}
	if layer == "auto" {
		sub.setAuto(true, r.URL.Query().Get("policy") == "bwe")
		log.Printf("[sub %s] auto mode on (policy %s), starting from %q", sub.id, sub.policyName(), sub.sel.targetLayer())
	} else {
		sub.setAuto(false, false)
		log.Printf("[sub %s] manual: asked for layer %q (currently %q)", sub.id, layer, sub.sel.currentLayer())
		sub.sel.setTarget(layer)
	}
	w.WriteHeader(http.StatusNoContent)
}

func ImpairHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "use POST", http.StatusMethodNotAllowed)
		return
	}
	sub := findSub(r)
	if sub == nil {
		http.Error(w, "unknown subscriber", http.StatusNotFound)
		return
	}
	loss, err1 := strconv.ParseFloat(r.URL.Query().Get("loss"), 64)
	capKbps, err2 := strconv.ParseFloat(r.URL.Query().Get("cap"), 64)
	if (r.URL.Query().Get("loss") != "" && err1 != nil) || (r.URL.Query().Get("cap") != "" && err2 != nil) || loss < 0 || loss > 100 || capKbps < 0 {
		http.Error(w, "loss must be 0-100 and cap >= 0", http.StatusBadRequest)
		return
	}
	sub.shaper.Set(loss, capKbps)
	log.Printf("[sub %s] impairment: loss=%.0f%% cap=%.0f kbps", sub.id, loss, capKbps)
	w.WriteHeader(http.StatusNoContent)
}

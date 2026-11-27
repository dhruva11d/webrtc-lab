package simulcast

import (
	"sync"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

type simLayer struct {
	pc   *webrtc.PeerConnection
	ssrc uint32
}

func (l *simLayer) requestKeyframe() {
	l.pc.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: l.ssrc}})
}

type simRegistry struct {
	mu     sync.RWMutex
	layers map[string]*simLayer
	subs   map[string]*simSub
}

var sim = &simRegistry{layers: map[string]*simLayer{}, subs: map[string]*simSub{}}

func (r *simRegistry) newPublisher() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.layers = map[string]*simLayer{}
	for _, s := range r.subs {
		s.sel.reset()
	}
}

func (r *simRegistry) setLayer(rid string, l *simLayer) {
	r.mu.Lock()
	r.layers[rid] = l
	r.mu.Unlock()
}

func (r *simRegistry) askKeyframe(rid string) {
	r.mu.RLock()
	l := r.layers[rid]
	r.mu.RUnlock()
	if l != nil {
		l.requestKeyframe()
	}
}

func (r *simRegistry) dispatch(rid string, pkt *rtp.Packet) {
	r.mu.RLock()
	subs := make([]*simSub, 0, len(r.subs))
	for _, s := range r.subs {
		subs = append(subs, s)
	}
	r.mu.RUnlock()
	for _, s := range subs {
		s.sel.handle(rid, pkt)
	}
}

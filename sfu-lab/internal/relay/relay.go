package relay

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"

	"sfu-lab/internal/rtprewrite"
)

type publisher struct {
	pc   *webrtc.PeerConnection
	ssrc atomic.Uint32
}

type stream struct {
	mu    sync.Mutex
	track *webrtc.TrackLocalStaticRTP
	rw    rtprewrite.Rewriter
}

var shared stream

func (s *stream) get(codec webrtc.RTPCodecCapability) (*webrtc.TrackLocalStaticRTP, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.track == nil {
		t, err := webrtc.NewTrackLocalStaticRTP(codec, "video", "sfu")
		if err != nil {
			return nil, err
		}
		s.track = t
	} else if s.track.Codec().MimeType != codec.MimeType {
		return nil, fmt.Errorf("stream is %s, new publisher sent %s", s.track.Codec().MimeType, codec.MimeType)
	}
	s.rw.BeginSource()
	return s.track, nil
}

func (s *stream) current() *webrtc.TrackLocalStaticRTP {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.track
}

func (s *stream) write(p *rtp.Packet) error {
	s.mu.Lock()
	s.rw.Rewrite(p, time.Now())
	t := s.track
	s.mu.Unlock()
	return t.WriteRTP(p)
}

func requestKeyframe() {
	if p := getPublisher(); p != nil {
		p.requestKeyframe()
	}
}

func (p *publisher) requestKeyframe() {
	if ssrc := p.ssrc.Load(); ssrc != 0 {
		p.pc.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: ssrc}})
	}
}

var (
	pubMu   sync.Mutex
	current *publisher
	subs    atomic.Int32
)

func getPublisher() *publisher {
	pubMu.Lock()
	defer pubMu.Unlock()
	return current
}

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

type publisher struct {
	pc   *webrtc.PeerConnection
	ssrc atomic.Uint32
}

type stream struct {
	mu    sync.Mutex
	track *webrtc.TrackLocalStaticRTP
	rw    rewriter
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
	s.rw.beginSource()
	return s.track, nil
}

func (s *stream) current() *webrtc.TrackLocalStaticRTP {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.track
}

func (s *stream) write(p *rtp.Packet) error {
	s.mu.Lock()
	s.rw.rewrite(p, time.Now())
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

func negotiate(pc *webrtc.PeerConnection, offer webrtc.SessionDescription) (*webrtc.SessionDescription, error) {
	if err := pc.SetRemoteDescription(offer); err != nil {
		return nil, err
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return nil, err
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		return nil, err
	}
	<-gathered
	return pc.LocalDescription(), nil
}

func decodeOffer(w http.ResponseWriter, r *http.Request) (webrtc.SessionDescription, bool) {
	var offer webrtc.SessionDescription
	if err := json.NewDecoder(r.Body).Decode(&offer); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return offer, false
	}
	return offer, true
}

func publishHandler(api *webrtc.API) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		offer, ok := decodeOffer(w, r)
		if !ok {
			return
		}
		pc, err := api.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		pub := &publisher{pc: pc}

		pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
			log.Println("publisher connection:", s)
			if s == webrtc.PeerConnectionStateFailed || s == webrtc.PeerConnectionStateClosed {
				pubMu.Lock()
				if current == pub {
					current = nil
				}
				pubMu.Unlock()
				pc.Close()
			}
		})

		pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
			if remote.Kind() != webrtc.RTPCodecTypeVideo {
				return
			}
			if _, err := shared.get(remote.Codec().RTPCodecCapability); err != nil {
				log.Println("rejecting publisher:", err)
				pc.Close()
				return
			}
			pub.ssrc.Store(uint32(remote.SSRC()))
			pubMu.Lock()
			current = pub
			pubMu.Unlock()
			log.Printf("publisher live: codec=%s ssrc=%d", remote.Codec().MimeType, remote.SSRC())
			pub.requestKeyframe()

			var n, bytes uint64
			start := time.Now()
			for {
				pkt, _, err := remote.ReadRTP()
				if err != nil {
					log.Println("publisher track ended:", err)
					return
				}
				n++
				bytes += uint64(len(pkt.Payload))
				if err := shared.write(pkt); err != nil {
					log.Println("fan-out write:", err)
					return
				}
				if time.Since(start) >= 2*time.Second {
					s := time.Since(start).Seconds()
					log.Printf("publisher in: %.0f pkt/s %.0f kbps | subscribers=%d", float64(n)/s, float64(bytes)*8/s/1000, subs.Load())
					n, bytes, start = 0, 0, time.Now()
				}
			}
		})

		answer, err := negotiate(pc, offer)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(answer)
	}
}

func subscribeHandler(api *webrtc.API) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		track := shared.current()
		if track == nil {
			http.Error(w, "no publisher has ever connected", http.StatusConflict)
			return
		}
		offer, ok := decodeOffer(w, r)
		if !ok {
			return
		}
		pc, err := api.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		sender, err := pc.AddTrack(track)
		if err != nil {
			pc.Close()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var counted atomic.Bool
		pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
			log.Println("subscriber connection:", s)
			switch s {
			case webrtc.PeerConnectionStateConnected:
				if counted.CompareAndSwap(false, true) {
					subs.Add(1)
				}
				requestKeyframe()
			case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed, webrtc.PeerConnectionStateDisconnected:
				if counted.CompareAndSwap(true, false) {
					subs.Add(-1)
				}
				if s != webrtc.PeerConnectionStateDisconnected {
					pc.Close()
				}
			}
		})

		go func() {
			for {
				pkts, _, err := sender.ReadRTCP()
				if err != nil {
					return
				}
				for _, p := range pkts {
					switch p.(type) {
					case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
						log.Println("subscriber PLI/FIR -> forwarding to publisher")
						requestKeyframe()
					}
				}
			}
		}()

		answer, err := negotiate(pc, offer)
		if err != nil {
			pc.Close()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(answer)
	}
}

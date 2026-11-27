package simulcast

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"

	"sfu-lab/internal/sdp"
	"sfu-lab/internal/vp8"
)

func PublishHandler(api *webrtc.API) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		offer, ok := sdp.DecodeOffer(w, r)
		if !ok {
			return
		}
		pc, err := api.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
			log.Println("simulcast publisher connection:", s)
			if s == webrtc.PeerConnectionStateFailed || s == webrtc.PeerConnectionStateClosed {
				pc.Close()
			}
		})

		sim.newPublisher()

		pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
			if remote.Kind() != webrtc.RTPCodecTypeVideo {
				return
			}
			rid := remote.RID()
			if rid == "" {
				log.Println("track without RID: the browser did not send simulcast")
			}
			log.Printf("layer started: rid=%q ssrc=%d codec=%s", rid, remote.SSRC(), remote.Codec().MimeType)
			sim.setLayer(rid, &simLayer{pc: pc, ssrc: uint32(remote.SSRC())})
			pc.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: uint32(remote.SSRC())}})

			var n, keyframes, lost uint64
			var bytes uint64
			var last uint16
			first := true
			start := time.Now()
			for {
				pkt, _, err := remote.ReadRTP()
				if err != nil {
					log.Printf("layer %q ended: %v", rid, err)
					return
				}
				n++
				bytes += uint64(len(pkt.Payload))
				if vp8.IsKeyframe(pkt.Payload) {
					keyframes++
				}
				if !first {
					if gap := pkt.SequenceNumber - last; gap > 1 && gap < 0x8000 {
						lost += uint64(gap - 1)
					} else if gap == 0 || gap >= 0x8000 {
						continue
					}
				}
				first = false
				last = pkt.SequenceNumber
				sim.dispatch(rid, pkt)

				if time.Since(start) >= 2*time.Second {
					s := time.Since(start).Seconds()
					log.Printf("layer %-2q: %4.0f pkt/s %5.0f kbps | keyframes=%d lost=%d", rid, float64(n)/s, float64(bytes)*8/s/1000, keyframes, lost)
					n, bytes, start = 0, 0, time.Now()
				}
			}
		})

		answer, err := sdp.Negotiate(pc, offer)
		if err != nil {
			pc.Close()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(answer)
	}
}

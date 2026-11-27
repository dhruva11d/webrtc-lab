package room

import (
	"crypto/rand"
	"encoding/hex"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
)

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

type signal struct {
	Type string `json:"type"`
	ID   string `json:"id,omitempty"`
	SDP  string `json:"sdp,omitempty"`
}

type roomPeer struct {
	id   string
	ws   *websocket.Conn
	wsMu sync.Mutex
	pc   *webrtc.PeerConnection

	negMu    sync.Mutex
	inFlight bool
	pending  bool

	sMu     sync.Mutex
	senders map[string]*webrtc.RTPSender

	track atomic.Pointer[webrtc.TrackLocalStaticRTP]
	ssrc  atomic.Uint32
}

type room struct {
	mu    sync.Mutex
	peers map[string]*roomPeer
}

var theRoom = &room{peers: map[string]*roomPeer{}}

func (r *room) others(except string) []*roomPeer {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*roomPeer
	for id, p := range r.peers {
		if id != except {
			out = append(out, p)
		}
	}
	return out
}

func (p *roomPeer) send(s signal) {
	p.wsMu.Lock()
	defer p.wsMu.Unlock()
	if err := p.ws.WriteJSON(s); err != nil {
		log.Printf("[%s] ws write: %v", p.id, err)
	}
}

func (p *roomPeer) requestKeyframe() {
	if ssrc := p.ssrc.Load(); ssrc != 0 {
		p.pc.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: ssrc}})
	}
}

func (p *roomPeer) addPublisher(src *roomPeer) {
	track := src.track.Load()
	if track == nil || p.pc.ConnectionState() == webrtc.PeerConnectionStateClosed {
		return
	}
	p.sMu.Lock()
	if _, dup := p.senders[src.id]; dup {
		p.sMu.Unlock()
		return
	}
	sender, err := p.pc.AddTrack(track)
	if err != nil {
		p.sMu.Unlock()
		log.Printf("[%s] add track of %s: %v", p.id, src.id, err)
		return
	}
	p.senders[src.id] = sender
	p.sMu.Unlock()

	go func() {
		for {
			pkts, _, err := sender.ReadRTCP()
			if err != nil {
				return
			}
			for _, pk := range pkts {
				switch pk.(type) {
				case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
					src.requestKeyframe()
				}
			}
		}
	}()
	log.Printf("[%s] now receives video of %s", p.id, src.id)
}

func (p *roomPeer) removePublisher(srcID string) {
	p.sMu.Lock()
	sender, ok := p.senders[srcID]
	delete(p.senders, srcID)
	p.sMu.Unlock()
	if ok {
		p.pc.RemoveTrack(sender)
		log.Printf("[%s] stopped receiving video of %s", p.id, srcID)
	}
}

// Only the server offers; the client only answers. This avoids glare. Only one
// offer may be in flight; changes that arrive meanwhile are folded into one more round.
func (p *roomPeer) negotiate() {
	if p.pc.ConnectionState() == webrtc.PeerConnectionStateClosed {
		return
	}
	p.negMu.Lock()
	if p.inFlight {
		p.pending = true
		p.negMu.Unlock()
		return
	}
	p.inFlight = true
	p.negMu.Unlock()

	fail := func(err error) {
		log.Printf("[%s] negotiate: %v", p.id, err)
		p.negMu.Lock()
		p.inFlight = false
		p.negMu.Unlock()
	}
	offer, err := p.pc.CreateOffer(nil)
	if err != nil {
		fail(err)
		return
	}
	gathered := webrtc.GatheringCompletePromise(p.pc)
	if err := p.pc.SetLocalDescription(offer); err != nil {
		fail(err)
		return
	}
	<-gathered
	p.send(signal{Type: "offer", SDP: p.pc.LocalDescription().SDP})
}

func (p *roomPeer) onAnswer(sdp string) {
	if err := p.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: sdp}); err != nil {
		log.Printf("[%s] answer: %v", p.id, err)
	}
	p.negMu.Lock()
	p.inFlight = false
	again := p.pending
	p.pending = false
	p.negMu.Unlock()
	if again {
		go p.negotiate()
	}
}

func (p *roomPeer) onPublish(remote *webrtc.TrackRemote) {
	track, err := webrtc.NewTrackLocalStaticRTP(remote.Codec().RTPCodecCapability, "video", p.id)
	if err != nil {
		log.Printf("[%s] track: %v", p.id, err)
		return
	}
	p.track.Store(track)
	p.ssrc.Store(uint32(remote.SSRC()))
	log.Printf("[%s] publishing %s ssrc=%d", p.id, remote.Codec().MimeType, remote.SSRC())

	for _, o := range theRoom.others(p.id) {
		o.addPublisher(p)
		go o.negotiate()
	}
	for _, d := range []time.Duration{time.Second, 3 * time.Second} {
		time.AfterFunc(d, p.requestKeyframe)
	}
	p.requestKeyframe()

	for {
		pkt, _, err := remote.ReadRTP()
		if err != nil {
			return
		}
		if err := track.WriteRTP(pkt); err != nil {
			return
		}
	}
}

func (r *room) remove(p *roomPeer) {
	r.mu.Lock()
	delete(r.peers, p.id)
	r.mu.Unlock()
	p.pc.Close()
	for _, o := range r.others(p.id) {
		o.removePublisher(p.id)
		o.send(signal{Type: "peer-left", ID: p.id})
		go o.negotiate()
	}
	log.Printf("[%s] left; peers now %d", p.id, len(r.others("")))
}

func WSHandler(api *webrtc.API) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()

		buf := make([]byte, 4)
		rand.Read(buf)
		p := &roomPeer{id: hex.EncodeToString(buf), ws: ws, senders: map[string]*webrtc.RTPSender{}}

		pc, err := api.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			log.Println("pc:", err)
			return
		}
		p.pc = pc
		if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo,
			webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
			log.Println("transceiver:", err)
			return
		}
		pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
			if remote.Kind() == webrtc.RTPCodecTypeVideo {
				p.onPublish(remote)
			}
		})
		pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
			log.Printf("[%s] connection: %s", p.id, s)
			if s == webrtc.PeerConnectionStateFailed || s == webrtc.PeerConnectionStateClosed {
				ws.Close()
			}
		})

		theRoom.mu.Lock()
		theRoom.peers[p.id] = p
		theRoom.mu.Unlock()
		defer theRoom.remove(p)

		p.send(signal{Type: "welcome", ID: p.id})
		for _, o := range theRoom.others(p.id) {
			p.addPublisher(o)
			time.AfterFunc(2*time.Second, o.requestKeyframe)
		}
		go p.negotiate()

		for {
			var m signal
			if err := ws.ReadJSON(&m); err != nil {
				return
			}
			if m.Type == "answer" {
				p.onAnswer(m.SDP)
			}
		}
	}
}

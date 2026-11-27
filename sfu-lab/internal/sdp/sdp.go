package sdp

import (
	"encoding/json"
	"net/http"

	"github.com/pion/webrtc/v4"
)

func Negotiate(pc *webrtc.PeerConnection, offer webrtc.SessionDescription) (*webrtc.SessionDescription, error) {
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

func DecodeOffer(w http.ResponseWriter, r *http.Request) (webrtc.SessionDescription, bool) {
	var offer webrtc.SessionDescription
	if err := json.NewDecoder(r.Body).Decode(&offer); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return offer, false
	}
	return offer, true
}

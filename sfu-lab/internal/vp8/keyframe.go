package vp8

import "github.com/pion/rtp/codecs"

func IsKeyframe(payload []byte) bool {
	var p codecs.VP8Packet
	frame, err := p.Unmarshal(payload)
	if err != nil || p.S != 1 || p.PID != 0 || len(frame) == 0 {
		return false
	}
	return frame[0]&0x01 == 0
}

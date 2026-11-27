package main

import "github.com/pion/rtp/codecs"

func isVP8Keyframe(payload []byte) bool {
	var p codecs.VP8Packet
	frame, err := p.Unmarshal(payload)
	if err != nil || p.S != 1 || p.PID != 0 || len(frame) == 0 {
		return false
	}
	return frame[0]&0x01 == 0
}

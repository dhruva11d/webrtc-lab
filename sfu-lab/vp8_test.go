package main

import "testing"

func TestVP8Keyframe(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		want    bool
	}{
		{"keyframe start", []byte{0x10, 0x10, 0x00, 0x00}, true},
		{"delta frame start", []byte{0x10, 0x11, 0x00, 0x00}, false},
		{"continuation packet of a keyframe", []byte{0x00, 0x10, 0x00, 0x00}, false},
		{"second partition", []byte{0x11, 0x10, 0x00, 0x00}, false},
		{"empty", nil, false},
		{"descriptor only", []byte{0x10}, false},
	}
	for _, c := range cases {
		if got := isVP8Keyframe(c.payload); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

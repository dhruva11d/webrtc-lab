package main

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func dial(t *testing.T, srvURL string) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(srvURL, "http") + "/ws"
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func read(t *testing.T, c *websocket.Conn) Message {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	var m Message
	if err := c.ReadJSON(&m); err != nil {
		t.Fatalf("read: %v", err)
	}
	return m
}

func TestRoomForwarding(t *testing.T) {
	srv := httptest.NewServer(newMux(NewHub()))
	defer srv.Close()

	a, b, other := dial(t, srv.URL), dial(t, srv.URL), dial(t, srv.URL)
	defer a.Close()
	defer b.Close()
	defer other.Close()

	a.WriteJSON(Message{Type: "join", Room: "r1"})
	if m := read(t, a); m.Type != "joined" {
		t.Fatalf("a: want joined, got %+v", m)
	}

	b.WriteJSON(Message{Type: "join", Room: "r1"})
	if m := read(t, b); m.Type != "joined" {
		t.Fatalf("b: want joined, got %+v", m)
	}
	if m := read(t, a); m.Type != "peer-joined" {
		t.Fatalf("a: want peer-joined, got %+v", m)
	}

	other.WriteJSON(Message{Type: "join", Room: "r2"})
	read(t, other)

	a.WriteJSON(Message{Type: "chat", Data: []byte(`"hi"`)})
	if m := read(t, b); m.Type != "chat" || string(m.Data) != `"hi"` {
		t.Fatalf("b: want chat hi, got %+v", m)
	}

	// Isolation check using ORDER, not timeouts (a timed-out read breaks a
	// gorilla connection). If r1's chat had leaked to `other`, it would arrive
	// before this peer-joined. Same idea for `a` and its own echo below.
	c3 := dial(t, srv.URL)
	defer c3.Close()
	c3.WriteJSON(Message{Type: "join", Room: "r2"})
	read(t, c3)
	if m := read(t, other); m.Type != "peer-joined" {
		t.Fatalf("other: want peer-joined first, got %+v (r1 traffic leaked?)", m)
	}

	b.Close()
	if m := read(t, a); m.Type != "peer-left" {
		t.Fatalf("a: want peer-left, got %+v (self-echo?)", m)
	}
}

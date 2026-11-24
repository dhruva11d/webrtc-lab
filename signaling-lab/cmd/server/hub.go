package main

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/gorilla/websocket"
)

type Message struct {
	Type string          `json:"type"`
	Room string          `json:"room,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

type Client struct {
	conn *websocket.Conn
	mu   sync.Mutex // gorilla allows only ONE writer at a time per connection
	room string     // guarded by Hub.mu
}

func (c *Client) send(m Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteJSON(m)
}

type Hub struct {
	mu    sync.Mutex
	rooms map[string]map[*Client]bool
}

func NewHub() *Hub {
	return &Hub{rooms: make(map[string]map[*Client]bool)}
}

// othersLocked returns the other clients in c's room. Caller must hold h.mu.
func (h *Hub) othersLocked(c *Client) []*Client {
	var out []*Client
	for peer := range h.rooms[c.room] {
		if peer != c {
			out = append(out, peer)
		}
	}
	return out
}

func (h *Hub) removeLocked(c *Client) {
	if c.room == "" {
		return
	}
	delete(h.rooms[c.room], c)
	if len(h.rooms[c.room]) == 0 {
		delete(h.rooms, c.room)
	}
	c.room = ""
}

func (h *Hub) Join(c *Client, room string) {
	h.mu.Lock()
	h.removeLocked(c)
	if h.rooms[room] == nil {
		h.rooms[room] = make(map[*Client]bool)
	}
	h.rooms[room][c] = true
	c.room = room
	others := h.othersLocked(c)
	count := len(h.rooms[room])
	h.mu.Unlock()

	// Send outside the lock so a slow client cannot block the whole hub.
	c.send(Message{Type: "joined", Room: room, Data: json.RawMessage(fmt.Sprintf(`{"peers":%d}`, count))})
	for _, o := range others {
		o.send(Message{Type: "peer-joined", Room: room})
	}
}

func (h *Hub) Leave(c *Client) {
	h.mu.Lock()
	room := c.room
	others := h.othersLocked(c)
	h.removeLocked(c)
	h.mu.Unlock()

	for _, o := range others {
		o.send(Message{Type: "peer-left", Room: room})
	}
}

func (h *Hub) Forward(c *Client, m Message) {
	h.mu.Lock()
	room := c.room
	others := h.othersLocked(c)
	h.mu.Unlock()

	if room == "" {
		c.send(Message{Type: "error", Data: json.RawMessage(`"join a room first"`)})
		return
	}
	m.Room = room
	for _, o := range others {
		o.send(m)
	}
}

package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	// Allow any origin for local learning. A real server must restrict this.
	CheckOrigin: func(r *http.Request) bool { return true },
}

func wsHandler(hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("upgrade failed:", err)
			return
		}
		defer conn.Close()

		c := &Client{conn: conn}
		log.Println("client connected:", r.RemoteAddr)
		defer func() {
			hub.Leave(c)
			log.Println("client disconnected:", r.RemoteAddr)
		}()

		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}

			var m Message
			if err := json.Unmarshal(raw, &m); err != nil {
				c.send(Message{Type: "error", Data: json.RawMessage(`"invalid JSON"`)})
				continue
			}
			log.Printf("[%s] type=%s room=%q", r.RemoteAddr, m.Type, m.Room)

			switch m.Type {
			case "join":
				if m.Room == "" {
					c.send(Message{Type: "error", Data: json.RawMessage(`"room name required"`)})
					continue
				}
				hub.Join(c, m.Room)
			default:
				hub.Forward(c, m)
			}
		}
	}
}

func newMux(hub *Hub) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", wsHandler(hub))
	mux.Handle("/", http.FileServer(http.Dir("web")))
	return mux
}

func main() {
	addr := flag.String("addr", ":8080", "listen address (:8080 = all network interfaces)")
	cert := flag.String("cert", "", "TLS certificate file (enables HTTPS + WSS)")
	key := flag.String("key", "", "TLS private key file")
	flag.Parse()

	mux := newMux(NewHub())
	if *cert != "" && *key != "" {
		log.Println("signaling server on https://" + *addr + " (TLS)")
		log.Fatal(http.ListenAndServeTLS(*addr, *cert, *key, mux))
	}
	log.Println("signaling server on http://" + *addr + " (plain HTTP: camera works only on localhost)")
	log.Fatal(http.ListenAndServe(*addr, mux))
}

package main

import (
	"fmt"
	"log"
	"net"
	"strings"
	"time"
)

// Wire format: SESSION_ID|SEQUENCE_ID|PAYLOAD
// A sequence number alone is only meaningful inside one sender's session,
// so duplicates are detected by (session ID, sequence ID).

const sessionTTL = 30 * time.Second

type session struct {
	processed       map[string]bool
	lastSeen        time.Time
	droppedFirstACK bool
}

func main() {
	serverAddress := &net.UDPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 9000,
	}

	conn, err := net.ListenUDP("udp", serverAddress)
	if err != nil {
		log.Fatal("Failed to start the UDP server: ", err)
	}
	defer conn.Close()
	fmt.Println("UDP server listening on", serverAddress)

	sessions := make(map[string]*session)
	lastCleanup := time.Now()

	buffer := make([]byte, 1024)

	for {
		n, clientAddress, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Println("Error receiving packet:", err)
			continue
		}

		if time.Since(lastCleanup) > time.Second {
			for id, s := range sessions {
				if time.Since(s.lastSeen) > sessionTTL {
					fmt.Println("Session expired:", id)
					delete(sessions, id)
				}
			}
			lastCleanup = time.Now()
		}

		parts := strings.SplitN(string(buffer[:n]), "|", 3)
		if len(parts) != 3 || parts[0] == "" || parts[1] == "" {
			log.Println("Invalid message:", string(buffer[:n]))
			continue
		}
		sessionID, sequenceID, payload := parts[0], parts[1], parts[2]

		s, ok := sessions[sessionID]
		if !ok {
			s = &session{processed: make(map[string]bool)}
			sessions[sessionID] = s
			fmt.Println("New session:", sessionID)
		}
		s.lastSeen = time.Now()

		fmt.Printf("Received: [%s] %s | Payload: %s\n", sessionID, sequenceID, payload)

		if s.processed[sequenceID] {
			fmt.Println("Duplicate request detected:", sequenceID, "- skipping processing, resending ACK")
		} else {
			s.processed[sequenceID] = true
			fmt.Println("Processing request:", sequenceID)
		}

		if sequenceID == "MSG-005" && !s.droppedFirstACK {
			s.droppedFirstACK = true
			fmt.Println("SIMULATION: First ACK dropped for", sequenceID)
			continue
		}

		ack := []byte("ACK|" + sessionID + "|" + sequenceID)
		if _, err = conn.WriteToUDP(ack, clientAddress); err != nil {
			log.Println("Failed to send ACK:", err)
			continue
		}
		fmt.Println("ACK sent:", sequenceID)
	}
}

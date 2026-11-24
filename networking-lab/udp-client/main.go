package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"
)

func main() {
	serverAddress := &net.UDPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 9000,
	}

	conn, err := net.DialUDP("udp", nil, serverAddress)
	if err != nil {
		log.Fatal("Failed to create UDP client:", err)
	}
	defer conn.Close()

	fmt.Println("Connected to UDP server:", serverAddress)

	sessionID := fmt.Sprintf("%08x", rand.Uint32())
	fmt.Println("Session ID:", sessionID)

	buffer := make([]byte, 1024)

	const totalMessages = 10
	const timeout = 1 * time.Second
	const maxRetries = 3

	successful := 0
	failed := 0
	totalTransmissions := 0

	for i := 1; i <= totalMessages; i++ {

		sequenceID := fmt.Sprintf("MSG-%03d", i)
		message := sessionID + "|" + sequenceID + "|Hello from client"

		acknowledged := false

		fmt.Printf("\nSending request: %s\n", sequenceID)

		for attempt := 0; attempt <= maxRetries; attempt++ {

			if attempt > 0 {
				fmt.Printf("RETRY %d: %s\n", attempt, sequenceID)
			}

			err := conn.SetReadDeadline(time.Now().Add(timeout))
			if err != nil {
				log.Fatal(err)
			}

			_, err = conn.Write([]byte(message))
			if err != nil {
				log.Println("Write failed:", err)
				continue
			}

			totalTransmissions++

			for {
				n, err := conn.Read(buffer)

				if err != nil {
					if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
						fmt.Println("TIMEOUT:", sequenceID)
						break
					}

					log.Println("Read error:", err)
					break
				}

				response := string(buffer[:n])
				expectedACK := "ACK|" + sessionID + "|" + sequenceID

				if response != expectedACK {
					fmt.Println("Unexpected response:", response)
					continue
				}

				fmt.Println("ACK received:", response)

				acknowledged = true
				break
			}

			if acknowledged {
				successful++
				break
			}
		}

		if !acknowledged {
			failed++
			fmt.Println("FAILED:", sequenceID)
		}
	}

	fmt.Println("\n========== RELIABLE UDP RESULTS ==========")
	fmt.Println("Requests:", totalMessages)
	fmt.Println("Successfully acknowledged:", successful)
	fmt.Println("Failed:", failed)
	fmt.Println("Total transmissions:", totalTransmissions)
	fmt.Println("Retransmissions:", totalTransmissions-totalMessages)
}

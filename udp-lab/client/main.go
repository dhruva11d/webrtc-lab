package main

import (
	"fmt"
	"net"
	"time"
)

func main() {
	serverAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:9000")
	if err != nil {
		panic(err)
	}

	// DialUDP does NOT contact the server. It only sets the default
	// destination and picks a local port. No handshake happens.
	conn, err := net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	fmt.Println("client socket:", conn.LocalAddr(), "->", conn.RemoteAddr())

	start := time.Now()
	if _, err := conn.Write([]byte("hello udp")); err != nil {
		panic(err)
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 2048)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println("no reply:", err)
		return
	}
	fmt.Printf("reply: %q (RTT %v)\n", string(buf[:n]), time.Since(start))
}

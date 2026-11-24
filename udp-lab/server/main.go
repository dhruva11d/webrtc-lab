package main

import (
	"fmt"
	"net"
)

func main() {
	addr, err := net.ResolveUDPAddr("udp", ":9000")
	if err != nil {
		panic(err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	fmt.Println("UDP server listening on", conn.LocalAddr())

	buf := make([]byte, 2048)
	for {
		n, from, err := conn.ReadFromUDP(buf)
		if err != nil {
			fmt.Println("read error:", err)
			continue
		}
		msg := string(buf[:n])
		fmt.Printf("got %d bytes from %s: %q\n", n, from, msg)

		if _, err := conn.WriteToUDP([]byte("echo: "+msg), from); err != nil {
			fmt.Println("write error:", err)
		}
	}
}

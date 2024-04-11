package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	// sem := make(chan struct{}, 100)
	for {
		con, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go func(con net.Conn) {
			handleClient(con)
		}(con)
	}
}
func handleClient(con net.Conn) {
	defer con.Close()
	buf := make([]byte, 1024)
	for {
		i, err := con.Read(buf)
		if err != nil {
			os.Exit(1)
		}
		response, err := parse(buf[:i])
		if err != nil {
			fmt.Println("Error parsing input: ", err.Error())
			os.Exit(1)
		}
		_, err = con.Write([]byte(response))
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			os.Exit(1)
		}
	}
}

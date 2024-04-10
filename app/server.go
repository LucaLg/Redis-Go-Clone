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

	for {
		con, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		handleClient(con)
	}
}
func handleClient(con net.Conn) {
	defer con.Close()
	ping := []byte("PING\r\n")
	var i, err = con.Read(ping)
	for i > 0 {
		if i > 0 {
			fmt.Println("Received: ", string(ping))
			con.Write([]byte("+PONG\r\n"))
		}
		i, err = con.Read(ping)
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			os.Exit(1)
		}
	}
}

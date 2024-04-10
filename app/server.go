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

	con, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	ping := []byte("PING\r\n")
	for j := 0; j < 3; j++ {
		i, err := con.Read(ping)
		if i > 0 {
			fmt.Println("Received: ", string(ping))
			con.Write([]byte("+PONG\r\n"))
		}
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			os.Exit(1)
		}
	}

}

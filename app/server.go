package main

import (
	"flag"
	"fmt"
	"net"
	"os"
)

var portFlag = flag.String("port", "6379", "Give a custom port to run the server ")

func main() {
	flag.Parse()
	add := fmt.Sprintf("0.0.0.0:%s", *portFlag)
	l, err := net.Listen("tcp", add)
	if err != nil {
		fmt.Printf("Failed to bind to port %s", portFlag)
		os.Exit(1)
	}
	sem := make(chan struct{}, 100)
	for {
		con, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		sem <- struct{}{}
		go func(con net.Conn) {
			handleClient(con)
			<-sem
		}(con)
	}
}
func handleClient(con net.Conn) {
	defer con.Close()
	buf := make([]byte, 1024)
	for {
		i, err := con.Read(buf)
		if err != nil {
			fmt.Println("Error parsing input: ", err.Error())
			return
		}
		response, err := parse(buf[:i])
		if err != nil {
			fmt.Println("Error parsing input: ", err.Error())
			return
		}
		_, err = con.Write([]byte(response))
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			return
		}
	}
}

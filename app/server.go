package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
)

type Replication struct {
	HOST_IP   string
	HOST_PORT string
}
type Server struct {
	host        string
	port        string
	status      string
	replication Replication
}

func (s *Server) handleReplication() {
	if len(flag.Args()) != 2 {
		fmt.Println("No Master IP or Port given ")
		return
	}
	s.replication = Replication{
		HOST_IP:   flag.Args()[0],
		HOST_PORT: flag.Args()[1],
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", s.replication.HOST_IP, s.replication.HOST_PORT))
	if err != nil {
		fmt.Printf("Replication coulndt connect to master on port %s", s.replication.HOST_PORT)
		return
	}
	_, err = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	if err != nil {
		log.Fatal(err)
	}
	response, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Response from server:", response)
	status = "slave"

}
func (s *Server) setup() (net.Listener, error) {
	portFlag := flag.String("port", "6379", "Give a custom port to run the server ")
	replicationFlag := flag.Bool("replicaof", false, "Specify if the server is a replica")
	flag.Parse()
	if *replicationFlag {
		s.handleReplication()
	} else {
		s.status = "master"
	}
	s.host = "0.0.0.0"
	s.port = *portFlag
	address := fmt.Sprintf("%s:%s", s.host, s.port)
	fmt.Println(address)
	return net.Listen("tcp", address)

}

var status = "master"

func main() {

	server := Server{}
	l, err := server.setup()
	if err != nil {
		fmt.Printf("Failed to bind to port %s", server.port)
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

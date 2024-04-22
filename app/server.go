package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type Replication struct {
	HOST_IP   string
	HOST_PORT string
}
type Value struct {
	value   string
	savedAt time.Time
	expire  time.Duration
}
type Server struct {
	addr        string
	status      string
	replication Replication

	Store  *Store
	Parser Parser
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
	err := s.handshake()
	if err != nil {
		fmt.Println("An error occured during the handshake")
		log.Fatalf(err.Error())
	}
	status = "slave"

}
func (s *Server) handshake() error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", s.replication.HOST_IP, s.replication.HOST_PORT))
	if err != nil {
		fmt.Printf("Replication couldnt connect to master on port %s", s.replication.HOST_PORT)
		return err
	}
	handshakeStages := []string{
		SliceToBulkString([]string{"PING"}),
		SliceToBulkString([]string{"REPLCONF", "listening-port", strings.Split(s.addr, ":")[1]}),
		SliceToBulkString([]string{"REPLCONF", "capa", "psync2"}),
		SliceToBulkString([]string{"PSYNC", "?", "-1"})}
	reader := bufio.NewReader(conn)
	for _, hsInput := range handshakeStages {
		_, err = conn.Write([]byte(hsInput))
		if err != nil {
			return err
		}
		respping, err := reader.ReadString('\n')
		fmt.Println(respping)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *Server) start() (net.Listener, error) {
	portFlag := flag.String("port", "6379", "Give a custom port to run the server ")
	replicationFlag := flag.Bool("replicaof", false, "Specify if the server is a replica")
	flag.Parse()
	s.addr = fmt.Sprintf("%s:%s", "localhost", *portFlag)
	if *replicationFlag {
		s.handleReplication()
	} else {
		s.status = "master"
	}
	return net.Listen("tcp", s.addr)

}

var status = "master"

func main() {

	store := &Store{
		Mutex: sync.Mutex{},
		Data:  make(map[string]Value),
	}
	server := Server{
		Store: store,
	}
	l, err := server.start()
	if err != nil {
		fmt.Printf("Failed to bind to port %s", strings.Split(server.addr, ":")[1])
		os.Exit(1)
	}
	fmt.Println("Server started on ", server.addr)
	sem := make(chan struct{}, 100)
	for {
		con, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		sem <- struct{}{}
		go func(con net.Conn) {
			server.handleClient(con)
			<-sem
		}(con)
	}
}
func (s *Server) handleClient(con net.Conn) {
	defer con.Close()
	buf := make([]byte, 2048)
	for {
		i, err := con.Read(buf)
		if err != nil {
			log.Printf("Error reading from connection: %v", err)
			continue
		}
		cmds, err := s.Parser.Parse(buf[:i], s)
		if err != nil {
			log.Printf("Error parsing: %v", err)
			continue
		}
		response, err := s.handleCmds(cmds)
		if err != nil {
			log.Printf("Error parsing input: %v", err)
			continue
		}
		_, err = con.Write([]byte(response))
		if err != nil {
			log.Printf("Error writing to connection: %v", err)
			continue
		}
	}
}

func (s *Server) handleCmds(cmdArr []string) (string, error) {
	switch strings.ToLower(cmdArr[0]) {
	case "echo":
		return fmt.Sprintf("+%s\r\n", cmdArr[1]), nil
	case "ping":
		return "+PONG\r\n", nil
	case "COMMAND":
		return "+PONG\r\n", nil
	case "set":
		s.Store.handleSet(cmdArr)
		return "+OK\r\n", nil
	case "get":
		result, err := s.Store.handleGet(cmdArr[1])
		if err != nil {
			return "", err
		}
		return result, nil
	case "info":
		return handleInfo(cmdArr), nil
	case "replconf":
		return "+OK\r\n", nil
	case "psync":
		id := StringToBulkString("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb")
		fmt.Println(id)
		return fmt.Sprintf("+FULLRESYNC %s 0\r\n", id), nil
	default:
		return "", fmt.Errorf("Unknown command: %s", cmdArr[0])
	}
}

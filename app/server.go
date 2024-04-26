package main

import (
	"flag"
	"fmt"
	"io"
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

	consMu   sync.Mutex
	repConns []*net.Conn
}

func (s *Server) handleReplication() {
	s.status = "slave"
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
	buf := make([]byte, 2048)
	for _, hsInput := range handshakeStages {
		i, err := conn.Write([]byte(hsInput))
		if err != nil {
			return err
		}
		response, err := conn.Read(buf[:i])
		// respping, err := reader.ReadString('\n')
		fmt.Println(response)
		if err != nil {
			return err
		}
	}
	//Keep up established connection
	go func(net.Conn, []byte) {
		s.handleClient(conn, buf)
	}(conn, buf)
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
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		fmt.Println("Connection added to ", conn.RemoteAddr().String())
		sem <- struct{}{}
		go func(con net.Conn) {
			buf := make([]byte, 2048)
			server.handleClient(con, buf)
			<-sem
		}(conn)
	}
}
func (s *Server) handleClient(conn net.Conn, buf []byte) {
	defer conn.Close()
	for {
		i, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				log.Printf("Connection closed by client: %v", conn.RemoteAddr())
				// Exit the loop on error or when the connection is closed
				break
			} else {
				log.Printf("Error reading from connection: %v", err)
			}
			continue
		}
		// fmt.Printf("%d bytes received from the connection %v", i, conn.RemoteAddr().String())
		if s.status == "master" {
			cmds, err := s.Parser.Parse(buf[:i], s)
			if err != nil {
				log.Printf("Error parsing: %v", err)
				continue
			}
			response, err := s.handleCmds(cmds, conn)
			if err != nil {
				log.Printf("Error parsing input: %v", err)
				continue
			}
			err = s.writeResponse(conn, response)
			if err != nil {
				log.Printf("Error writing a response", err)
				continue
			}
		} else {
			cmds, err := s.Parser.parseReplication(buf[:i], s)
			if err != nil {
				log.Printf("Error parsing: %v", err)
				continue
			}
			fmt.Println("Read ", cmds)
			for _, cmd := range cmds {
				response, err := s.handleCmds(cmd, conn)
				if err != nil {
					log.Printf("Error occured handleCmds in replication")
					continue
				}
				if conn.RemoteAddr().String() != fmt.Sprintf("%s:%s", s.replication.HOST_IP, s.replication.HOST_PORT) {
					err = s.writeResponse(conn, response)
					if err != nil {
						log.Printf("Error writing a response", err)
						continue
					}
				}
			}
		}

	}
}
func (s *Server) writeResponse(conn net.Conn, mess string) error {
	_, err := conn.Write([]byte(mess))
	// fmt.Printf("%d Bytes written to %v %s \n", i, conn, mess)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) handleCmds(cmdArr []string, conn net.Conn) (string, error) {
	switch strings.ToLower(cmdArr[0]) {
	case "echo":
		return s.echo(cmdArr, conn), nil
	case "ping":
		return "+PONG\r\n", nil
	case "command":
		return "+PONG\r\n", nil
	case "set":
		return s.set(cmdArr, conn)
	case "get":
		return s.get(cmdArr, conn)
	case "info":
		return s.info(cmdArr), nil
	case "replconf":
		return s.replconf(cmdArr)
	case "psync":
		return s.psync(cmdArr, conn)
	default:
		return "", fmt.Errorf("unknown command: %v", cmdArr[0])
	}
}
func (s *Server) handlePropagation(cmdArr []string) {
	for _, conn := range s.repConns {
		cmd := SliceToBulkString(cmdArr)
		log.Printf("Sending command to replication: %s", cmd)

		err := s.writeResponse(*conn, cmd)
		if err != nil {
			fmt.Errorf("An error occurred while sending propagations %v", err)
		}
	}

}

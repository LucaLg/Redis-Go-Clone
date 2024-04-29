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
	// connCh := make(chan net.Conn)
	// go func() {
	conn, lastRef, err := s.handshake()
	if err != nil {
		fmt.Println("An error occured during the handshake", err)
		// close(connCh)
		return
	}
	buf := make([]byte, 2048)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println("An error occured during reading  handshake", err)
	}
	if strings.Contains(string(buf[:n]), "GETACK") || strings.Contains(lastRef, "GETACK") {
		response := "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n"
		_, err = conn.Write([]byte(response))
		if err != nil {
			fmt.Println("An error occured during the handshake", err)
		}
	}
	// connCh <- conn
	// }()

	// if err != nil {
	// 	log.Fatalf(err.Error())
	// }
	// conn := <-connCh
	go func() {
		s.handleClient(conn, buf)
	}()

}
func (s *Server) handshake() (net.Conn, string, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", s.replication.HOST_IP, s.replication.HOST_PORT))
	if err != nil {
		fmt.Printf("Replication couldnt connect to master on port %s", s.replication.HOST_PORT)
		return nil, "", err
	}
	handshakeStages := []string{
		SliceToBulkString([]string{"PING"}),
		SliceToBulkString([]string{"REPLCONF", "listening-port", strings.Split(s.addr, ":")[1]}),
		SliceToBulkString([]string{"REPLCONF", "capa", "psync2"}),
		SliceToBulkString([]string{"PSYNC", "?", "-1"})}

	buf := make([]byte, 2048)
	lastres := ""
	for _, hsInput := range handshakeStages {
		_, err := conn.Write([]byte(hsInput))
		if err != nil {
			return nil, "", err
		}
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return conn, "", err
		}
		lastres = string(buf[:n])
	}

	fmt.Println("Handshake finished")
	return conn, lastres, nil
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
				break
			} else {
				log.Printf("Error reading from connection: %v", err)
			}
			continue
		}
		// if s.status == "master" {

		// 	if s.Parser.isValidBulkString(buf[:i]) {
		// 		cmds, err := s.Parser.Parse(buf[:i], s)
		// 		if err != nil {
		// 			log.Printf("Error parsing: %s", err.Error())
		// 			continue
		// 		}
		// 		response, err := s.handleCmds(cmds, conn)
		// 		if err != nil {
		// 			log.Printf("Error parsing input: %v", err)
		// 			continue
		// 		}
		// 		err = s.writeResponse(conn, response)
		// 		if err != nil {
		// 			log.Printf("Error writing a response: %v", err)
		// 			continue
		// 		}
		// 	}
		// } else {
		fmt.Println("res", string(buf[:i]))
		if s.Parser.isValidBulkString(buf[:i]) {
			cmds, err := s.Parser.parseReplication(buf[:i], s)
			if err != nil {
				log.Printf("Error parsing: %v", err)
				continue
			}
			for _, cmd := range cmds {
				response, err := s.handleCmds(cmd, conn)
				if s.status == "master" || ((cmd[0] == "replconf" && cmd[1] == "getack") || conn.RemoteAddr().String() != fmt.Sprintf("%s:%s", s.replication.HOST_IP, s.replication.HOST_PORT)) {
					writeErr := s.writeResponse(conn, response)
					if writeErr != nil {
						log.Printf("Error writing a response: %v", writeErr)
						continue
					}
				}
				if err != nil {
					log.Printf("Error occurred handleCmds in replication: %v", err)
					continue
				}
			}
		}
	}

}

func (s *Server) writeResponse(writer io.Writer, mess string) error {
	_, err := writer.Write([]byte(mess))
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) handleCmds(cmdArr []string, conn net.Conn) (string, error) {
	if len(cmdArr) == 0 {
		return "", fmt.Errorf("Command Array is empty")
	}
	switch strings.ToLower(cmdArr[0]) {
	case "echo":
		return s.echo(cmdArr, conn), nil
	case "ping":
		return "+PONG\r\n", nil
	case "+ok":
		return "", nil
	case "command":
		return "+PONG\r\n", nil
	case "set":
		return s.set(cmdArr, conn)
	case "get":
		return s.get(cmdArr, conn)
	case "info":
		return s.info(cmdArr), nil
	case "test":
		s.handlePropagation([]string{"replconf", "getack", "*"})
		return "+PONG\r\n", nil
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
		err := s.writeResponse(*conn, cmd)
		if err != nil {
			log.Printf("An error occurred while sending propagations %v", err)
		}
	}

}

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

const (
	CommandEcho     = "echo"
	CommandPing     = "ping"
	CommandOK       = "+ok"
	CommandCommand  = "command"
	CommandWait     = "wait"
	CommandSet      = "set"
	CommandGet      = "get"
	CommandInfo     = "info"
	CommandKeys     = "keys"
	CommandReplconf = "replconf"
	CommandPsync    = "psync"
	CommandConfig   = "config"
	CommandType     = "type"
	CommandXAdd     = "xadd"
	CommandXRange   = "xrange"
	CommandXRead    = "xread"
)

type Replication struct {
	HOST_IP   string
	HOST_PORT string
	offset    int
}
type Server struct {
	addr   string
	status string

	replication Replication

	Store     *Store
	Parser    Parser
	rdbParser RdbParser

	consMu              sync.Mutex
	repConns            []*net.Conn
	acks                int
	ackCh               chan bool
	mu                  sync.Mutex
	ackCond             *sync.Cond
	pendingPropagations bool
	wait                int
}

func main() {
	server := newServer()
	l, err := server.start()
	if server.rdbParser.dir != "" && server.rdbParser.filename != "" {
		server.rdbParser.loadData(server)
	}
	if err != nil {
		fmt.Printf("Failed to bind to port %s", strings.Split(server.addr, ":")[1])
		os.Exit(1)
	}
	fmt.Println("Server started on ", server.addr)
	sem := make(chan struct{}, 100)
	//Listen for incoming connections
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
func newServer() *Server {
	s := &Server{
		Store: &Store{
			Mutex:  sync.Mutex{},
			Data:   make(map[string]Value),
			Stream: make(map[string][]Entry),
		},
		pendingPropagations: false,
		ackCh:               make(chan bool, 100),
	}
	s.ackCond = sync.NewCond(&s.mu)
	return s
}
func (s *Server) start() (net.Listener, error) {
	portFlag := flag.String("port", "6379", "Give a custom port to run the server ")
	rdbDir := flag.String("dir", "", "Specify a filepath where the rdb file is stored")
	rdbfileName := flag.String("dbfilename", "", "Specify a filename for the rdb ")
	replicationFlag := flag.Bool("replicaof", false, "Specify if the server is a replica")
	flag.Parse()
	s.rdbParser.dir = *rdbDir
	s.rdbParser.filename = *rdbfileName
	s.addr = fmt.Sprintf("%s:%s", "localhost", *portFlag)
	if *replicationFlag {
		s.handleReplication()
	} else {
		s.status = "master"
	}
	return net.Listen("tcp", s.addr)
}

/*
HandleClient handles the client connection
in an infinite loop, it reads the input from the client and
sends it to the parser to parse the commands and handle them accordingly
*/
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
		res := string(buf[:i])
		// fmt.Printf("Received data: %s\n", res)
		if s.status == "slave" && s.isRemoteMaster(conn) {
			if strings.Contains(res, "redis") {
				s.handleRDBAndGetAck(res, &conn)
				s.replication.offset = 37
			}
		}
		if s.Parser.isValidBulkString(buf[:i]) {
			v, err := s.Parser.parseMultipleCmds(buf[:i], s)
			if err != nil {
				log.Printf("Error parsing: %v", err)
				continue
			}
			cmds, err := s.Parser.parseReplication(v, s)
			// fmt.Println(cmds)
			if err != nil {
				log.Printf("Error parsing: %v", err)
				continue
			}
			for i := 0; i < len(cmds); i++ {
				go func(i int) {
					cmd := cmds[i]
					response, err := s.handleCmds(cmd, conn)
					if s.shouldRespond(cmd, conn) {
						if response != "" {
							_, err := conn.Write([]byte(response))
							if err != nil {
								log.Printf("Error writing a response: %v", err)
							}
						}
					}
					if err != nil {
						log.Printf("Error occurred handleCmds in replication: %v", err)
					}
					if s.status != "master" {
						// fmt.Printf("Added %d to offset %d with %s\n", len(v[i]), s.replication.offset, res)
						s.replication.offset += len(v[i])
					}
				}(i)
			}
		}
	}
}

func (s *Server) isRemoteMaster(conn net.Conn) bool {
	hostIP := s.replication.HOST_IP
	if hostIP == "localhost" {
		hostIP = "[::1]"
	}
	val := conn.RemoteAddr().String() == fmt.Sprintf("%s:%s", hostIP, s.replication.HOST_PORT)
	return val
}

func (s *Server) shouldRespond(cmd []string, conn net.Conn) bool {
	return s.status == "master" || (cmd[0] == "replconf" && cmd[1] == "getack") || !s.isRemoteMaster(conn)
}

/*
Create a new replication connection and initiate the handshake
*/
func (s *Server) handleReplication() {
	s.status = "slave"
	fmt.Println("Replication started", flag.Args()[0])

	if len(flag.Args()) < 2 {
		p := strings.Split(flag.Args()[0], " ")
		s.replication = Replication{
			HOST_IP:   p[0],
			HOST_PORT: p[1],
		}
	} else {
		s.replication = Replication{
			HOST_IP:   flag.Args()[0],
			HOST_PORT: flag.Args()[1],
		}
	}
	conn, err := s.handshake()
	if err != nil {
		fmt.Println("An error occured during the handshake", err)
		return
	}

	buf := make([]byte, 2048)
	go func() {
		s.handleClient(conn, buf)
	}()
}

func (s *Server) handshake() (net.Conn, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", s.replication.HOST_IP, s.replication.HOST_PORT))
	if err != nil {
		fmt.Printf("Replication couldnt connect to master on port %s", s.replication.HOST_PORT)
		return nil, err
	}
	handshakeStages := []string{
		SliceToBulkString([]string{"PING"}),
		SliceToBulkString([]string{"REPLCONF", "listening-port", strings.Split(s.addr, ":")[1]}),
		SliceToBulkString([]string{"REPLCONF", "capa", "psync2"}),
		SliceToBulkString([]string{"PSYNC", "?", "-1"}),
	}

	buf := make([]byte, 2048)
	for _, hsInput := range handshakeStages {
		_, err := conn.Write([]byte(hsInput))
		if err != nil {
			return nil, err
		}
		i, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return conn, err
		}
		res := string(buf[:i])

		if strings.Contains(res, "*") {
			fmt.Println("1. Read in handshake after write ", res)
			s.handleRDBAndGetAck(res, &conn)
			break
		}
	}
	conn.SetReadDeadline(time.Time{})
	fmt.Println("Handshake finished")
	return conn, nil
}

/*
HandlePropagation sends the command to all replication servers
*/
func (s *Server) handlePropagation(cmdArr []string) {
	s.mu.Lock()
	s.acks = 0
	s.mu.Unlock()
	var wg sync.WaitGroup
	for _, conn := range s.repConns {
		wg.Add(1)
		go func(conn *net.Conn) {
			defer wg.Done()
			cmd := SliceToBulkString(cmdArr)
			// fmt.Println("Sending propagation", cmdArr[0])
			_, err := (*conn).Write([]byte(cmd))
			if err != nil {
				log.Printf("An error occurred while sending propagation %v", err)
			}
		}(conn)
	}

	wg.Wait()
	log.Println("All propagations sent")
	s.getAcks()
}

/*
Get the acks from the replication servers after a propagation
*/
func (s *Server) getAcks() {
	wg := sync.WaitGroup{}
	for _, conn := range s.repConns {
		wg.Add(1)
		go func(conn *net.Conn) {
			defer wg.Done()
			cmd := SliceToBulkString([]string{"replconf", "GETACK", "*"})
			fmt.Println("Sending getack")
			_, err := (*conn).Write([]byte(cmd))
			if err != nil {
				log.Printf("An error occurred while sending getack %v", err)
			}
			buffer := make([]byte, 1024)
			_, err = (*conn).Read(buffer)
			if err != nil {
				log.Printf("An error occurred while reading getack %v", err)
			}
			// fmt.Println("Got replconf response", string(buffer[:r]))
			s.mu.Lock()
			s.acks++
			// fmt.Println("Ack received in master currentAcks:", s.acks)
			s.mu.Unlock()
			s.ackCh <- true
		}(conn)
	}
	wg.Wait()
}

func (s *Server) handleCmds(cmdArr []string, conn net.Conn) (string, error) {
	if len(cmdArr) == 0 {
		return "", fmt.Errorf("command Array is empty")
	}
	switch strings.ToLower(cmdArr[0]) {
	case CommandEcho:
		return s.handleEcho(cmdArr, conn), nil
	case CommandPing:
		return s.handlePing(), nil
	case CommandOK:
		return "", nil
	case CommandCommand:
		return s.handleCommdand()
	case CommandWait:
		return s.handleWait(cmdArr, &conn)
	case CommandSet:
		return s.handleSet(cmdArr, conn)
	case CommandGet:
		return s.handleGet(cmdArr, conn)
	case CommandInfo:
		return s.handleInfo(cmdArr)
	case CommandKeys:
		return s.handleKeys()
	case CommandReplconf:
		return s.handleReplconf(cmdArr, &conn)
	case CommandPsync:
		return s.handlePsync(cmdArr, conn)
	case CommandConfig:
		return s.handleConfig(cmdArr)
	case CommandType:
		return s.handleType(cmdArr)
	case CommandXAdd:
		return s.handleXADD(cmdArr)
	case CommandXRange:
		return s.handleXRANGE(cmdArr)
	case CommandXRead:
		return s.handleXREAD(cmdArr)
	default:
		return "", fmt.Errorf("unknown command: %v", cmdArr[0])
	}
}

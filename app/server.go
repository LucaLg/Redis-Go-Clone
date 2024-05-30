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

	consMu   sync.Mutex
	repConns []*net.Conn
}

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
	getack := false
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
			fmt.Println("1.Read in handshake after write ", res)
			s.handleRDBAndGetAck(res, conn)
			getack = true
			break
		}
	}
	if !getack {
		// for {
		// 	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		// 	i, err := conn.Read(buf)
		// 	if err != nil {
		// 		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		// 			fmt.Println("Read timed out")
		// 			fmt.Println("2.Read in handshake after write ", string(buf[:i]))

		// 		} else {
		// 			fmt.Println("Read error:", err)
		// 		}
		// 		break
		// 	}
		// }
	}
	conn.SetReadDeadline(time.Time{})
	fmt.Println("Handshake finished")
	return conn, nil
}
func newServer() *Server {
	return &Server{
		Store: &Store{
			Mutex:  sync.Mutex{},
			Data:   make(map[string]Value),
			Stream: make(map[string][]Entry),
		},
	}
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
		res := string(buf[:i])
		if s.status == "slave" && s.isRemoteMaster(conn) {
			if strings.Contains(res, "redis") {
				s.handleRDBAndGetAck(res, conn)
				s.replication.offset = 37
			}
		}
		if s.Parser.isValidBulkString(buf[:i]) {
			v, err := s.Parser.parseMultipleCmds(buf[:i], s)
			if err != nil {
				log.Printf("Error parsing: %v", err)
				continue
			}
			for _, cmd := range v {
				fmt.Println("Length of cmd", len(cmd))
			}
			cmds, err := s.Parser.parseReplication(v, s)
			fmt.Println(cmds)
			if err != nil {
				log.Printf("Error parsing: %v", err)
				continue
			}
			for i := 0; i < len(cmds); i++ {
				cmd := cmds[i]
				response, err := s.handleCmds(cmd, conn)
				if s.shouldRespond(cmd, conn) {
					fmt.Println("Received cmd", cmd)
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
				fmt.Printf("Added %d to offset %d with %s\n", len(v[i]), s.replication.offset, res)
				s.replication.offset += len(v[i])
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
	fmt.Println()
	return s.status == "master" || (cmd[0] == "replconf" && cmd[1] == "getack") || !s.isRemoteMaster(conn)
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
		return s.handleWait()
	case CommandSet:
		return s.handleSet(cmdArr, conn)
	case CommandGet:
		return s.handleGet(cmdArr, conn)
	case CommandInfo:
		return s.handleInfo(cmdArr)
	case CommandKeys:
		return s.handleKeys()
	case CommandReplconf:
		return s.handleReplconf(cmdArr)
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

func (s *Server) handlePropagation(cmdArr []string) {
	for _, conn := range s.repConns {
		cmd := SliceToBulkString(cmdArr)
		err := s.writeResponse(*conn, cmd)
		if err != nil {
			log.Printf("An error occurred while sending propagations %v", err)
		}
	}
}

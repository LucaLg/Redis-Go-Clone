package main

import (
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	typeString = "+string\r\n"
	typeNone   = "+none\r\n"
	typeStream = "+stream\r\n"
)

func (s *Server) handleEcho(cmdArr []string, conn net.Conn) string {
	return fmt.Sprintf("+%s\r\n", cmdArr[1])
}
func (s *Server) handleSet(cmdArr []string, conn net.Conn) (string, error) {
	if s.status == "master" {
		s.handlePropagation(cmdArr)
	}
	s.Store.handleSet(cmdArr)
	return "+OK\r\n", nil
}
func (s *Server) handleGet(cmdArr []string, conn net.Conn) (string, error) {
	if s.status == "master" {
		s.handlePropagation(cmdArr)
	}
	result, err := s.Store.handleGet(cmdArr[1])
	if err != nil {
		return "", err
	}
	return result, nil
}
func (s *Server) handleInfo(cmdArr []string) (string, error) {
	if cmdArr[1] == "replication" {
		role := fmt.Sprintf("role:%s", s.status)
		replid := fmt.Sprintf("master_replid:%s", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb")
		offset := fmt.Sprintf("master_repl_offset:%s", "0")
		info := fmt.Sprintf("%s\n%s\n%s", role, replid, offset)
		res := StringToBulkString(info)
		return res, nil
	}
	return "", nil
}
func (s *Server) handleReplconf(cmdArr []string) (string, error) {

	fmt.Println(" received ", cmdArr[1])
	if len(cmdArr) > 1 {
		switch cmdArr[1] {
		case "getack":
			fmt.Println("GetACK received ", cmdArr)
			offset := fmt.Sprint(s.replication.offset)
			res := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%s\r\n", len(offset), offset)
			return res, nil
		case "ack":
			return "", nil
		default:
			return "+OK\r\n", nil
		}
	}
	return "+OK\r\n", nil
}
func (s *Server) handlePsync(cmdArr []string, conn net.Conn) (string, error) {
	conn.(*net.TCPConn).SetNoDelay(true)
	id := "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	fullResync := fmt.Sprintf("+FULLRESYNC %s 0\r\n", id)
	s.writeResponse(conn, fullResync)
	s.consMu.Lock()
	s.repConns = append(s.repConns, &conn)
	s.consMu.Unlock()
	f := s.rdbFile()
	_, err := conn.Write([]byte(f))
	if err != nil {
		fmt.Printf("File not written %s", f)
	}
	time.Sleep(1 * time.Second)
	repl := SliceToBulkString([]string{"REPLCONF", "GETACK", "*"})
	return repl, nil
}
func (s *Server) rdbFile() string {
	emptyFileBase64 := "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
	emptyFile, err := base64.RawStdEncoding.DecodeString(emptyFileBase64)
	if err != nil {
		fmt.Errorf("An error occured encoding the rdbfile %v", err)
	}
	return fmt.Sprintf("$%d\r\n%s", len(emptyFile), emptyFile)
}
func (s *Server) handleRDBAndGetAck(c string, w io.Writer) error {
	cmds := strings.Split(c, "*")
	if len(cmds) > 1 {
		replRes, err := s.handleReplconf([]string{"replconf", "getack", "*"})
		if err != nil {
			return err
		}
		_, err = w.Write([]byte(replRes))
		if err != nil {
			return err
		}
	}
	return fmt.Errorf("rdb file was send seperate from getack ")
}
func (s *Server) handleConfig(cmdArr []string) (string, error) {
	if len(cmdArr) < 2 {
		return "", fmt.Errorf("error handling config ")
	} else {
		switch cmdArr[2] {
		case "dir":
			return SliceToBulkString([]string{"dir", s.rdbParser.dir}), nil
		case "dbfilename":
			return SliceToBulkString([]string{"dbfilename", s.rdbParser.filename}), nil
		}
	}
	return "", fmt.Errorf("no config message handled")
}
func (s *Server) handleKeys() (string, error) {
	return SliceToBulkString(s.Store.getKeys()), nil
}
func (s *Server) handleCommdand() (string, error) {
	return "+PONG\r\n", nil
}
func (s *Server) handlePing() string {
	return "+PONG\r\n"
}
func (s *Server) handleWait() (string, error) {
	return fmt.Sprintf(":%d\r\n", len(s.repConns)), nil
}
func (s *Server) handleType(cmdArr []string) (string, error) {
	if len(cmdArr) < 2 {
		return "", fmt.Errorf("couldnt return type no key given")
	}
	// If it exists in Data Map it is a string
	_, exists := s.Store.Data[cmdArr[1]]
	if exists {
		return typeString, nil
	}
	// If it exists in Stream Map it is a stream
	_, exists = s.Store.Stream[cmdArr[1]]
	if exists {
		return typeStream, nil
	}
	if !exists {
		return typeNone, nil
	}

	return "", fmt.Errorf("couldnt get type of value")

}
func (s *Server) handleXADD(cmdArr []string) (string, error) {
	if len(cmdArr) < 3 {
		return "", fmt.Errorf("couldnt save stream with no key")
	}
	pairs := make([]EntryPair, 0)
	var p EntryPair
	for i := 3; i < len(cmdArr); i++ {
		if i%2 == 0 {
			p.val = cmdArr[i]
			pairs = append(pairs, p)
		} else {
			p.key = cmdArr[i]
		}
	}
	id := s.Store.storeStream(cmdArr[2], cmdArr[1], pairs)
	return id, nil
}
func (s *Server) handleXRANGE(cmdArr []string) (string, error) {
	if len(cmdArr) < 4 {
		return "", fmt.Errorf("no valid range given")
	}
	res, err := s.Store.getEntriesOfRange(cmdArr[1], cmdArr[2], cmdArr[3])
	if err != nil {
		return "", err
	}
	return res, nil
}
func (s *Server) handleXREAD(cmdArr []string) (string, error) {
	if len(cmdArr) < 4 {
		return "", fmt.Errorf("no valid input")
	}
	if cmdArr[1] == "block" {
		key := cmdArr[4]
		id := cmdArr[5]
		blockTime, err := strconv.Atoi(cmdArr[2])
		if err != nil {
			fmt.Println("error parsing block time ", err)
			return "", err
		}
		if blockTime == 0 {
			res, err := s.blockWithNull(key, id)
			if err != nil {
				return "", err
			}
			return res, nil
		}
		if blockTime > 0 {
			res, err := s.blockWithTime(blockTime, key, id)
			if err != nil {
				return "", err
			}
			return res, nil
		}
	}
	if cmdArr[1] == "streams" {
		split := (len(cmdArr) - 2) / 2
		res, err := s.Store.readMultipleStreams(cmdArr[2:split+2], cmdArr[2+split:])
		if err != nil {

			return "", err
		}
		return res, nil
	}
	return "", nil
}
func (s *Server) blockWithNull(key string, id string) (string, error) {

	firstRes, err := s.Store.readRange(key, id)
	if err != nil {
		fmt.Println("error reading multiple streams ", err)
		return "", err
	}
	for {
		res, err := s.Store.readRange(key, id)
		if err != nil {
			fmt.Println("error reading multiple streams ", err)
			return "", err
		}
		if res != firstRes {
			res = fmt.Sprintf("*%d\r\n%s", 1, res)
			return res, nil
		}
	}

}
func (s *Server) blockWithTime(blockTime int, key string, id string) (string, error) {
	time.Sleep(time.Duration(blockTime) * time.Millisecond)
	res, err := s.Store.readRange(key, id)
	if err != nil {
		fmt.Println("error reading multiple streams ", err)
		return "", err
	}
	if res == "" {
		return "$-1\r\n", nil
	} else {
		res = fmt.Sprintf("*%d\r\n%s", 1, res)
		return res, nil
	}
}

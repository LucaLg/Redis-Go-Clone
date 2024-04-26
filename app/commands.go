package main

import (
	"encoding/base64"
	"fmt"
	"net"
)

func (s *Server) echo(cmdArr []string, conn net.Conn) string {
	return fmt.Sprintf("+%s\r\n", cmdArr[1])
}
func (s *Server) set(cmdArr []string, conn net.Conn) (string, error) {
	s.handlePropagation(cmdArr)
	s.Store.handleSet(cmdArr)
	return "+OK\r\n", nil
}
func (s *Server) get(cmdArr []string, conn net.Conn) (string, error) {
	s.handlePropagation(cmdArr)
	result, err := s.Store.handleGet(cmdArr[1])
	if err != nil {
		return "", err
	}
	return result, nil
}
func (s *Server) info(cmdArr []string) string {
	if cmdArr[1] == "replication" {
		role := fmt.Sprintf("role:%s", s.status)
		replid := fmt.Sprintf("master_replid:%s", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb")
		offset := fmt.Sprintf("master_repl_offset:%s", "0")
		info := fmt.Sprintf("%s\n%s\n%s", role, replid, offset)
		res := StringToBulkString(info)
		return res
	}
	return ""
}
func (s *Server) replconf(cmdArr []string) (string, error) {
	// return *3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n
	if len(cmdArr) == 1 {
		return "+OK\r\n", nil
	} else {
		switch cmdArr[1] {
		case "getack":
			return "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n", nil
		default:
			return "", nil
		}
	}
	//handle that the response is written to the master from replication only for replconf
}
func (s *Server) psync(cmdArr []string, conn net.Conn) (string, error) {
	id := "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	fullResync := fmt.Sprintf("+FULLRESYNC %s 0\r\n", id)
	s.writeResponse(conn, fullResync)
	s.consMu.Lock()
	s.repConns = append(s.repConns, &conn)
	s.consMu.Unlock()
	return s.rdbFile(), nil
}
func (s *Server) rdbFile() string {
	emptyFileBase64 := "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
	emptyFile, err := base64.RawStdEncoding.DecodeString(emptyFileBase64)
	if err != nil {
		fmt.Errorf("An error occured encoding the rdbfile %v", err)
	}
	return fmt.Sprintf("$%d\r\n%s", len(emptyFile), emptyFile)
}

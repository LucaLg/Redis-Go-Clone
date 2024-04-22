package main

import (
	"fmt"
	"strings"
)

type Parser struct {
}

func parseLength(input []byte, index int) (int, int) {
	var arrayLength int
	var i int = index + 1
	for input[i] != '\r' {
		arrayLength = (arrayLength * 10) + ByteToDigit(input[i])
		i++
	}
	return arrayLength, i + 2
}

func parseWords(input []byte, startIndex int) (string, int) {
	wordLength, index := parseLength(input, startIndex)
	return string(input[index : index+wordLength]), wordLength + index + 2
}
func (p *Parser) Parse(input []byte, s *Server) ([]string, error) {
	arrayLength, index := parseLength(input, 0)
	cmds := make([]string, arrayLength)
	for i := 0; i < arrayLength; i++ {
		cmds[i], index = parseWords(input, index)
		cmds[i] = strings.ToLower(cmds[i])
	}
	return cmds, nil
}

// func (p *Parser) handleCmds(cmdArr []string, s *Server) (string, error) {

// 	switch strings.ToLower(cmdArr[0]) {
// 	case "echo":
// 		return fmt.Sprintf("+%s\r\n", cmdArr[1]), nil
// 	case "ping":
// 		return "+PONG\r\n", nil
// 	case "COMMAND":
// 		return "+PONG\r\n", nil
// 	case "set":
// 		s.Store.handleSet(cmdArr)
// 		return "+OK\r\n", nil
// 	case "get":
// 		return s.Store.handleGet(cmdArr[1])
// 	case "info":
// 		return handleInfo(cmdArr), nil
// 	case "replconf":
// 		return "+OK\r\n", nil
// 	case "psync":
// 		return "+OK\r\n", nil
// 	default:
// 		return "", fmt.Errorf("Unknown command: %s", cmdArr[0])
// 	}
// }

// func parse(input []byte) (string, error) {
// 	arrayLength, index := parseLength(input, 0)
// 	cmds := make([]string, arrayLength)
// 	for i := 0; i < arrayLength; i++ {
// 		cmds[i], index = parseWords(input, index)
// 		cmds[i] = strings.ToLower(cmds[i])
// 	}
// 	return handleCmds(cmds)
// }

// func handleCmds(cmdArr []string) (string, error) {
// 	switch strings.ToLower(cmdArr[0]) {
// 	case "echo":
// 		return fmt.Sprintf("+%s\r\n", cmdArr[1]), nil
// 	case "ping":
// 		return "+PONG\r\n", nil
// 	case "COMMAND":
// 		return "+PONG\r\n", nil
// 	case "set":
// 		handleSet(cmdArr)
// 		return "+OK\r\n", nil
// 	case "get":
// 		return handleGet(cmdArr[1]), nil
// 	case "info":
// 		return handleInfo(cmdArr), nil
// 	case "replconf":
// 		return "+OK\r\n", nil
// 	case "psync":
// 		return "+OK\r\n", nil
// 	default:
// 		return "", fmt.Errorf("Unknown command: %s", cmdArr[0])
// 	}
// }

func handleInfo(cmdArr []string) string {
	if cmdArr[1] == "replication" {
		role := fmt.Sprintf("role:%s", status)
		replid := fmt.Sprintf("master_replid:%s", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb")
		offset := fmt.Sprintf("master_repl_offset:%s", "0")
		info := fmt.Sprintf("%s\n%s\n%s", role, replid, offset)
		res := StringToBulkString(info)
		return res
	}
	return ""
}

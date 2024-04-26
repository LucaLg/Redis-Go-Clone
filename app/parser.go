package main

import (
	"fmt"
	"strings"
)

type Parser struct {
}

func parseLength(input []byte, index int) (int, int, error) {
	if string(input[0]) != "$" && string(input[0]) != "*" {
		return -1, -1, fmt.Errorf("Isnt a valid input to parse")
	}
	var arrayLength int
	var i int = index + 1
	for input[i] != '\r' {
		arrayLength = (arrayLength * 10) + ByteToDigit(input[i])
		i++
	}
	return arrayLength, i + 2, nil
}

func parseWords(input []byte, startIndex int) (string, int) {
	wordLength, index, err := parseLength(input, startIndex)
	if err != nil {
		return "", -1
	}
	return string(input[index : index+wordLength]), wordLength + index + 2
}
func (p *Parser) Parse(input []byte, s *Server) ([]string, error) {
	arrayLength, index, err := parseLength(input, 0)
	if arrayLength < 0 || err != nil {
		return nil, fmt.Errorf("the input coulndt be parsed %s", string(input))
	}
	cmds := make([]string, arrayLength)
	for i := 0; i < arrayLength; i++ {
		cmds[i], index = parseWords(input, index)
		cmds[i] = strings.ToLower(cmds[i])
	}
	return cmds, nil
}
func (p *Parser) parseReplication(input []byte, s *Server) ([][]string, error) {
	inputs := make([][]byte, 0)
	var lastStartIndex = 0
	for i := 0; i < len(input); i++ {
		if input[i] == '*' && i != 0 {
			inputs = append(inputs, input[lastStartIndex:i])
			lastStartIndex = i
		}
	}
	inputs = append(inputs, input[lastStartIndex:])
	//the first input got added twice but the second one wasnt added
	commandsSlices := make([][]string, 0)
	for _, input := range inputs {
		cmds, err := s.Parser.Parse(input, s)
		if err != nil {
			return nil, fmt.Errorf(err.Error())
		}
		commandsSlices = append(commandsSlices, cmds)
	}
	fmt.Println(commandsSlices)
	return commandsSlices, nil
}

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

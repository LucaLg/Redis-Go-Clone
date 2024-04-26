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
	for i < len(input) && input[i] != '\r' {
		arrayLength = (arrayLength * 10) + ByteToDigit(input[i])
		i++
	}
	return arrayLength, i + 2, nil
}

func parseWords(input []byte, startIndex int) (string, int, error) {
	wordLength, index, err := parseLength(input, startIndex)
	if err != nil {
		return "", -1, err
	}
	if index+wordLength > len(input) {
		return "", -1, fmt.Errorf("index out of bounce while reading word ")
	}
	return string(input[index : index+wordLength]), wordLength + index + 2, nil
}
func (p *Parser) Parse(input []byte, s *Server) ([]string, error) {
	arrayLength, index, err := parseLength(input, 0)
	if arrayLength < 0 || err != nil {
		return nil, fmt.Errorf("the input coulndt be parsed %s", string(input))
	}
	cmds := make([]string, arrayLength)
	for i := 0; i < arrayLength; i++ {
		cmds[i], index, err = parseWords(input, index)
		if err != nil {
			return nil, err
		}
		cmds[i] = strings.ToLower(cmds[i])
	}
	return cmds, nil
}

/*
ParseReplication gets an []byte as input
it handles the parsing of messages send by the master server to the replication server
it splits up multiple commands send in one input slice so the client parser can handle it
*/
func (p *Parser) parseReplication(input []byte, s *Server) ([][]string, error) {
	inputs := make([][]byte, 0)
	var lastStartIndex = 0
	for i := 0; i < len(input); i++ {
		if input[i] == '*' && i != 0 && isValidCommandStart(input, i) {
			inputs = append(inputs, input[lastStartIndex:i])
			lastStartIndex = i
		}
	}
	inputs = append(inputs, input[lastStartIndex:])
	commandsSlices := make([][]string, 0)
	for _, input := range inputs {
		cmds, err := s.Parser.Parse(input, s)
		if err != nil {
			return nil, fmt.Errorf(err.Error())
		}
		if len(cmds) > 0 {
			commandsSlices = append(commandsSlices, cmds)
		}
	}
	return commandsSlices, nil
}
func isValidCommandStart(input []byte, i int) bool {
	return i+1 < len(input) && input[i+1] != '\r'
}
func (p *Parser) countCommands(input []byte) (int, error) {
	if input[0] != '*' {
		return -1, fmt.Errorf("received not a valid input")
	}
	var cmdCount = 1
	for i := 0; i < len(input); i++ {
		if input[i] == '*' && i != 0 && i+1 < len(input) && input[i+1] != '\r' {
			cmdCount++
		}
	}

	return cmdCount, nil
}

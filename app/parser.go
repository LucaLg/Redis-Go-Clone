package main

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

func parse(input []byte) (string, error) {
	var arrayLength string
	i := 1
	for unicode.IsDigit(rune(input[i])) {
		arrayLength += string(input[i])
		i++
	}
	inputArr := strings.Split(string(input), "\r\n")
	// *2\r\n$4\r\necho\r\n$3\r\nhey\r\n
	// [0] *2
	// [1] $4
	// [2] echo
	// [3] $3
	// [4] hey
	num, err := strconv.Atoi(arrayLength)
	cmdArr := make([]string, num)
	for _, inputString := range inputArr {
		if inputString[0] != '$' && inputString[0] != '*' {
			cmdArr = append(cmdArr, inputString)
		}
	}
	if err != nil {
		fmt.Println("Error parsing array length: ", err.Error())
		return "", err
	}

	return handleCmds(cmdArr)
}
func handleCmds(cmdArr []string) (string, error) {
	switch cmdArr[0] {
	case "echo":
		return cmdArr[1], nil
	case "ping":
		return "+PONG\r\n", nil
	default:
		return "", fmt.Errorf("Unknown command: %s", cmdArr[0])
	}
}

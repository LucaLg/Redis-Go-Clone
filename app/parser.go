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
	num, err := strconv.Atoi(arrayLength)
	cmdArr := make([]string, num)
	y := 0
	for _, inputString := range inputArr {
		inputString = strings.TrimSpace(inputString)
		if len(inputString) > 0 && inputString[0] != '$' && inputString[0] != '*' {
			cmdArr[y] = inputString
			y++
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
		return fmt.Sprintf("+%s\r\n", cmdArr[1]), nil
	case "ping":
		return "+PONG\r\n", nil
	case "COMMAND":
		return "+PONG\r\n", nil
	default:
		return "", fmt.Errorf("Unknown command: %s", cmdArr[0])
	}
}

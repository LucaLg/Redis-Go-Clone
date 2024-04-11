package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

var store = make(map[string]string)
var mutex = &sync.Mutex{}

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
	case "set":
		if len(cmdArr) == 3 {
			handleSet(cmdArr[1], cmdArr[2])
			return "+OK\r\n", nil
		} else {
			return "", fmt.Errorf("Unknown command: %s", cmdArr[0])
		}
	case "get":
		if len(cmdArr) == 2 {
			return handleGet(cmdArr[1]), nil
		} else {

			return "", fmt.Errorf("Unknown command: %s", cmdArr[0])
		}
	default:
		return "", fmt.Errorf("Unknown command: %s", cmdArr[0])
	}
}
func handleSet(key, value string) {
	mutex.Lock()
	store[key] = value
	mutex.Unlock()
}
func handleGet(key string) string {
	mutex.Lock()
	val := store[key]
	mutex.Unlock()
	return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
}

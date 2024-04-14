package main

import (
	"fmt"
	"sync"
	"time"
)

type Value struct {
	value   string
	savedAt time.Time
	expire  time.Duration
}

var store = make(map[string]Value)
var mutex = &sync.Mutex{}

func parseLength(input []byte, index int) (int, int) {
	var arrayLength int
	var i int = index + 1
	for input[i] != '\r' {
		arrayLength = (arrayLength * 10) + byteToDigit(input[i])
		i++
	}
	return arrayLength, i + 2
}
func byteToDigit(b byte) int {
	return int(b - '0')
}
func parseWords(input []byte, startIndex int) (string, int) {
	wordLength, index := parseLength(input, startIndex)
	return string(input[index : index+wordLength]), wordLength + index + 2
}
func parse(input []byte) (string, error) {
	arrayLength, index := parseLength(input, 0)
	cmds := make([]string, arrayLength)
	for i := 0; i < arrayLength; i++ {
		cmds[i], index = parseWords(input, index)
	}
	fmt.Print(cmds)
	res, err := handleCmds(cmds)
	if err != nil {
		return "", fmt.Errorf("An error occurred handling the commands: %w", err)
	}
	fmt.Print(res)
	return handleCmds(cmds)
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
		handleSet(cmdArr)
		return "+Ok\r\n", nil
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
func handleSet(cmdArr []string) {
	var expire = time.Duration(-1)
	if len(cmdArr) == 5 {
		expoireTime, err := time.ParseDuration(cmdArr[4])
		if err != nil {
			fmt.Errorf("Error parsing expire Time %s occured", err)
		}
		expire = expoireTime
	}
	mutex.Lock()
	store[cmdArr[1]] = Value{
		value:   cmdArr[2],
		savedAt: time.Now(),
		expire:  expire,
	}
	mutex.Unlock()
}
func handleGet(key string) string {
	mutex.Lock()
	val := store[key]
	mutex.Unlock()
	if val.value == "" {
		val.value = "$-1\r\n"
	}
	now := time.Now()
	elapsed := val.savedAt.Sub(now)
	if time.Duration(val.expire) >= elapsed && val.expire != -1 {
		val.value = "$-1\r\n"
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(val.value), val.value)
}

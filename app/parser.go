package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

type Value struct {
	value   string
	savedAt time.Time
	expire  int
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
		if len(cmdArr) == 3 {
			handleSet(cmdArr[1], cmdArr[2])
			return "+OK\r\n", nil
		} else {
			if len(cmdArr) == 5 {
				time, err := strconv.Atoi(cmdArr[4])
				if err != nil {
					return "", fmt.Errorf("Parsing error of time")
				}
				handleSetExpire(cmdArr[1], cmdArr[2], time)
			}
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
func handleSetExpire(key, value string, expire int) {
	mutex.Lock()
	store[key] = Value{
		value:   value,
		savedAt: time.Now(),
		expire:  expire,
	}
	mutex.Unlock()
}
func handleSet(key, value string) {
	mutex.Lock()
	store[key] = Value{
		value:   value,
		savedAt: time.Now(),
		expire:  -1,
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

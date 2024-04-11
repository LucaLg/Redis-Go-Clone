package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

type Value struct {
	value   string
	savedAt time.Time
	expire  int
}

var store = make(map[string]Value)
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
	if time.Duration(val.expire) >= elapsed {
		val.value = "$-1\r\n"
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(val.value), val.value)
}

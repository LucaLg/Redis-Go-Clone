package main

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type Value struct {
	value   string
	savedAt time.Time
	expire  time.Duration
}

var (
	store = make(map[string]Value)
	mutex = &sync.Mutex{}
)

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
		cmds[i] = strings.ToLower(cmds[i])
	}
	return handleCmds(cmds)
}

func handleCmds(cmdArr []string) (string, error) {
	switch strings.ToLower(cmdArr[0]) {
	case "echo":
		return fmt.Sprintf("+%s\r\n", cmdArr[1]), nil
	case "ping":
		return "+PONG\r\n", nil
	case "COMMAND":
		return "+PONG\r\n", nil
	case "set":
		handleSet(cmdArr)
		return "+OK\r\n", nil
	case "get":
		return handleGet(cmdArr[1]), nil
	case "info":
		return handleInfo(cmdArr), nil

	default:
		return "", fmt.Errorf("Unknown command: %s", cmdArr[0])
	}
}

func handleInfo(cmdArr []string) string {
	if cmdArr[1] == "replication" {
		role := fmt.Sprintf("role:%s", status)
		replid := fmt.Sprintf("master_replid:%s", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb")
		offset := fmt.Sprintf("master_repl_offset:%s", "0")
		info := fmt.Sprintf("%s\n%s\n%s", role, replid, offset)
		res := transformStringToBulkString(info)
		return res
	}
	return ""
}

func handleSet(cmdArr []string) {
	expire := time.Duration(-1)
	if len(cmdArr) == 5 {
		expoireTime, err := time.ParseDuration(cmdArr[4] + "ms")
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
	if val.expire != time.Duration(-1) {
		now := time.Now()
		elapsed := now.Sub(val.savedAt)
		if val.expire < elapsed {
			delete(store, key)
			return "$-1\r\n"
		}
	}
	return transformStringToBulkString(val.value)
}

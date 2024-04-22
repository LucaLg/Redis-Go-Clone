package main

import (
	"fmt"
	"sync"
	"time"
)

type Store struct {
	Mutex sync.Mutex
	Data  map[string]Value
}

func (s *Store) handleGet(key string) (string, error) {
	s.Mutex.Lock()
	val, exist := s.Data[key]
	s.Mutex.Unlock()
	if !exist {
		return "", fmt.Errorf("Key not found")
	}
	if val.expire != time.Duration(-1) {
		now := time.Now()
		elapsed := now.Sub(val.savedAt)
		if val.expire < elapsed {
			delete(s.Data, key)
			return "$-1\r\n", nil
		}
	}
	return StringToBulkString(val.value), nil
}
func (s *Store) handleSet(cmdArr []string) {
	expire := time.Duration(-1)
	if len(cmdArr) == 5 {
		expoireTime, err := time.ParseDuration(cmdArr[4] + "ms")
		if err != nil {
			fmt.Errorf("Error parsing expire Time %s occured", err)
		}
		expire = expoireTime
	}
	s.Mutex.Lock()
	s.Data[cmdArr[1]] = Value{
		value:   cmdArr[2],
		savedAt: time.Now(),
		expire:  expire,
	}
	s.Mutex.Unlock()
}

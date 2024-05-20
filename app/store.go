package main

import (
	"fmt"
	"sync"
	"time"
)

type Store struct {
	Mutex   sync.Mutex
	Data    map[string]Value
	Streams map[string]Stream
}

type Value struct {
	value      string
	savedAt    time.Time
	expireDate time.Time
}
type Stream struct {
	id    string
	pairs []KeyValPair
}

func (s *Store) handleGet(key string) (string, error) {
	s.Mutex.Lock()
	val, exist := s.Data[key]
	s.Mutex.Unlock()
	if !exist {
		return "$-1\r\n", nil
	}
	if val.expireDate != val.savedAt {
		if !time.Now().Before(val.expireDate) {
			delete(s.Data, key)
			return "$-1\r\n", nil
		}
	}
	return StringToBulkString(val.value), nil
}
func (s *Store) handleSet(cmdArr []string) {
	expire := time.Duration(0)
	if len(cmdArr) == 5 {
		expoireTime, err := time.ParseDuration(cmdArr[4] + "ms")
		if err != nil {
			fmt.Errorf("error parsing expire Time %s occured", err)
		}
		expire = expoireTime
	}
	savedAt := time.Now()
	expireDate := savedAt.Add(expire)
	v := Value{
		value:      cmdArr[2],
		expireDate: expireDate,
		savedAt:    savedAt,
	}
	k := cmdArr[1]
	s.set(k, v)
}
func (s *Store) set(key string, val Value) {
	s.Mutex.Lock()
	s.Data[key] = val
	s.Mutex.Unlock()
}
func (s *Store) getKeys() []string {
	keys := make([]string, len(s.Data))
	i := 0
	for v, _ := range s.Data {
		keys[i] = v
		i++
	}
	return keys
}
func (s *Store) storeStream(id string, key string, pairs []KeyValPair) string {
	val, exists := s.Streams[key]
	if !exists {
		stream := Stream{id: id, pairs: pairs}
		s.Streams[key] = stream
	} else {
		val.pairs = append(val.pairs, pairs...)
	}
	return fmt.Sprintf("+%s\r\n", s.Streams[key].id)
}

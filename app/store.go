package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Store struct {
	Mutex  sync.Mutex
	Data   map[string]Value
	Stream map[string][]Entry
}

type Value struct {
	value      string
	savedAt    time.Time
	expireDate time.Time
}
type Entry struct {
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
	entries, exists := s.Stream[key]
	entry := Entry{id: id, pairs: pairs}
	if id == "*" {
		id, _ = genID(id, "")
	}
	if !exists || len(entries) == 0 || id == "0-0" {
		validID, validatedID, err := checkID(id, "0-0")
		entry.id = validatedID
		errmsg := "ERR The ID specified in XADD must be greater than 0-0"
		if !validID || err != nil {
			return fmt.Sprintf("-%s\r\n", errmsg)
		}
		s.Stream[key] = []Entry{entry}
	} else {
		lastID := findLastID(id, entries)
		validID, validatedID, err := checkID(id, lastID)
		errmsg := "ERR The ID specified in XADD is equal or smaller than the target stream top item"
		if !validID || err != nil {
			return fmt.Sprintf("-%s\r\n", errmsg)
		}
		entry.id = validatedID
		s.Stream[key] = append(entries, entry)
	}
	return fmt.Sprintf("+%s\r\n", entry.id)
}
func findLastID(id string, entries []Entry) string {
	time := strings.Split(id, "-")[0]
	for i := len(entries) - 1; i > 0; i-- {
		t := strings.Split(entries[i].id, "-")[0]
		if t == time {
			return entries[i].id
		}
	}
	return entries[len(entries)-1].id
}
func checkID(id string, lastId string) (bool, string, error) {
	if strings.Contains(id, "*") {
		newID, err := genID(id, lastId)
		if err != nil {
			return false, id, err
		}
		id = newID
	}
	msID, seqID, err := getNumValueOfID(id)
	if err != nil {
		return false, id, err
	}
	msLastID, seqLastID, err := getNumValueOfID(lastId)
	if err != nil {
		return false, id, err
	}
	// lastID is 0-0 when there is no entry eg. the given id is the firs id
	if lastId == "0-0" {
		return msID > 0 || seqID > 0, id, nil
	} else {
		if msID < msLastID {
			return false, id, nil
		} else {
			if msID == msLastID {
				return seqID > seqLastID, id, nil
			} else {
				return true, id, nil
			}
		}
	}
}
func genID(id string, lastID string) (string, error) {
	if len(id) == 1 {
		timestamp := time.Now().UnixMilli()
		return fmt.Sprintf("%d-%s", timestamp, "0"), nil
	}
	parts := strings.Split(id, "-")
	partsLast := strings.Split(lastID, "-")
	if parts[1] == "*" {
		lastSeq, err := strconv.Atoi(partsLast[1])
		if err != nil {
			return "", fmt.Errorf("coulndt parse last sequence number")
		}
		if lastSeq == 0 && partsLast[0] == "0" {
			parts[1] = "1"
		} else {
			if partsLast[0] == parts[0] {
				newSeq := lastSeq + 1
				parts[1] = fmt.Sprintf("%d", newSeq)
			} else {
				parts[1] = "0"
			}
		}
	}
	return fmt.Sprintf("%s-%s", parts[0], parts[1]), nil
}
func getNumValueOfID(id string) (int, int, error) {
	nums := strings.Split(id, "-")
	if len(nums) != 2 {
		return -1, -1, fmt.Errorf("id was given in a false format")
	}
	ms, err := strconv.Atoi(nums[0])
	if err != nil {
		return -1, -1, fmt.Errorf("ms couldnt be parsed  ")
	}
	seq, err := strconv.Atoi(nums[1])
	if err != nil {
		return -1, -1, fmt.Errorf("sequence num couldnt be parsed  ")
	}
	return ms, seq, nil
}

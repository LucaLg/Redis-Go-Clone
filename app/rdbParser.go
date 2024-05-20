package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"time"
)

type RdbParser struct {
	filename string
	dir      string
}
type KeyValPair struct {
	key string
	val Value
}

const (
	RedisMagicString         = "REDIS"
	RdbVersionNumber         = "0003"
	ExpireSeconds            = 0xFD
	ExpireMilliseconds       = 0xFC
	HashTableResize          = 0xFB
	EndOfFile                = 0xFF
	ExpireSecondsLength      = 4
	ExpireMillisecondsLength = 8
)

func (r *RdbParser) loadData(s *Server) {
	if err := r.ParseFile(s); err != nil {
		fmt.Printf("Error loading data: %v\n", err)
	}
}
func (r *RdbParser) ParseFile(s *Server) error {
	path := fmt.Sprintf("%s/%s", r.dir, r.filename)
	c, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("error occured reading rdb file from path %s", path)
	}
	if !r.isValid(c) {
		return fmt.Errorf("the given file is not a valid rdb file")
	}
	keyValPairs, err := r.readKeys(c)
	if err != nil {
		return fmt.Errorf("error occured while reading keys %w", err)
	}

	keys := []string{}
	for _, p := range keyValPairs {
		// fmt.Printf("Key %s val %s \n", p.key, p.val.value)
		s.Store.set(p.key, p.val)
		keys = append(keys, p.key)
	}
	return nil
}
func (r *RdbParser) readKeys(content []byte) ([]KeyValPair, error) {
	reader := bytes.NewReader(content)
	bufReader := bufio.NewReader(reader)
	_, err := bufReader.ReadString(HashTableResize)
	if err != nil {
		return nil, fmt.Errorf("error reading hash table resize: %w", err)
	}
	pairSec, err := bufReader.ReadBytes(0xFF)
	if err != nil {
		return nil, fmt.Errorf("error reading key-value pairs: %w", err)

	}
	i := 2
	hashLen := int(pairSec[0])
	expHashLen := int(pairSec[1])
	if expHashLen > 0 {
		expPairs, i := r.parseExpirePairs(pairSec, expHashLen)
		pairs := r.parsePairs(pairSec, hashLen-expHashLen, i)
		return append(expPairs, pairs...), nil
	} else {
		return r.parsePairs(pairSec, hashLen-expHashLen, i), nil
	}
}
func (r *RdbParser) parsePairs(pairSec []byte, pairLength int, i int) []KeyValPair {
	pairs := make([]KeyValPair, pairLength)
	keyIndex := 0
	for i < len(pairSec) && keyIndex < len(pairs) {
		if i >= len(pairSec) {
			break
		}
		valueType := int(pairSec[i])
		i++
		if valueType == 0 {
			keyLength := int(pairSec[i])
			i++
			key := string(pairSec[i : i+keyLength])
			i += keyLength
			valueLength := int(pairSec[i])
			i++
			value := string(pairSec[i : i+valueLength])
			pairs[keyIndex] = KeyValPair{key: key, val: Value{value: value}}
			i = i + valueLength
			keyIndex++
		}
	}
	return pairs
}
func (r *RdbParser) parseExpirePairs(keyString []byte, l int) ([]KeyValPair, int) {
	pairs := make([]KeyValPair, l)
	i := 2
	keyIndex := 0
	for keyIndex < l {
		expireTimestamp, x := parseTimestamp(keyString, i)
		i = x
		valueType := int(keyString[i])
		i++
		if valueType == 0 {
			// keyLength := int(keyString[i])
			// i++
			// key := string(keyString[i : i+keyLength])
			// i += keyLength
			// valueLength := int(keyString[i])
			// i++
			// v := Value{
			// 	value:      string(keyString[i : i+valueLength]),
			// 	expireDate: expireTimestamp,
			// 	savedAt:    time.Now(),
			// }
			var pair KeyValPair
			pair, i = parseKeyAndValue(keyString, i, expireTimestamp)
			if expireTimestamp.Before(time.Now()) {
				// fmt.Printf("skipped %s with val %s because expre %v ", key, v.value, v.expireDate)
				keyIndex++
				continue
			} else {
				pairs[keyIndex] = pair
			}
			keyIndex++
		}
	}
	return pairs, i
}
func parseKeyAndValue(pairSec []byte, i int, expTs time.Time) (KeyValPair, int) {
	keyLength := int(pairSec[i])
	i++
	key := string(pairSec[i : i+keyLength])
	i += keyLength
	valueLength := int(pairSec[i])
	i++
	v := Value{
		value:      string(pairSec[i : i+valueLength]),
		expireDate: expTs,
		savedAt:    time.Now(),
	}
	i += valueLength
	return KeyValPair{key: key, val: v}, i
}
func parseTimestamp(tsSlice []byte, i int) (time.Time, int) {
	var timestampLen int
	if tsSlice[i] == ExpireSeconds {
		timestampLen = ExpireSecondsLength
	} else {
		timestampLen = ExpireMillisecondsLength
	}
	i++
	timestamp := tsSlice[i : i+timestampLen]
	expiryTimeMs := binary.LittleEndian.Uint64(timestamp)
	var expireTimestamp time.Time
	if timestampLen == 4 {
		expireTimestamp = time.Unix(int64(expiryTimeMs), 0)
	} else {
		expireTimestamp = time.UnixMilli(int64(expiryTimeMs))
	}
	i += timestampLen
	return expireTimestamp, i
}

func (r *RdbParser) isValid(c []byte) bool {
	if len(c) < 9 {
		return false
	}

	//magic string
	if string(c[:5]) != RedisMagicString {
		return false
	}

	//version number
	version := string(c[5:9])
	for _, char := range version {
		if char < '0' || char > '9' {
			return false
		}
	}

	return true
}

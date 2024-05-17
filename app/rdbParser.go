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

func (r *RdbParser) loadData(s *Server) {
	r.ParseFile(s)
}
func (r *RdbParser) ParseFile(s *Server) (string, error) {
	path := fmt.Sprintf("%s/%s", r.dir, r.filename)
	c, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("error occured reading rdb file from path %s", path)
	}
	if !r.isValid(c) {
		return "", fmt.Errorf("the given file is not a valid rdb file")
	}
	keyValPairs, err := r.readKeys(c)
	if err != nil {
		return "", fmt.Errorf("error occured while reading keys %v", err)
	}

	keys := []string{}
	for _, p := range keyValPairs {
		fmt.Printf("Key %s val %s \n", p.key, p.val.value)
		s.Store.set(p.key, p.val)
		keys = append(keys, p.key)
	}
	return SliceToBulkString(keys), nil
}
func (r *RdbParser) readKeys(c []byte) ([]KeyValPair, error) {
	reader := bytes.NewReader(c)
	s := bufio.NewReader(reader)
	_, err := s.ReadString(0xFB)
	if err != nil {
		return []KeyValPair{}, err

	}
	pairSec, err := s.ReadBytes(0xFF)
	if err != nil {
		return []KeyValPair{}, err

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
		valueType := int(pairSec[i])
		i++
		if valueType == 0 {
			keyLength := int(pairSec[i])
			i++
			pairs[keyIndex].key = string(pairSec[i : i+keyLength])
			i += keyLength
			valueLength := int(pairSec[i])
			i++
			pairs[keyIndex].val.value = string(pairSec[i : i+valueLength])
			i = i + valueLength
			keyIndex++
		}
	}
	return pairs
}
func (r *RdbParser) parseExpirePairs(keyString []byte, l int) ([]KeyValPair, int) {
	pairs := make([]KeyValPair, l)
	i := 3
	keyIndex := 0
	for keyIndex < l {
		timestamp := keyString[i : i+8]
		expiryTimeMs := binary.LittleEndian.Uint64(timestamp)
		expireTimestamp := time.UnixMilli(int64(expiryTimeMs))
		i += 8
		valueType := int(keyString[i])
		i++
		if valueType == 0 {
			keyLength := int(keyString[i])
			i++
			key := string(keyString[i : i+keyLength])
			i += keyLength
			valueLength := int(keyString[i])
			i++
			v := Value{
				value:      string(keyString[i : i+valueLength]),
				expireDate: expireTimestamp,
				savedAt:    time.Now(),
			}
			if expireTimestamp.Before(time.Now()) {
				// fmt.Printf("skipped %s with val %s because expre %v ", key, v.value, v.expireDate)
				keyIndex++
				i = i + valueLength + 1
				continue
			} else {
				pairs[keyIndex].key = key
				pairs[keyIndex].val = v
			}
			i = i + valueLength + 1
			keyIndex++
		}
	}
	return pairs, i
}

func (r *RdbParser) isValid(c []byte) bool {
	return string(c[:5]) == "REDIS"
}

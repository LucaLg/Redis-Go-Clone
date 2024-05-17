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
	keyString, err := s.ReadBytes(0xFF)
	fmt.Println(keyString)
	if err != nil {
		return []KeyValPair{}, err

	}
	i := 2
	mapLength := int(keyString[0])
	expireMapLength := int(keyString[1])
	expirePairs := make([]KeyValPair, 0)
	if expireMapLength > 0 {
		expirePairs, i = r.parseExpirePairs(keyString, expireMapLength)
	}

	keys := make([]KeyValPair, mapLength-expireMapLength)
	keyIndex := 0
	for i < len(keyString) && keyIndex < len(keys) {
		valueType := int(keyString[i])
		i++
		if valueType == 0 {
			keyLength := int(keyString[i])
			i++
			keys[keyIndex].key = string(keyString[i : i+keyLength])
			i += keyLength
			valueLength := int(keyString[i])
			i++
			keys[keyIndex].val.value = string(keyString[i : i+valueLength])
			fmt.Println(keys)
			i = i + valueLength
			keyIndex++
		}
	}
	keys = append(keys, expirePairs...)
	return keys, nil
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

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
)

type RdbParser struct {
	filename string
	dir      string
}
type KeyValPair struct {
	key string
	val string
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
		s.Store.handleSet([]string{"set", p.key, p.val})
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
	if err != nil {
		return []KeyValPair{}, err

	}
	i := 2
	mapLength := int(keyString[0])
	keys := make([]KeyValPair, mapLength)
	keyIndex := 0
	fmt.Println(keyString)
	for i < len(keyString) {
		valueType := int(keyString[i])
		i++
		if valueType == 0 {
			keyLength := int(keyString[i])
			i++
			keys[keyIndex].key = string(keyString[i : i+keyLength])
			i += keyLength
			valueLength := int(keyString[i])
			i++
			keys[keyIndex].val = string(keyString[i : i+valueLength])
			i = i + valueLength + 1
			keyIndex++
		}
	}
	fmt.Println(keys)
	return keys, nil
}

func (r *RdbParser) isValid(c []byte) bool {
	return string(c[:5]) == "REDIS"
}

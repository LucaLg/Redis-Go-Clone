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

func (r *RdbParser) ParseFile() (string, error) {

	path := fmt.Sprintf("%s/%s", r.dir, r.filename)
	c, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("error occured reading rdb file from path %s", path)
	}
	if !r.isValid(c) {
		return "", fmt.Errorf("the given file is not a valid rdb file")
	}
	keys, err := r.readKeys(c)
	if err != nil {
		return "", fmt.Errorf("error occured while reading keys", err)
	}
	return SliceToBulkString(keys), nil
}
func (r *RdbParser) readKeys(c []byte) ([]string, error) {
	reader := bytes.NewReader(c)
	s := bufio.NewReader(reader)
	fileString, err := s.ReadString(0xFB)
	if err != nil {
		return []string{}, err

	}
	fmt.Println("File string", fileString)
	keyString, err := s.ReadBytes(0xFF)
	if err != nil {
		return []string{}, err

	}
	i := 2
	mapLength := int(keyString[0])
	keys := make([]string, mapLength)
	keyIndex := 0
	for i <= len(keyString) {
		valueType := int(keyString[i])
		i++
		if valueType == 0 {
			keyLength := int(keyString[i])
			i++
			keys[keyIndex] = string(keyString[i : i+keyLength])
			i += keyLength + 1
			valueLength := int(keyString[i])
			i = i + valueLength + 1
		}
		keyIndex++
	}
	fmt.Println("Key String", keys[0])
	return keys, nil
}

func (r *RdbParser) isValid(c []byte) bool {
	return string(c[:5]) == "REDIS"
}

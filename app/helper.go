package main

import (
	"fmt"
)

func StringToBulkString(input string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(input), input)
}
func SliceToBulkString(inputs []string) string {
	response := fmt.Sprintf("*%d\r\n", len(inputs))
	for _, input := range inputs {
		response = fmt.Sprintf("%s%s", response, StringToBulkString(input))
	}
	return response
}
func StreamEntriesToBulkString(input []Entry) string {
	response := fmt.Sprintf("*%d\r\n", len(input))
	for _, e := range input {
		pairStringArr := []string{}
		for _, p := range e.pairs {
			pairStringArr = append(pairStringArr, p.key)
			pairStringArr = append(pairStringArr, p.val)
		}
		pairsString := SliceToBulkString(pairStringArr)
		entryString := fmt.Sprintf("*2\r\n%s%s", StringToBulkString(e.id), pairsString)
		response = fmt.Sprintf("%s%s", response, entryString)
	}
	return response
}
func ByteToDigit(b byte) int {
	return int(b - '0')
}

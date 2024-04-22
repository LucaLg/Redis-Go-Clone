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
func ByteToDigit(b byte) int {
	return int(b - '0')
}

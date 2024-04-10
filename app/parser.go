package main

import (
	"fmt"
	"unicode"
)

func parse(input []byte) string {
	var length int
	i := 1
	for unicode.IsDigit(rune(input[i])) {
		length++
		i++
	}
	i += 4
	bulkString := input[i : i+length]
	fmt.Println("Bulk string: ", string(bulkString))

	return string(bulkString)
}

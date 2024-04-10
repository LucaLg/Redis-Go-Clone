package main

import "unicode"

func parse(input []byte) string {
	var length int
	i := 1
	for unicode.IsDigit(rune(input[i])) {
		length++
		i++
	}
	i += 4
	bulkString := input[i : i+length]
	return string(bulkString)
}

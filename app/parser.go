package main

import (
	"fmt"
	"strconv"
	"unicode"
)

func parse(input []byte) (string, error) {
	var arrayLength string
	i := 1
	for unicode.IsDigit(rune(input[i])) {
		arrayLength += string(input[i])
		i++
	}
	num, err := strconv.Atoi(arrayLength)
	if num == 1 {
		return "+PONG\r\n", nil
	}
	if err != nil {
		fmt.Println("Error parsing array length: ", err.Error())
		return "", err
	}

	return string(""), nil
}
func parseWord(word []byte) string {
	return string(word)
}

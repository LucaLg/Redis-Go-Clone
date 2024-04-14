package main

import (
	"fmt"
)

func transformStringToBulkString(input string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(input), input)
}
func transformStringSliceToBulkString(inputs []string) string {
	response := fmt.Sprintf("*%d\r\n", len(inputs))
	for _, input := range inputs {
		response = fmt.Sprintf("%s%s", response, transformStringToBulkString(input))
	}
	return response
}

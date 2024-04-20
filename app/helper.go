package main

import (
	"fmt"
)

func TransformStringToBulkString(input string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(input), input)
}
func TransformStringSliceToBulkString(inputs []string) string {
	response := fmt.Sprintf("*%d\r\n", len(inputs))
	for _, input := range inputs {
		response = fmt.Sprintf("%s%s", response, TransformStringToBulkString(input))
	}
	return response
}

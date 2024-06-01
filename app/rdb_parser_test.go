package main

import (
	"testing"
	"time"
)

func TestRdbParser(t *testing.T) {
	t.Run("Test if given input slice is valid rdbFile", func(t *testing.T) {

	})
	t.Run("Is a valid rdb file", func(t *testing.T) {
		parser := &RdbParser{}
		tests := []struct {
			name  string
			input []byte
			want  bool
		}{
			{
				name:  "Valid input with correct magic string and version",
				input: []byte("REDIS0006"),
				want:  true,
			},
			{
				name:  "Invalid input with incorrect magic string",
				input: []byte("REDIX0006"),
				want:  false,
			},
			{
				name:  "Invalid input with correct magic string but non-numeric version",
				input: []byte("REDIS00a6"),
				want:  false,
			},
			{
				name:  "Invalid input with correct magic string but short version",
				input: []byte("REDIS006"),
				want:  false,
			},
			{
				name:  "Invalid input with empty byte slice",
				input: []byte(""),
				want:  false,
			},
			{
				name:  "Invalid input with less than 9 characters",
				input: []byte("REDIS00"),
				want:  false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := parser.isValid(tt.input); got != tt.want {
					t.Errorf("isValid() = %v, want %v", got, tt.want)
				}
			})
		}
	})
	t.Run("Timestamp parsing", func(t *testing.T) {
		tests := []struct {
			name           string
			tsSlice        []byte
			expectedTime   time.Time
			expectedOffset int
		}{
			{
				name:           "Parse timestamp with seconds",
				tsSlice:        []byte{0xFD, 0x00, 0x00, 0x00, 0x00},
				expectedTime:   time.Unix(0, 0),
				expectedOffset: 5,
			},
			{
				name:           "Parse timestamp with milliseconds",
				tsSlice:        []byte{0xFC, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
				expectedTime:   time.UnixMilli(0),
				expectedOffset: 9,
			},
		}
		for _, tt := range tests {
			actualTime, actualOffset, err := parseTimestamp(tt.tsSlice, 0)
			if err != nil {
				t.Errorf("parseTimestamp() returned error: %v", err)
			}
			if !actualTime.Equal(tt.expectedTime) {
				t.Errorf("parseTimestamp() returned unexpected time. Got %v, want %v", actualTime, tt.expectedTime)
			}
			if actualOffset != tt.expectedOffset {
				t.Errorf("parseTimestamp() returned unexpected offset. Got %v, want %v", actualOffset, tt.expectedOffset)
			}
		}

	})

}

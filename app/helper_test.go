package main

import (
	"testing"
)

func TestHelper(t *testing.T) {
	t.Run("Test bulk transform", func(t *testing.T) {
		want := "$11\r\nrole:master\r\n"
		got := transformStringToBulkString("role:master")
		if want != got {
			t.Errorf("Got %s but wanted %s", got, want)
		}

	})
	t.Run("Test string[] to BulkString", func(t *testing.T) {

		want := "*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n"
		got := transformStringSliceToBulkString([]string{"GET", "mykey"})
		if want != got {
			t.Errorf("Got %s but wanted %s", got, want)
		}
	})

}

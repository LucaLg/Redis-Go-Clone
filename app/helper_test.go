package main

import (
	"testing"
)

func TestHelper(t *testing.T) {
	t.Run("Test bulk transform", func(t *testing.T) {
		want := "$11\r\nrole:master\r\n"
		got := StringToBulkString("role:master")
		if want != got {
			t.Errorf("Got %s but wanted %s", got, want)
		}

	})
	t.Run("Test string[] to BulkString", func(t *testing.T) {

		want := "*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n"
		got := SliceToBulkString([]string{"GET", "mykey"})
		if want != got {
			t.Errorf("Got %s but wanted %s", got, want)
		}
	})
	t.Run("Test stream entry to bulk string", func(t *testing.T) {
		want := "*2\r\n*2\r\n$15\r\n1526985054069-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n*2\r\n$15\r\n1526985054079-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n37\r\n$8\r\nhumidity\r\n$2\r\n94\r\n"
		input := []Entry{{id: "1526985054069-0", pairs: []EntryPair{{key: "temperature", val: "36"}, {key: "humidity", val: "95"}}}, {id: "1526985054079-0", pairs: []EntryPair{{key: "temperature", val: "37"}, {key: "humidity", val: "94"}}}}
		got := StreamEntriesToBulkString(input)
		if want != got {
			t.Errorf("Got %s but wanted %s", got, want)
		}
	})

}

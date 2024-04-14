package main

import (
	"fmt"
	"testing"
)

func TestParsetwo(t *testing.T) {
	pingCommand := []byte("*1\r\n$4\r\nping\r\n")
	echoCommand := []byte("*2\r\n$4\r\necho\r\n$5\r\nhello\r\n")
	setCommand := []byte("*3\r\n$3\r\nset\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n")
	getCommand := []byte("*2\r\n$3\r\nget\r\n$5\r\nmykey\r\n")

	gotPing, err := parse(pingCommand)
	if err != nil {
		t.Fatalf("Test failed")
	}
	wantPing := "+PONG\r\n"
	if gotPing != wantPing {
		t.Fatalf("Test failed because %s not equal to %s", gotPing, wantPing)
	}

	gotEcho, err := parse(echoCommand)
	if err != nil {
		t.Fatalf("Test failed")
	}
	wantEcho := fmt.Sprintf("+%s\r\n", "hello")
	if gotEcho != wantEcho {
		t.Fatalf("Test failed because %s not equal to %s", gotPing, wantEcho)
	}

	gotSet, err := parse(setCommand)
	if err != nil {
		t.Fatalf("Test failed")
	}
	wantSet := "+OK\r\n"
	if gotSet != wantSet {
		t.Fatalf("Test failed because %s not equal to %s", gotSet, wantSet)
	}

	gotGet, err := parse(getCommand)
	if err != nil {
		t.Fatalf("Test failed")
	}
	wantGet := "$7\r\nmyvalue\r\n"
	if gotGet != wantGet {
		t.Fatalf("Test failed because %s not equal to %s", gotGet, wantGet)
	}
}

package main

import (
	"fmt"
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	t.Run("Ping command", func(t *testing.T) {
		pingCommand := []byte("*1\r\n$4\r\nping\r\n")
		gotPing, err := parse(pingCommand)
		if err != nil {
			t.Fatalf("Test failed")
		}
		wantPing := "+PONG\r\n"
		if gotPing != wantPing {
			t.Fatalf("Test failed because %s not equal to %s", gotPing, wantPing)
		}
	})

	t.Run("Echo command", func(t *testing.T) {
		echoCommand := []byte("*2\r\n$4\r\necho\r\n$5\r\nhello\r\n")
		gotEcho, err := parse(echoCommand)
		if err != nil {
			t.Fatalf("Test failed")
		}
		wantEcho := fmt.Sprintf("+%s\r\n", "hello")
		if gotEcho != wantEcho {
			t.Fatalf("Test failed because %s not equal to %s", gotEcho, wantEcho)
		}
	})

	t.Run("Set command", func(t *testing.T) {
		setCommand := []byte("*3\r\n$3\r\nset\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n")
		gotSet, err := parse(setCommand)
		if err != nil {
			t.Fatalf("Test failed")
		}
		wantSet := "+OK\r\n"
		if gotSet != wantSet {
			t.Fatalf("Test failed because %s not equal to %s", gotSet, wantSet)
		}
	})

	t.Run("SetArgs and GetPx commands", func(t *testing.T) {
		setArgsCommand := []byte("*5\r\n$3\r\nset\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nPX\r\n$3\r\n100\r\n")
		gotSetArgs, err := parse(setArgsCommand)
		if err != nil {
			t.Fatalf("Test failed")
		}
		wantSetArgs := "+OK\r\n"
		if gotSetArgs != wantSetArgs {
			t.Fatalf("Test failed because %s not equal to %s", gotSetArgs, wantSetArgs)
		}
		time.Sleep(200 * time.Millisecond)
		getPxCommand := []byte("*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n")
		gotGetPx, err := parse(getPxCommand)
		if err != nil {
			t.Fatalf("Test failed")
		}
		wantGetPx := "$-1\r\n"
		if gotGetPx != wantGetPx {
			t.Fatalf("Test failed because %s not equal to %s", gotGetPx, wantGetPx)
		}
	})

	t.Run("Get command", func(t *testing.T) {
		getCommand := []byte("*2\r\n$3\r\nget\r\n$5\r\nmykey\r\n")
		gotGet, err := parse(getCommand)
		if err != nil {
			t.Fatalf("Test failed")
		}
		wantGet := "$7\r\nmyvalue\r\n"
		if gotGet != wantGet {
			t.Fatalf("Test failed because %s not equal to %s", gotGet, wantGet)
		}
	})
	t.Run("Test info command ", func(t *testing.T) {
		infoCmd := transformStringSliceToBulkString([]string{"info", "replication"})
		want := "$11\r\nrole:master\r\n"
		got, err := parse([]byte(infoCmd))
		if err != nil {
			t.Fatalf("Test failed")
		}
		if got != want {
			t.Fatalf("Test failed because %s not equal to %s", got, want)
		}

	})
}

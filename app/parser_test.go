package main

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	server := &Server{
		// Initialize other fields if necessary
		Parser: Parser{},
	}
	t.Run("Ping command", func(t *testing.T) {
		pingCommand := []byte("*1\r\n$4\r\nping\r\n")
		gotPing, err := server.Parser.Parse(pingCommand, server)
		if err != nil {
			t.Fatalf("Test failed")
		}
		wantPing := []string{"ping"}
		if !reflect.DeepEqual(gotPing, wantPing) {
			t.Fatalf("Test failed because %v not equal to %v", gotPing, wantPing)
		}
	})

	t.Run("Echo command", func(t *testing.T) {
		echoCommand := []byte("*2\r\n$4\r\necho\r\n$5\r\nhello\r\n")
		gotEcho, err := server.Parser.Parse(echoCommand, server)
		if err != nil {
			t.Fatalf("Test failed")
		}
		wantEcho := []string{"echo", "hello"}
		if !reflect.DeepEqual(wantEcho, gotEcho) {
			t.Fatalf("Test failed because %s not equal to %s", gotEcho, wantEcho)
		}
	})

	t.Run("Set command", func(t *testing.T) {
		setCommand := []byte("*3\r\n$3\r\nset\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n")
		gotSet, err := server.Parser.Parse(setCommand, server)
		if err != nil {
			t.Fatalf("Test failed")
		}
		wantSet := []string{"set", "mykey", "myvalue"}
		if !reflect.DeepEqual(gotSet, wantSet) {
			t.Fatalf("Test failed because %v not equal to %v", gotSet, wantSet)
		}

		t.Run("SetArgs and GetPx commands", func(t *testing.T) {
			setArgsCommand := []byte("*5\r\n$3\r\nset\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nPX\r\n$3\r\n100\r\n")
			gotSetArgs, err := server.Parser.Parse(setArgsCommand, server)
			if err != nil {
				t.Fatalf("Test failed")
			}
			wantSetArgs := []string{"set", "foo", "bar", "px", "100"}
			if !reflect.DeepEqual(gotSetArgs, wantSetArgs) {
				t.Fatalf("Test failed because %v not equal to %v", gotSetArgs, wantSetArgs)
			}
			time.Sleep(200 * time.Millisecond)
			getPxCommand := []byte("*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n")
			gotGetPx, err := server.Parser.Parse(getPxCommand, server)
			if err != nil {
				t.Fatalf("Test failed")
			}
			wantGetPx := []string{"get", "foo"}
			if !reflect.DeepEqual(gotGetPx, wantGetPx) {
				t.Fatalf("Test failed because %v not equal to %v", gotGetPx, wantGetPx)
			}
		})

		t.Run("Get command", func(t *testing.T) {
			getCommand := []byte("*2\r\n$3\r\nget\r\n$5\r\nmykey\r\n")
			gotGet, err := server.Parser.Parse(getCommand, server)
			if err != nil {
				t.Fatalf("Test failed")
			}
			wantGet := []string{"get", "mykey"}
			if !reflect.DeepEqual(gotGet, wantGet) {
				t.Fatalf("Test failed because %v not equal to %v", gotGet, wantGet)
			}
		})
		t.Run("Parse length of word or array", func(t *testing.T) {

		})
		t.Run("Test info command ", func(t *testing.T) {
			infoCmd := SliceToBulkString([]string{"info", "replication"})
			// role := fmt.Sprintf("role:%s", status)
			// replid := fmt.Sprintf("master_replid:%s", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb")
			// offset := fmt.Sprintf("master_repl_offset:%s", "0")
			// info := fmt.Sprintf("%s\n%s\n%s", role, replid, offset)
			want := []string{"info", "replication"}
			got, err := server.Parser.Parse([]byte(infoCmd), server)
			if err != nil {
				t.Fatalf("Test failed")
			}
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("Test failed because %s not equal to %s", got, want)
			}

		})
		t.Run("Test replication parser", func(t *testing.T) {
			input := []byte("*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$1\r\n1\r\n*3\r\n$3\r\nset\r\n$3\r\nbar\r\n$1\r\n1\r\n")
			got, err := server.Parser.parseReplication(input, server)
			if err != nil {
				t.Fatalf("Test failed coulndt parse input")
			}
			cmdOne := []string{"set", "foo", "1"}
			cmdTwo := []string{"set", "bar", "1"}
			want := [][]string{cmdOne, cmdTwo}
			fmt.Println("Got", got)
			fmt.Println("Want", want)
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("replication parsed false got %s and wanted %s", got, want)
			}

		})

	})
}

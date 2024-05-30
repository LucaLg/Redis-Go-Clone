package main

import (
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
)

func TestCommand(t *testing.T) {
	s := Server{} // replace with your server initialization code
	t.Run("Test psync answer", func(t *testing.T) {
		var wg sync.WaitGroup
		serverConn, clientConn := net.Pipe()

		// Reading goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 2048)
			var total []byte
			for {
				i, err := clientConn.Read(buf)
				if err != nil {
					if err == io.EOF {
						break // break the loop when no more data to read
					}
					t.Fatalf("unexpected error: %v", err)
				}
				total = append(total, buf[:i]...)
			}
			fmt.Println(string(total))
		}()

		// Writing goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			psyncRes, err := s.handlePsync([]string{}, serverConn)
			if err != nil {
				t.Fatalf("Error while writing")
			}
			serverConn.Write([]byte(psyncRes))
			serverConn.Close() // close the connection when done writing
		}()

		wg.Wait() // wait for all goroutines to finish
	})

}

package main

import (
	"net"
	"testing"
)

func TestServer(t *testing.T) {

	t.Run("Test Master start", func(t *testing.T) {

		masterServer := Server{}
		l, err := masterServer.start()
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		defer l.Close()
		if masterServer.status != "master" {
			t.Fatalf("Master status is not master")
		}
		expectedAddr := &net.TCPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: 6379,
		}

		if l.Addr().String() != expectedAddr.String() {
			t.Fatalf("Master started on the wrong address got  %s but wanted %s", l.Addr().String(), expectedAddr.String())
		}
	})

	t.Run("Test handshake", func(t *testing.T) {
		masterServer := Server{}
		lm, err := masterServer.start()
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		defer lm.Close()

		replication := Server{
			addr: "localhost:6400",
			replication: Replication{
				HOST_IP:   "127.0.0.1",
				HOST_PORT: "6379",
			},
		}

		replication.handshake()
		replication.status = "slave"
		if replication.status != "slave" {
			t.Fatalf("Replication status is wrong got %s but wanted %s", replication.status, "slave")
		}
	})
}

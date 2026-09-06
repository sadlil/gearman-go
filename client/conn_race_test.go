package client

import (
	"net"
	"sync"
	"testing"
)

// TestCloseDuringReadLoopIsRaceFree guards the synchronisation around
// Client.conn and Client.rw.
//
// New() starts readLoop() in its own goroutine. That loop reads client.conn in
// its loop condition and reassigns conn/rw when it self-redials after an
// unexpected read error. Close() writes client.conn = nil. Without
// synchronisation on the reading side these are concurrent accesses to the
// same pointer words, which the race detector reports.
//
// Run with -race; without it this test always passes and proves nothing.
func TestCloseDuringReadLoopIsRaceFree(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	// The server accepts and holds connections open without replying, so
	// readLoop sits in its blocking read when Close() lands — the situation a
	// caller creates by closing a client while it is still connected.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var conns []net.Conn
		defer func() {
			for _, c := range conns {
				c.Close()
			}
		}()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conns = append(conns, conn)
		}
	}()

	for i := 0; i < 20; i++ {
		c, err := New(Network, ln.Addr().String())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		c.ErrorHandler = func(error) {}
		if err := c.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	ln.Close()
	wg.Wait()
}

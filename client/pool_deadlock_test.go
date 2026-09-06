package client

import (
	"net"
	"testing"
	"time"
)

// jobCreatedServer acknowledges the option and answers every SUBMIT_JOB* with a
// JOB_CREATED, which is all Pool.Do and Pool.DoBg need to return.
func jobCreatedServer(conn net.Conn, dataType uint32, data []byte) bool {
	switch dataType {
	case dtOptionReq:
		conn.Write(resPacket(dtOptionRes, data))
	case dtSubmitJob, dtSubmitJobBg, dtSubmitJobHigh, dtSubmitJobHighBg,
		dtSubmitJobLow, dtSubmitJobLowBg:
		conn.Write(resPacket(dtJobCreated, []byte("H:localhost:1")))
	}
	return true
}

// TestPoolDoDoesNotDeadlock guards the locking in Pool.Do and Pool.DoBg.
//
// Both used to take the client's embedded mutex and then call through to
// (*Client).do, which takes that same mutex. sync.Mutex is not reentrant, so
// the call never returned. The tests that would have caught it are behind
// -integration and would have hung rather than failed.
//
// Each call runs in its own goroutine so a regression fails this test instead
// of blocking the whole suite.
func TestPoolDoDoesNotDeadlock(t *testing.T) {
	s := newFakeServer(t)
	defer s.close()
	s.handle = jobCreatedServer

	p := NewPool()
	if err := p.Add(Network, s.addr(), 1); err != nil {
		t.Fatalf("Pool.Add: %v", err)
	}

	for _, tc := range []struct {
		name string
		call func() (string, string, error)
	}{
		{"Do", func() (string, string, error) {
			return p.Do("ToUpper", []byte("x"), JobNormal, func(*Response) {})
		}},
		{"DoBg", func() (string, string, error) {
			return p.DoBg("ToUpper", []byte("x"), JobNormal)
		}},
	} {
		type result struct {
			handle string
			err    error
		}
		done := make(chan result, 1)
		go func() {
			_, handle, err := tc.call()
			done <- result{handle, err}
		}()

		select {
		case r := <-done:
			if r.err != nil {
				t.Errorf("Pool.%s: %v", tc.name, r.err)
			}
			if r.handle != "H:localhost:1" {
				t.Errorf("Pool.%s handle = %q, want %q", tc.name, r.handle, "H:localhost:1")
			}
		case <-time.After(3 * time.Second):
			// Do not close the pool: the stuck goroutine still holds the
			// client mutex, so Close would block too.
			t.Fatalf("Pool.%s did not return: the client mutex is taken twice", tc.name)
		}
	}
	p.Close()
}

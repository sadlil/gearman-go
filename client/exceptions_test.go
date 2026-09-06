package client

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The tests in this file cover the "exceptions" option handshake and the
// WORK_EXCEPTION path it unlocks. They talk to a scripted fake job server over
// a real socket rather than to gearmand, so they run without -integration.

// fakeServer is a minimal job server: it accepts connections, decodes whole
// packets and hands them to a per-connection script.
type fakeServer struct {
	t  *testing.T
	ln net.Listener

	// handle is called for every packet a client sends. It writes whatever the
	// scripted server should reply. Returning false closes the connection.
	handle func(conn net.Conn, dataType uint32, data []byte) bool

	mu       sync.Mutex
	received []uint32 // packet types seen, in order, across all connections
	conns    []net.Conn
	wg       sync.WaitGroup
}

func newFakeServer(t *testing.T) *fakeServer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &fakeServer{t: t, ln: ln}
	s.wg.Add(1)
	go s.accept()
	return s
}

func (s *fakeServer) addr() string { return s.ln.Addr().String() }

func (s *fakeServer) accept() {
	defer s.wg.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns = append(s.conns, conn)
		s.mu.Unlock()
		s.wg.Add(1)
		go s.serve(conn)
	}
}

func (s *fakeServer) serve(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	head := make([]byte, minPacketLength)
	for {
		if _, err := io.ReadFull(conn, head); err != nil {
			return
		}
		dataType := binary.BigEndian.Uint32(head[4:8])
		size := int(binary.BigEndian.Uint32(head[8:12]))
		data := make([]byte, size)
		if _, err := io.ReadFull(conn, data); err != nil {
			return
		}
		s.mu.Lock()
		s.received = append(s.received, dataType)
		s.mu.Unlock()
		if s.handle != nil && !s.handle(conn, dataType, data) {
			return
		}
	}
}

// packetTypes returns the packet types the server has seen so far.
func (s *fakeServer) packetTypes() []uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint32(nil), s.received...)
}

func (s *fakeServer) close() {
	s.ln.Close()
	s.mu.Lock()
	for _, c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

// resPacket encodes a \x00RES packet the way a job server would.
func resPacket(dataType uint32, data []byte) []byte {
	buf := make([]byte, minPacketLength+len(data))
	copy(buf[:4], resStr)
	binary.BigEndian.PutUint32(buf[4:8], dataType)
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(data)))
	copy(buf[minPacketLength:], data)
	return buf
}

// waitFor polls until cond holds or the deadline passes. The handshake is
// asynchronous by design, so tests cannot simply read the state after New().
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// echoServer replies to ECHO_REQ and acknowledges the option, the behaviour of
// a normal gearmand.
func echoServer(conn net.Conn, dataType uint32, data []byte) bool {
	switch dataType {
	case dtOptionReq:
		conn.Write(resPacket(dtOptionRes, data))
	case dtEchoReq:
		conn.Write(resPacket(dtEchoRes, data))
	}
	return true
}

func TestOptionReqEncode(t *testing.T) {
	// \x00REQ, type 26, length 10, then the bare option name -- no trailing
	// NULL byte, per the protocol.
	want := []byte("\x00REQ\x00\x00\x00\x1a\x00\x00\x00\x0aexceptions")
	if got := getOptionReq(optionExceptions).Encode(); !bytes.Equal(got, want) {
		t.Errorf("OPTION_REQ encoded as %q, want %q", got, want)
	}
}

func TestDecodeOptionRes(t *testing.T) {
	raw := resPacket(dtOptionRes, []byte(optionExceptions))
	resp, l, err := decodeResponse(raw)
	if err != nil {
		t.Fatalf("decodeResponse: %v", err)
	}
	if l != len(raw) {
		t.Errorf("consumed %d bytes, want %d", l, len(raw))
	}
	if resp.DataType != dtOptionRes {
		t.Errorf("DataType = %d, want %d", resp.DataType, dtOptionRes)
	}
	// OPTION_RES has a single argument and no job handle.
	if resp.Handle != "" {
		t.Errorf("Handle = %q, want empty", resp.Handle)
	}
	if string(resp.Data) != optionExceptions {
		t.Errorf("Data = %q, want %q", resp.Data, optionExceptions)
	}
}

func TestDecodeWorkException(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
	}{
		{"with payload", "boom"},
		{"empty payload", ""},
	} {
		raw := resPacket(dtWorkException, []byte("H:localhost:1\x00"+tc.payload))
		resp, _, err := decodeResponse(raw)
		if err != nil {
			t.Fatalf("%s: decodeResponse: %v", tc.name, err)
		}
		if resp.Handle != "H:localhost:1" {
			t.Errorf("%s: Handle = %q, want %q", tc.name, resp.Handle, "H:localhost:1")
		}
		data, err := resp.Result()
		if err != ErrWorkException {
			t.Errorf("%s: Result err = %v, want ErrWorkException", tc.name, err)
		}
		if data == nil {
			t.Errorf("%s: Result data is nil, want non-nil", tc.name)
		}
		if string(data) != tc.payload {
			t.Errorf("%s: Result data = %q, want %q", tc.name, data, tc.payload)
		}
		// Result must not disturb the handle the decoder filled in.
		if resp.Handle != "H:localhost:1" {
			t.Errorf("%s: Result clobbered Handle to %q", tc.name, resp.Handle)
		}
	}
}

// TestResultWorkFailKeepsHandle guards against Result() overwriting the handle
// with the (nil) Data of a WORK_FAIL, which emptied it.
func TestResultWorkFailKeepsHandle(t *testing.T) {
	raw := resPacket(dtWorkFail, []byte("H:localhost:7"))
	resp, _, err := decodeResponse(raw)
	if err != nil {
		t.Fatalf("decodeResponse: %v", err)
	}
	data, err := resp.Result()
	if err != ErrWorkFail {
		t.Errorf("Result err = %v, want ErrWorkFail", err)
	}
	if data != nil {
		t.Errorf("Result data = %q, want nil", data)
	}
	if resp.Handle != "H:localhost:7" {
		t.Errorf("Handle = %q after Result, want %q", resp.Handle, "H:localhost:7")
	}
}

// TestHandshakeFirstPacketIsOptionReq pins the ordering the whole design rests
// on: the OPTION_REQ must reach the server before anything else, otherwise a
// job submitted right after New() could still be running without the option.
func TestHandshakeFirstPacketIsOptionReq(t *testing.T) {
	s := newFakeServer(t)
	defer s.close()
	s.handle = echoServer

	c, err := New(Network, s.addr())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
	c.ErrorHandler = func(error) {}

	if _, err := c.Echo([]byte("hi")); err != nil {
		t.Fatalf("Echo: %v", err)
	}
	got := s.packetTypes()
	if len(got) == 0 || got[0] != dtOptionReq {
		t.Fatalf("server saw %v, want OPTION_REQ (%d) first", got, dtOptionReq)
	}
	waitFor(t, "exceptions to be enabled", c.ExceptionsEnabled)
}

// TestHandshakeRefusedDegradesSilently covers a job server that rejects the
// option. The client must stay usable and must not report the refusal to the
// user's ErrorHandler -- there is nothing they can do about it.
func TestHandshakeRefusedDegradesSilently(t *testing.T) {
	s := newFakeServer(t)
	defer s.close()
	s.handle = func(conn net.Conn, dataType uint32, data []byte) bool {
		switch dataType {
		case dtOptionReq:
			conn.Write(resPacket(dtError, []byte("unknown_option\x00Server does not recognize given option")))
		case dtEchoReq:
			conn.Write(resPacket(dtEchoRes, data))
		}
		return true
	}

	c, err := New(Network, s.addr())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	var mu sync.Mutex
	var errs []error
	c.ErrorHandler = func(e error) {
		mu.Lock()
		errs = append(errs, e)
		mu.Unlock()
	}

	// Echo round-trips only after the ERROR has been processed, so by the time
	// it returns the ErrorHandler would have fired if it were going to.
	echo, err := c.Echo([]byte("still alive"))
	if err != nil {
		t.Fatalf("Echo after refused option: %v", err)
	}
	if string(echo) != "still alive" {
		t.Errorf("Echo = %q, want %q", echo, "still alive")
	}
	if c.ExceptionsEnabled() {
		t.Error("ExceptionsEnabled() is true after the server refused the option")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(errs) != 0 {
		t.Errorf("ErrorHandler called with %v, want no calls", errs)
	}
}

// TestHandshakeIgnoredByServer covers a server that neither acknowledges nor
// rejects the OPTION_REQ.
func TestHandshakeIgnoredByServer(t *testing.T) {
	s := newFakeServer(t)
	defer s.close()
	s.handle = func(conn net.Conn, dataType uint32, data []byte) bool {
		if dataType == dtEchoReq {
			conn.Write(resPacket(dtEchoRes, data))
		}
		return true
	}

	c, err := New(Network, s.addr())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
	c.ErrorHandler = func(error) {}

	if _, err := c.Echo([]byte("hi")); err != nil {
		t.Fatalf("Echo: %v", err)
	}
	if c.ExceptionsEnabled() {
		t.Error("ExceptionsEnabled() is true although the server never answered")
	}
}

// TestOptOut checks that DefaultExceptions = false puts no OPTION_REQ on the
// wire at all.
func TestOptOut(t *testing.T) {
	s := newFakeServer(t)
	defer s.close()
	s.handle = echoServer

	DefaultExceptions = false
	defer func() { DefaultExceptions = true }()

	c, err := New(Network, s.addr())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
	c.ErrorHandler = func(error) {}

	if _, err := c.Echo([]byte("hi")); err != nil {
		t.Fatalf("Echo: %v", err)
	}
	for _, dt := range s.packetTypes() {
		if dt == dtOptionReq {
			t.Fatalf("server saw an OPTION_REQ although DefaultExceptions was false")
		}
	}
	if c.ExceptionsEnabled() {
		t.Error("ExceptionsEnabled() is true although the option was never requested")
	}
}

// TestWorkExceptionReachesHandler is the end-to-end path: a worker's exception
// arrives at the handler registered with Do, with its payload intact.
func TestWorkExceptionReachesHandler(t *testing.T) {
	s := newFakeServer(t)
	defer s.close()

	const handle = "H:localhost:42"
	s.handle = func(conn net.Conn, dataType uint32, data []byte) bool {
		switch dataType {
		case dtOptionReq:
			conn.Write(resPacket(dtOptionRes, data))
		case dtSubmitJob:
			conn.Write(resPacket(dtJobCreated, []byte(handle)))
			conn.Write(resPacket(dtWorkException, []byte(handle+"\x00it went wrong")))
			// gearmand treats WORK_EXCEPTION as terminal, but an older worker
			// may still send a WORK_FAIL behind it. The client must have
			// dropped the handler by then and not deliver it twice.
			conn.Write(resPacket(dtWorkFail, []byte(handle)))
		}
		return true
	}

	c, err := New(Network, s.addr())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
	c.ErrorHandler = func(error) {}

	var mu sync.Mutex
	var got []*Response
	done := make(chan struct{}, 1)
	h := func(resp *Response) {
		mu.Lock()
		got = append(got, resp)
		mu.Unlock()
		select {
		case done <- struct{}{}:
		default:
		}
	}

	if _, err := c.Do("Sleep", []byte("x"), JobNormal, h); err != nil {
		t.Fatalf("Do: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler was never called for the WORK_EXCEPTION")
	}
	// Give a stray second delivery a chance to show up before counting.
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("handler called %d times, want 1", len(got))
	}
	resp := got[0]
	if resp.DataType != WorkException {
		t.Errorf("DataType = %d, want WorkException (%d)", resp.DataType, WorkException)
	}
	if resp.Handle != handle {
		t.Errorf("Handle = %q, want %q", resp.Handle, handle)
	}
	data, err := resp.Result()
	if err != ErrWorkException {
		t.Errorf("Result err = %v, want ErrWorkException", err)
	}
	if string(data) != "it went wrong" {
		t.Errorf("Result data = %q, want %q", data, "it went wrong")
	}
}

// TestOptionReplayedAfterReconnect checks that the option is requested again
// after a redial -- the job server keeps it per connection, so skipping the
// replay would silently lose exceptions. The first connection is killed
// mid-packet, which also exercises the leftdata reset: without it the stale
// half packet would corrupt the OPTION_RES on the new connection.
func TestOptionReplayedAfterReconnect(t *testing.T) {
	s := newFakeServer(t)
	defer s.close()

	var mu sync.Mutex
	conns := 0
	s.handle = func(conn net.Conn, dataType uint32, data []byte) bool {
		if dataType != dtOptionReq {
			if dataType == dtEchoReq {
				conn.Write(resPacket(dtEchoRes, data))
			}
			return true
		}
		mu.Lock()
		conns++
		first := conns == 1
		mu.Unlock()
		if first {
			// Acknowledge, then send half a packet and hang up, so the client
			// is left holding an incomplete buffer when it redials.
			conn.Write(resPacket(dtOptionRes, data))
			conn.Write([]byte("\x00RES\x00\x00\x00\x08\x00\x00\x00\x20trunc"))
			return false
		}
		conn.Write(resPacket(dtOptionRes, data))
		return true
	}

	c, err := New(Network, s.addr())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
	// Deliberately no ErrorHandler: the dropped connection makes readLoop call
	// client.err() from its own goroutine, and assigning the field after New()
	// -- as the README tells users to -- races with that read. That race is
	// pre-existing and unrelated to this test; a nil handler is a no-op.

	waitFor(t, "a second connection carrying an OPTION_REQ", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return conns >= 2
	})
	// The option must be acknowledged again on the new connection, and the
	// client must still work on it.
	waitFor(t, "exceptions to be enabled again", c.ExceptionsEnabled)
	if _, err := c.Echo([]byte("after reconnect")); err != nil {
		t.Fatalf("Echo after reconnect: %v", err)
	}
}

// TestProcessLoopOptionOrdering drives processLoop directly, without a socket,
// to pin the marker logic including the case that an unrelated ERROR must
// still reach the ErrorHandler.
func TestProcessLoopOptionOrdering(t *testing.T) {
	for _, tc := range []struct {
		name      string
		marker    bool
		reply     *Response
		wantState int32
		wantErrs  int
	}{
		{
			name: "acknowledged", marker: true,
			reply:     &Response{DataType: dtOptionRes, Data: []byte(optionExceptions)},
			wantState: exceptionsOn, wantErrs: 0,
		},
		{
			name: "refused", marker: true,
			reply:     &Response{DataType: dtError, Data: []byte("unknown_option\x00nope")},
			wantState: exceptionsRefused, wantErrs: 0,
		},
		{
			name: "ignored", marker: true,
			reply:     &Response{DataType: dtJobCreated, Handle: "H:1"},
			wantState: exceptionsRefused, wantErrs: 0,
		},
		{
			name: "unrelated error still reported", marker: false,
			reply:     &Response{DataType: dtError, Data: []byte("some_code\x00some text")},
			wantState: exceptionsPending, wantErrs: 1,
		},
	} {
		c := &Client{
			innerHandler: newResponseHandlerMap(),
			in:           make(chan *Response, queueSize),
		}
		var mu sync.Mutex
		var errs []error
		c.ErrorHandler = func(e error) {
			mu.Lock()
			errs = append(errs, e)
			mu.Unlock()
		}
		stopped := make(chan struct{})
		go func() {
			c.processLoop()
			close(stopped)
		}()

		if tc.marker {
			c.in <- &Response{DataType: dtOptionSent}
		}
		c.in <- tc.reply
		close(c.in)
		<-stopped

		if got := atomic.LoadInt32(&c.exceptionsState); got != tc.wantState {
			t.Errorf("%s: exceptionsState = %d, want %d", tc.name, got, tc.wantState)
		}
		mu.Lock()
		if len(errs) != tc.wantErrs {
			t.Errorf("%s: ErrorHandler called %d times (%v), want %d", tc.name, len(errs), errs, tc.wantErrs)
		}
		mu.Unlock()
	}
}

// TestNewFailsWhenUnreachable covers connect()'s dial error reaching the
// caller: New must report it rather than hand back a half-built client.
func TestNewFailsWhenUnreachable(t *testing.T) {
	// Take a port and drop it, so nothing is listening on a known address.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	c, err := New(Network, addr)
	if err == nil {
		c.Close()
		t.Fatal("New succeeded against a closed port, want an error")
	}
	if c != nil {
		t.Errorf("New returned a client (%v) alongside the error, want nil", c)
	}
}

// TestLateOptionResIsIgnored covers the OPTION_RES branch of processLoop that
// is reached without a pending marker -- a duplicate or late acknowledgement.
// It must be dropped quietly instead of reaching the ErrorHandler or upsetting
// the state.
func TestLateOptionResIsIgnored(t *testing.T) {
	c := &Client{
		innerHandler: newResponseHandlerMap(),
		in:           make(chan *Response, queueSize),
	}
	var mu sync.Mutex
	var errs []error
	c.ErrorHandler = func(e error) {
		mu.Lock()
		errs = append(errs, e)
		mu.Unlock()
	}
	stopped := make(chan struct{})
	go func() {
		c.processLoop()
		close(stopped)
	}()

	// Negotiate normally, then let a second OPTION_RES arrive unannounced.
	c.in <- &Response{DataType: dtOptionSent}
	c.in <- &Response{DataType: dtOptionRes, Data: []byte(optionExceptions)}
	c.in <- &Response{DataType: dtOptionRes, Data: []byte(optionExceptions)}
	close(c.in)
	<-stopped

	if !c.ExceptionsEnabled() {
		t.Error("ExceptionsEnabled() is false after a duplicate OPTION_RES")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(errs) != 0 {
		t.Errorf("ErrorHandler called with %v, want no calls", errs)
	}
}

// TestRedialFailureStopsReadLoop covers the branch where connect() fails on the
// reconnect path: readLoop has to give up instead of spinning.
func TestRedialFailureStopsReadLoop(t *testing.T) {
	s := newFakeServer(t)
	s.handle = func(conn net.Conn, dataType uint32, data []byte) bool {
		if dataType == dtOptionReq {
			conn.Write(resPacket(dtOptionRes, data))
		}
		return true
	}

	c, err := New(Network, s.addr())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
	waitFor(t, "the handshake to complete", c.ExceptionsEnabled)

	// Drop the server entirely: the read fails, the redial cannot connect, and
	// readLoop must exit -- which closes client.in and ends processLoop.
	s.close()

	waitFor(t, "readLoop to close its channel", func() bool {
		select {
		case _, open := <-c.in:
			return !open
		default:
			return false
		}
	})
}

// TestConcurrentDo runs many submits at once against one client. The point is
// the race detector: the option state and the connection pointers are touched
// by the caller goroutines, readLoop and processLoop at the same time.
func TestConcurrentDo(t *testing.T) {
	s := newFakeServer(t)
	defer s.close()

	var hmu sync.Mutex
	n := 0
	s.handle = func(conn net.Conn, dataType uint32, data []byte) bool {
		switch dataType {
		case dtOptionReq:
			conn.Write(resPacket(dtOptionRes, data))
		case dtSubmitJob:
			hmu.Lock()
			n++
			handle := fmt.Sprintf("H:localhost:%d", n)
			hmu.Unlock()
			conn.Write(resPacket(dtJobCreated, []byte(handle)))
		}
		return true
	}

	c, err := New(Network, s.addr())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
	c.ErrorHandler = func(error) {}

	// Hammer the atomic while processLoop writes it.
	stop := make(chan struct{})
	var reader sync.WaitGroup
	reader.Add(1)
	go func() {
		defer reader.Done()
		for {
			select {
			case <-stop:
				return
			default:
				c.ExceptionsEnabled()
			}
		}
	}()

	const workers = 20
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			handle, err := c.Do("ToUpper", []byte("x"), JobNormal, func(*Response) {})
			if err != nil {
				errs <- err
				return
			}
			if handle == "" {
				errs <- fmt.Errorf("empty handle")
			}
		}()
	}
	wg.Wait()
	close(stop)
	reader.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent Do: %v", err)
	}
	if !c.ExceptionsEnabled() {
		t.Error("ExceptionsEnabled() is false after concurrent submits")
	}
}

// TestExceptionsStateRaceDuringReconnect exercises the option state while the
// client is redialling, which is when readLoop writes it from its own
// goroutine. Run with -race; the assertion is that the option survives the
// reconnect, the race detector does the rest.
func TestExceptionsStateRaceDuringReconnect(t *testing.T) {
	s := newFakeServer(t)
	defer s.close()

	var mu sync.Mutex
	conns := 0
	s.handle = func(conn net.Conn, dataType uint32, data []byte) bool {
		if dataType != dtOptionReq {
			return true
		}
		mu.Lock()
		conns++
		kill := conns <= 3 // drop the first few connections
		mu.Unlock()
		conn.Write(resPacket(dtOptionRes, data))
		return !kill
	}

	c, err := New(Network, s.addr())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
	// No ErrorHandler on purpose: assigning it after New races with readLoop's
	// own read of the field, which the dropped connections would trigger.

	stop := make(chan struct{})
	var reader sync.WaitGroup
	reader.Add(1)
	go func() {
		defer reader.Done()
		for {
			select {
			case <-stop:
				return
			default:
				c.ExceptionsEnabled()
			}
		}
	}()

	waitFor(t, "the client to redial past the dropped connections", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return conns > 3
	})
	// The option must be back on the surviving connection.
	waitFor(t, "exceptions to be enabled again", c.ExceptionsEnabled)
	close(stop)
	reader.Wait()
}

// TestWriteToPropagatesError covers writeTo's error path, which connect()
// relies on to abandon a connection whose OPTION_REQ could not be sent.
func TestWriteToPropagatesError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	conn.Close()

	if err := writeTo(rw, getOptionReq(optionExceptions)); err == nil {
		t.Error("writeTo on a closed connection returned nil, want an error")
	}

	// A payload past bufio's buffer makes the error surface from the write
	// inside the loop rather than from the final Flush.
	conn2, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	rw2 := bufio.NewReadWriter(bufio.NewReader(conn2), bufio.NewWriter(conn2))
	conn2.Close()
	if err := writeTo(rw2, getOptionReq(strings.Repeat("x", 1<<16))); err == nil {
		t.Error("writeTo of a large payload on a closed connection returned nil, want an error")
	}
}

// TestPartialPacketReassembly feeds a packet in two chunks so readLoop has to
// carry the first half in leftdata. That buffer is what connect()'s redial path
// resets, so this pins the normal reassembly it must not disturb.
func TestPartialPacketReassembly(t *testing.T) {
	s := newFakeServer(t)
	defer s.close()

	const handle = "H:localhost:9"
	s.handle = func(conn net.Conn, dataType uint32, data []byte) bool {
		switch dataType {
		case dtOptionReq:
			conn.Write(resPacket(dtOptionRes, data))
		case dtSubmitJob:
			conn.Write(resPacket(dtJobCreated, []byte(handle)))
			pkt := resPacket(dtWorkException, []byte(handle+"\x00split payload"))
			// Three chunks, to hit both halves of the reassembly: the first is
			// shorter than a header, so readLoop cannot even look at it; the
			// second makes a header but not a whole packet, so the decode
			// fails and the remainder is carried over.
			for _, chunk := range [][]byte{
				pkt[:6], pkt[6 : len(pkt)-6], pkt[len(pkt)-6:],
			} {
				conn.Write(chunk)
				time.Sleep(50 * time.Millisecond)
			}
		}
		return true
	}

	c, err := New(Network, s.addr())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()
	c.ErrorHandler = func(error) {}

	got := make(chan *Response, 2)
	if _, err := c.Do("Split", []byte("x"), JobNormal, func(r *Response) {
		got <- r
	}); err != nil {
		t.Fatalf("Do: %v", err)
	}
	select {
	case r := <-got:
		data, err := r.Result()
		if err != ErrWorkException {
			t.Errorf("Result err = %v, want ErrWorkException", err)
		}
		if string(data) != "split payload" {
			t.Errorf("Result data = %q, want %q", data, "split payload")
		}
		if r.Handle != handle {
			t.Errorf("Handle = %q, want %q", r.Handle, handle)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the split packet never reached the handler")
	}
}

// TestWriteWithoutConnection covers write()'s guard after Close(), the half of
// the old write() that stayed behind when writeTo was split off.
func TestWriteWithoutConnection(t *testing.T) {
	c := &Client{}
	if err := c.write(getOptionReq(optionExceptions)); err != ErrLostConn {
		t.Errorf("write without a connection returned %v, want ErrLostConn", err)
	}
}

// TestResultRejectsNonTerminalDataType covers Result()'s default branch: only
// the three terminal packets carry a result.
func TestResultRejectsNonTerminalDataType(t *testing.T) {
	for _, dt := range []uint32{dtWorkData, dtWorkWarning, dtWorkStatus, dtOptionRes} {
		resp := &Response{DataType: dt, Data: []byte("x")}
		if _, err := resp.Result(); err != ErrDataType {
			t.Errorf("Result on data type %d returned %v, want ErrDataType", dt, err)
		}
	}
}

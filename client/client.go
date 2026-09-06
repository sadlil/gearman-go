// The client package helps developers connect to Gearmand, send
// jobs and fetch result.
package client

import (
	"bufio"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	DefaultTimeout time.Duration = time.Second

	// DefaultExceptions controls whether a client asks the job server for the
	// "exceptions" option on every connection it opens, both the initial one
	// and every reconnect.
	//
	// With the option on, a worker that fails with a payload is delivered as a
	// WORK_EXCEPTION carrying that payload. With it off, gearmand rewrites the
	// very same event into a WORK_FAIL and throws the payload away, which is
	// why this defaults to true.
	//
	// Set it to false before calling New() to keep the old behaviour; it is
	// read once, in New(), and applies process-wide.
	DefaultExceptions = true
)

// Negotiation state of the "exceptions" option, held in Client.exceptionsState.
const (
	exceptionsPending int32 = iota // not negotiated yet, or not requested at all
	exceptionsOn                   // the job server acknowledged the option
	exceptionsRefused              // the job server did not take the option
)

// One client connect to one server.
// Use Pool for multi-connections.
type Client struct {
	sync.Mutex

	net, addr    string
	innerHandler *responseHandlerMap
	in           chan *Response

	// connMu guards the conn/rw pointers below, and nothing else.
	//
	// Deliberately not the embedded Mutex: do() holds that one until the
	// ResponseTimeout while waiting for a response, and that response only
	// arrives if readLoop keeps reading in the meantime. Were readLoop to need
	// the embedded Mutex, every Do would time out.
	//
	// The lock is never held across blocking I/O: read() and write() take a
	// single snapshot of rw and operate outside the lock.
	connMu sync.RWMutex
	conn   net.Conn
	rw     *bufio.ReadWriter

	// wantExceptions is a copy of DefaultExceptions taken in New(). It is never
	// written again, so connect() may read it from readLoop's goroutine without
	// a lock. Not guarded by connMu, which covers conn/rw and nothing else.
	wantExceptions bool
	// exceptionsState is one of the exceptions* constants. processLoop writes
	// it, ExceptionsEnabled() reads it from the caller's goroutine, hence
	// atomic. Also not guarded by connMu.
	exceptionsState int32

	ResponseTimeout time.Duration // response timeout for do()

	ErrorHandler ErrorHandler
}

// ExceptionsEnabled reports whether the job server acknowledged the
// "exceptions" option on the current connection, and therefore whether a
// worker's exception will arrive as a WORK_EXCEPTION with its payload rather
// than as a bare WORK_FAIL.
//
// It is false while the handshake is still in flight, when DefaultExceptions
// was false when the client was created, and when the server refused or
// ignored the option.
func (client *Client) ExceptionsEnabled() bool {
	return atomic.LoadInt32(&client.exceptionsState) == exceptionsOn
}

// getConn returns the current connection, or nil after Close().
func (client *Client) getConn() net.Conn {
	client.connMu.RLock()
	defer client.connMu.RUnlock()
	return client.conn
}

// getRW returns the current buffered reader/writer. Callers do their I/O on
// the returned value rather than on client.rw, which keeps the lock out of the
// blocking read or write.
func (client *Client) getRW() *bufio.ReadWriter {
	client.connMu.RLock()
	defer client.connMu.RUnlock()
	return client.rw
}

// setConn swaps connection and reader/writer in one step, so no observer can
// ever see an rw that belongs to a different conn.
func (client *Client) setConn(conn net.Conn, rw *bufio.ReadWriter) {
	client.connMu.Lock()
	defer client.connMu.Unlock()
	client.conn = conn
	client.rw = rw
}

type responseHandlerMap struct {
	sync.Mutex
	holder map[string]handledResponse
}

type handledResponse struct {
	internal ResponseHandler // internal handler, always non-nil
	external ResponseHandler // handler passed in from (*Client).Do, sometimes nil
}

func newResponseHandlerMap() *responseHandlerMap {
	return &responseHandlerMap{holder: make(map[string]handledResponse, queueSize)}
}

func (r *responseHandlerMap) remove(key string) {
	r.Lock()
	delete(r.holder, key)
	r.Unlock()
}

func (r *responseHandlerMap) getAndRemove(key string) (handledResponse, bool) {
	r.Lock()
	rh, b := r.holder[key]
	delete(r.holder, key)
	r.Unlock()
	return rh, b
}

func (r *responseHandlerMap) putWithExternalHandler(key string, internal, external ResponseHandler) {
	r.Lock()
	r.holder[key] = handledResponse{internal: internal, external: external}
	r.Unlock()
}

func (r *responseHandlerMap) put(key string, rh ResponseHandler) {
	r.putWithExternalHandler(key, rh, nil)
}

// New returns a client.
func New(network, addr string) (client *Client, err error) {
	client = &Client{
		net:             network,
		addr:            addr,
		innerHandler:    newResponseHandlerMap(),
		in:              make(chan *Response, queueSize),
		wantExceptions:  DefaultExceptions,
		ResponseTimeout: DefaultTimeout,
	}
	if err = client.connect(); err != nil {
		return nil, err
	}
	go client.readLoop()
	go client.processLoop()
	return
}

// connect dials the job server and, when the client asks for it, negotiates the
// "exceptions" option. It is the single place where a connection is
// established, so the initial dial in New() and the redial in readLoop() cannot
// drift apart -- the job server keeps the option per connection, so it has to
// be requested again on every reconnect or it silently disappears.
//
// The OPTION_REQ is written to rw *before* setConn publishes it. That is what
// makes it the first packet on the wire: no other goroutine can reach this rw
// yet, so no SUBMIT_JOB can overtake it and no concurrent write can interleave
// with its bytes. Do not move this write after setConn.
func (client *Client) connect() (err error) {
	conn, err := net.Dial(client.net, client.addr)
	if err != nil {
		return
	}
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	if client.wantExceptions {
		if err = writeTo(rw, getOptionReq(optionExceptions)); err != nil {
			conn.Close()
			return
		}
		atomic.StoreInt32(&client.exceptionsState, exceptionsPending)
	}
	client.setConn(conn, rw)
	if client.wantExceptions {
		// Tell processLoop that the next packet it sees is the answer to the
		// OPTION_REQ above. client.in is buffered and the only other sender is
		// readLoop, which is either not running yet (New) or is the goroutine
		// executing this very call (redial), so this cannot block behind
		// another packet and it lands after everything from the old
		// connection.
		client.in <- &Response{DataType: dtOptionSent}
	}
	return
}

func (client *Client) write(req *request) (err error) {
	rw := client.getRW()
	if rw == nil {
		return ErrLostConn
	}
	return writeTo(rw, req)
}

// writeTo encodes and flushes a request onto rw. It takes the reader/writer as
// an argument so connect() can use it on a connection that is not published
// yet.
func writeTo(rw *bufio.ReadWriter, req *request) (err error) {
	var n int
	buf := req.Encode()
	for i := 0; i < len(buf); i += n {
		n, err = rw.Write(buf[i:])
		if err != nil {
			return
		}
	}
	return rw.Flush()
}

func (client *Client) read(length int) (data []byte, err error) {
	n := 0
	rw := client.getRW()
	if rw == nil {
		return nil, ErrLostConn
	}
	buf := getBuffer(bufferSize)
	// read until data can be unpacked
	for i := length; i > 0 || len(data) < minPacketLength; i -= n {
		if n, err = rw.Read(buf); err != nil {
			return
		}
		data = append(data, buf[0:n]...)
		if n < bufferSize {
			break
		}
	}
	return
}

func (client *Client) readLoop() {
	defer close(client.in)
	var data, leftdata []byte
	var err error
	var resp *Response
ReadLoop:
	for client.getConn() != nil {
		if data, err = client.read(bufferSize); err != nil {
			if opErr, ok := err.(*net.OpError); ok {
				if opErr.Timeout() {
					client.err(err)
				}
				if opErr.Temporary() {
					continue
				}
				break
			}
			client.err(err)
			// If it is unexpected error and the connection wasn't
			// closed by Gearmand, the client should close the conection
			// and reconnect to job server. connect() re-negotiates the
			// "exceptions" option, which the server holds per connection.
			client.Close()
			if err = client.connect(); err != nil {
				client.err(err)
				break
			}
			// Whatever was left half-parsed belongs to the connection that
			// just died. Keeping it would prepend stale bytes to the first
			// packet of the new one -- which is now the answer to the
			// OPTION_REQ.
			leftdata = nil
			continue
		}
		if len(leftdata) > 0 { // some data left for processing
			data = append(leftdata, data...)
			leftdata = nil
		}
		for {
			l := len(data)
			if l < minPacketLength { // not enough data
				leftdata = data
				continue ReadLoop
			}
			if resp, l, err = decodeResponse(data); err != nil {
				leftdata = data[l:]
				continue ReadLoop
			} else {
				client.in <- resp
			}
			data = data[l:]
			if len(data) > 0 {
				continue
			}
			break
		}
	}
}

func (client *Client) processLoop() {
	rhandlers := map[string]ResponseHandler{}
	// optionPending is true between the marker connect() pushes and the first
	// packet that arrives on the new connection. Only this goroutine touches
	// it, like rhandlers.
	optionPending := false
	for resp := range client.in {
		if resp.DataType == dtOptionSent {
			optionPending = true
			continue
		}
		if optionPending {
			optionPending = false
			if resp.DataType == dtOptionRes {
				atomic.StoreInt32(&client.exceptionsState, exceptionsOn)
				continue
			}
			// The OPTION_REQ was the first packet on this connection (see
			// connect), so the first reply answers it. Anything other than
			// OPTION_RES means the server did not take the option: fall back
			// to WORK_FAIL semantics for this connection.
			atomic.StoreInt32(&client.exceptionsState, exceptionsRefused)
			if resp.DataType == dtError {
				// This ERROR *is* that refusal, not a fault in the caller's
				// job. Swallowing it keeps a server that does not know the
				// option from firing the user's ErrorHandler for something
				// they cannot act on. ERROR packets that are not this reply
				// still reach the handler below.
				continue
			}
			// A server that ignored the OPTION_REQ outright: this packet
			// belongs to a later request, so let it fall through and be
			// handled normally.
		}
		switch resp.DataType {
		case dtError:
			client.err(getError(resp.Data))
		case dtOptionRes:
			// A late or repeated acknowledgement. The state is already set.
		case dtStatusRes:
			client.handleInner("s"+resp.Handle, resp, nil)
		case dtJobCreated:
			client.handleInner("c", resp, rhandlers)
		case dtEchoRes:
			client.handleInner("e", resp, nil)
		case dtWorkData, dtWorkWarning, dtWorkStatus:
			if cb := rhandlers[resp.Handle]; cb != nil {
				cb(resp)
			}
		case dtWorkComplete, dtWorkFail, dtWorkException:
			if cb := rhandlers[resp.Handle]; cb != nil {
				cb(resp)
				delete(rhandlers, resp.Handle)
			}
		}
	}
}

func (client *Client) err(e error) {
	if client.ErrorHandler != nil {
		client.ErrorHandler(e)
	}
}

func (client *Client) handleInner(key string, resp *Response, rhandlers map[string]ResponseHandler) {
	if h, ok := client.innerHandler.getAndRemove(key); ok {
		if h.external != nil && resp.Handle != "" {
			rhandlers[resp.Handle] = h.external
		}
		h.internal(resp)
	}
}

type handleOrError struct {
	handle string
	err    error
}

func (client *Client) do(funcname string, data []byte,
	flag uint32, h ResponseHandler, id string) (handle string, err error) {
	if len(id) == 0 {
		return "", ErrInvalidId
	}
	if client.getConn() == nil {
		return "", ErrLostConn
	}
	var result = make(chan handleOrError, 1)
	client.Lock()
	defer client.Unlock()
	client.innerHandler.putWithExternalHandler("c", func(resp *Response) {
		if resp.DataType == dtError {
			err = getError(resp.Data)
			result <- handleOrError{"", err}
			return
		}
		handle = resp.Handle
		result <- handleOrError{handle, nil}
	}, h)
	req := getJob(id, []byte(funcname), data)
	req.DataType = flag
	if err = client.write(req); err != nil {
		client.innerHandler.remove("c")
		return
	}
	var timer = time.After(client.ResponseTimeout)
	select {
	case ret := <-result:
		return ret.handle, ret.err
	case <-timer:
		client.innerHandler.remove("c")
		return "", ErrLostConn
	}
	return
}

// Call the function and get a response.
// flag can be set to: JobLow, JobNormal and JobHigh
func (client *Client) Do(funcname string, data []byte,
	flag byte, h ResponseHandler) (handle string, err error) {
	handle, err = client.DoWithId(funcname, data, flag, h, IdGen.Id())
	return
}

// Call the function in background, no response needed.
// flag can be set to: JobLow, JobNormal and JobHigh
func (client *Client) DoBg(funcname string, data []byte,
	flag byte) (handle string, err error) {
	handle, err = client.DoBgWithId(funcname, data, flag, IdGen.Id())
	return
}

// Status gets job status from job server.
func (client *Client) Status(handle string) (status *Status, err error) {
	if client.getConn() == nil {
		return nil, ErrLostConn
	}
	var mutex sync.Mutex
	mutex.Lock()
	client.innerHandler.put("s"+handle, func(resp *Response) {
		defer mutex.Unlock()
		var err error
		status, err = resp._status()
		if err != nil {
			client.err(err)
		}
	})
	req := getRequest()
	req.DataType = dtGetStatus
	req.Data = []byte(handle)
	client.write(req)
	mutex.Lock()
	return
}

// Echo.
func (client *Client) Echo(data []byte) (echo []byte, err error) {
	if client.getConn() == nil {
		return nil, ErrLostConn
	}
	var mutex sync.Mutex
	mutex.Lock()
	client.innerHandler.put("e", func(resp *Response) {
		echo = resp.Data
		mutex.Unlock()
	})
	req := getRequest()
	req.DataType = dtEchoReq
	req.Data = data
	client.write(req)
	mutex.Lock()
	return
}

// Close connection
func (client *Client) Close() (err error) {
	client.Lock()
	defer client.Unlock()
	client.connMu.Lock()
	defer client.connMu.Unlock()
	if client.conn != nil {
		err = client.conn.Close()
		client.conn = nil
		client.rw = nil
	}
	return
}

// Call the function and get a response.
// flag can be set to: JobLow, JobNormal and JobHigh
func (client *Client) DoWithId(funcname string, data []byte,
	flag byte, h ResponseHandler, id string) (handle string, err error) {
	var datatype uint32
	switch flag {
	case JobLow:
		datatype = dtSubmitJobLow
	case JobHigh:
		datatype = dtSubmitJobHigh
	default:
		datatype = dtSubmitJob
	}
	handle, err = client.do(funcname, data, datatype, h, id)
	return
}

// Call the function in background, no response needed.
// flag can be set to: JobLow, JobNormal and JobHigh
func (client *Client) DoBgWithId(funcname string, data []byte,
	flag byte, id string) (handle string, err error) {
	if client.getConn() == nil {
		return "", ErrLostConn
	}
	var datatype uint32
	switch flag {
	case JobLow:
		datatype = dtSubmitJobLowBg
	case JobHigh:
		datatype = dtSubmitJobHighBg
	default:
		datatype = dtSubmitJobBg
	}
	handle, err = client.do(funcname, data, datatype, nil, id)
	return
}

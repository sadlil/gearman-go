package client

import (
	"encoding/binary"
)

// Request from client
type request struct {
	DataType uint32
	Data     []byte
}

// Encode a Request to byte slice
func (req *request) Encode() (data []byte) {
	l := len(req.Data)        // length of data
	tl := l + minPacketLength // add 12 bytes head
	data = getBuffer(tl)
	copy(data[:4], reqStr)
	binary.BigEndian.PutUint32(data[4:8], req.DataType)
	binary.BigEndian.PutUint32(data[8:12], uint32(l))
	copy(data[minPacketLength:], req.Data)
	return
}

func getRequest() (req *request) {
	// TODO add a pool
	req = &request{}
	return
}

// getOptionReq builds an OPTION_REQ for the named option. The protocol takes
// the bare name as the only argument, with no trailing NULL byte.
func getOptionReq(name string) (req *request) {
	req = getRequest()
	req.DataType = dtOptionReq
	req.Data = []byte(name)
	return
}

func getJob(id string, funcname, data []byte) (req *request) {
	req = getRequest()
	a := len(funcname)
	b := len(id)
	c := len(data)
	l := a + b + c + 2
	req.Data = getBuffer(l)
	copy(req.Data[0:a], funcname)
	copy(req.Data[a+1:a+b+1], []byte(id))
	copy(req.Data[a+b+2:], data)
	return
}

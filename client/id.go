package client

import (
	"time"
	"strconv"
	"sync/atomic"
)

var (
	IdGen IdGenerator
)

func init() {
	IdGen = NewAutoIncId()
}

type IdGenerator interface {
	Id() string
}

// AutoIncId
type autoincId struct {
	value int64
}

func (ai *autoincId) Id() string {
	next := atomic.AddInt64(&ai.value, 1)
	return strconv.FormatInt(next, 10)
}

func NewAutoIncId() IdGenerator {
	// we'll consider the nano fraction of a second at startup unique
	// and count up from there.
	return &autoincId{
		value: int64(time.Now().Nanosecond()) << 32,
	}
}

Gearman-Go
==========

This module is a [Gearman](http://gearman.org/) API for the [Go Programming Language](http://golang.org).
The protocols were written in pure Go. It contains two sub-packages:

The client package is used for sending jobs to the Gearman job server,
and getting responses from the server.

	"github.com/mikespook/gearman-go/client"

The worker package will help developers in developing Gearman worker
service easily.

	"github.com/mikespook/gearman-go/worker"

[![Build Status](https://travis-ci.org/mikespook/gearman-go.png?branch=master)](https://travis-ci.org/mikespook/gearman-go)
[![GoDoc](https://godoc.org/github.com/mikespook/gearman-go?status.png)](https://godoc.org/github.com/mikespook/gearman-go)

Install
=======

Install the client package:

> $ go get github.com/mikespook/gearman-go/client
	
Install the worker package:

> $ go get github.com/mikespook/gearman-go/worker

Both of them:

> $ go get github.com/mikespook/gearman-go

Usage
=====

## Worker

```go
// Limit number of concurrent jobs execution. 
// Use worker.Unlimited (0) if you want no limitation.
w := worker.New(worker.OneByOne)
w.ErrHandler = func(e error) {
	log.Println(e)
}
w.AddServer("127.0.0.1:4730")
// Use worker.Unlimited (0) if you want no timeout
w.AddFunc("ToUpper", ToUpper, worker.Unlimited)
// This will give a timeout of 5 seconds
w.AddFunc("ToUpperTimeOut5", ToUpper, 5)

if err := w.Ready(); err != nil {
	log.Fatal(err)
	return
}
go w.Work()
```

## Client

```go
// ...
c, err := client.New("tcp4", "127.0.0.1:4730")
// ... error handling
defer c.Close()
c.ErrorHandler = func(e error) {
	log.Println(e)
}
echo := []byte("Hello\x00 world")
echomsg, err := c.Echo(echo)
// ... error handling
log.Println(string(echomsg))
jobHandler := func(resp *client.Response) {
	log.Printf("%s", resp.Data)
}
handle, err := c.Do("ToUpper", echo, client.JobNormal, jobHandler)
// ...	
```

## Worker exceptions

A worker that fails *and* returns data raises a Gearman exception. The job
server only forwards those to a client that asked for them, by sending an
`OPTION_REQ exceptions` on the connection; without it gearmand rewrites the
exception into a plain `WORK_FAIL` and throws the worker's payload away.

The client requests that option by default, on the initial connection and on
every reconnect, so a worker exception arrives intact:

```go
jobHandler := func(resp *client.Response) {
	switch resp.DataType {
	case client.WorkComplete:
		data, _ := resp.Result()
		log.Printf("done: %s", data)
	case client.WorkException:
		// err is client.ErrWorkException; data is the worker's payload.
		data, err := resp.Result()
		log.Printf("exception: %s (%v)", data, err)
	case client.WorkFail:
		log.Printf("failed without a payload")
	}
}
```

`WORK_EXCEPTION` is terminal: the job is finished and no `WORK_FAIL` or
`WORK_COMPLETE` follows it.

### Upgrading

Before this option existed, a worker exception reached the client as a
`WORK_FAIL`. It now arrives as a `WORK_EXCEPTION` instead:

| | before | now |
|---|---|---|
| `resp.DataType` | `client.WorkFail` | `client.WorkException` |
| `resp.Data` | `nil` | the worker's payload |
| `resp.Result()` | `nil, ErrWorkFail` | `payload, ErrWorkException` |

So check your job handlers for two things: a `switch resp.DataType` without a
`case client.WorkException` now drops those responses silently, and retry logic
keyed on `err == client.ErrWorkFail` no longer fires for exceptions.

To keep the old behaviour, opt out before creating any client:

```go
client.DefaultExceptions = false
```

`(*Client).ExceptionsEnabled()` reports whether the job server actually
acknowledged the option. A server that refuses or ignores it is not an error:
the client degrades to the old `WORK_FAIL` behaviour and stays usable.

Note the worker side of this: `gearman-go` sends a `WORK_EXCEPTION` only when
the job function returns a non-empty `data` *and* an error. `return nil, err`
still produces a `WORK_FAIL`, and the text of `err` never goes over the wire —
put anything the client needs to see in `data`.

Branches
========

Version 0.x means: _It is far far away from stable._

__Use at your own risk!__

 * master current usable version
 * 0.2-dev Refactoring a lot of things
 * 0.1-testing Old API and some known issues, eg. [issue-14](https://github.com/mikespook/gearman-go/issues/14)

Contributors
============

Great thanks to all of you for your support and interest!

(_Alphabetic order_)
 
 * [Alex Zylman](https://github.com/azylman)
 * [C.R. Kirkwood-Watts](https://github.com/kirkwood)
 * [Damian Gryski](https://github.com/dgryski)
 * [Gabriel Cristian Alecu](https://github.com/AzuraMeta)
 * [Graham Barr](https://github.com/gbarr)
 * [Ingo Oeser](https://github.com/nightlyone)
 * [jake](https://github.com/jbaikge)
 * [Joe Higton](https://github.com/draxil)
 * [Jonathan Wills](https://github.com/runningwild)
 * [Kevin Darlington](https://github.com/kdar)
 * [miraclesu](https://github.com/miraclesu)
 * [Paul Mach](https://github.com/paulmach)
 * [Randall McPherson](https://github.com/rlmcpherson)
 * [Sam Grimee](https://github.com/sgrimee)

Maintainer
==========

 * [Xing Xing](http://mikespook.com) &lt;<mikespook@gmail.com>&gt; [@Twitter](http://twitter.com/mikespook)

Open Source - MIT Software License
==================================

See LICENSE.

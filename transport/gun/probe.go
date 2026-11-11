package gun

import (
	"context"
	"net"
	"sync/atomic"
	"time"
)

// Probe stages, in the order a connection reaches them.
const (
	StageNone = iota
	StageTCP
	StageTLS
)

// Trace records how far a probe connection got and when.
type Trace struct {
	start   time.Time
	tcpDone atomic.Int64
	tlsDone atomic.Int64
}

func (t *Trace) markTCP() {
	if t != nil {
		t.tcpDone.Store(time.Now().UnixNano())
	}
}

func (t *Trace) markTLS() {
	if t != nil {
		t.tlsDone.Store(time.Now().UnixNano())
	}
}

// Stage is the last stage the connection completed.
func (t *Trace) Stage() int {
	if t.tlsDone.Load() != 0 {
		return StageTLS
	}
	if t.tcpDone.Load() != 0 {
		return StageTCP
	}
	return StageNone
}

// TCP is the time from the start to an open TCP connection, zero if it never
// opened.
func (t *Trace) TCP() time.Duration {
	return t.since(t.tcpDone.Load())
}

// TLS is the time from the start to a finished TLS handshake, zero if it
// never finished.
func (t *Trace) TLS() time.Duration {
	return t.since(t.tlsDone.Load())
}

func (t *Trace) since(nanos int64) time.Duration {
	if nanos == 0 {
		return 0
	}
	// The wall clock can be coarser than a local connect; a stage that was
	// reached never reads as zero.
	if elapsed := time.Unix(0, nanos).Sub(t.start); elapsed > 0 {
		return elapsed
	}
	return time.Nanosecond
}

// DialProbe opens a stream on a connection of its own. That connection obeys
// ctx from the TCP dial onwards, is never handed to traffic, and closes with
// the stream, so a probe neither reuses nor tears down the live tunnel.
func (c *Client) DialProbe(ctx context.Context) (net.Conn, *Trace) {
	transport := c.maker()
	trace := &Trace{start: time.Now()}
	transport.trace = trace
	transport.bind(ctx)
	conn := transport.dial(func() { _ = transport.Close() })
	return conn, trace
}

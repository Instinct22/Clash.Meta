package gun

import (
	"sync/atomic"
	"time"
)

// Activity records when stream bytes last moved on a transport, so a caller
// can tell a quiet connection from one that stopped answering.
type Activity struct {
	lastRead  atomic.Int64
	lastWrite atomic.Int64
	// The first write after the last read; zero once something came back.
	pendingSince atomic.Int64
}

func (a *Activity) wrote(now int64) {
	a.lastWrite.Store(now)
	a.pendingSince.CompareAndSwap(0, now)
}

func (a *Activity) read(now int64) {
	a.lastRead.Store(now)
	a.pendingSince.Store(0)
}

// Health is a snapshot of the transports of one client.
type Health struct {
	Streams   int64
	LastRead  time.Time
	LastWrite time.Time
	// The oldest write still waiting for any byte back on a transport that
	// carries streams. Zero when every write has been answered.
	PendingSince time.Time
}

func unixTime(nanos int64) time.Time {
	if nanos == 0 {
		return time.Time{}
	}
	return time.Unix(0, nanos)
}

// Health reports stream counts and byte timing across live transports.
func (c *Client) Health() Health {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var health Health
	var lastRead, lastWrite, pending int64
	for _, t := range c.transports {
		if t.closed.Load() {
			continue
		}
		streams := t.count.Load()
		health.Streams += streams
		if read := t.activity.lastRead.Load(); read > lastRead {
			lastRead = read
		}
		if write := t.activity.lastWrite.Load(); write > lastWrite {
			lastWrite = write
		}
		if streams == 0 {
			continue
		}
		if since := t.activity.pendingSince.Load(); since != 0 && (pending == 0 || since < pending) {
			pending = since
		}
	}
	health.LastRead = unixTime(lastRead)
	health.LastWrite = unixTime(lastWrite)
	health.PendingSince = unixTime(pending)
	return health
}

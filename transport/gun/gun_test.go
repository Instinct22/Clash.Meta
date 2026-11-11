package gun

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/httputils"
)

func plainTransport(dialFn DialFn) func() *Transport {
	return func() *Transport {
		return NewTransport(dialFn, nil, &Config{Host: "probe.test"})
	}
}

// A listener that accepts TCP and then never says anything, like a server
// whose handshake is being dropped.
func silentListener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()
	return listener
}

func dialTo(address string) DialFn {
	return func(ctx context.Context, network, _ string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, address)
	}
}

func TestProbeConnectionObeysItsDeadline(t *testing.T) {
	listener := silentListener(t)
	client := NewClient(plainTransport(dialTo(listener.Addr().String())), 1, 0, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	conn, trace := client.DialProbe(ctx)
	defer conn.Close()
	_, err := conn.Read(make([]byte, 1))
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("a silent server answered the probe")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("the probe outlived its deadline: %v", elapsed)
	}
	if trace.Stage() != StageTLS || trace.TCP() <= 0 {
		t.Fatalf("the open TCP connection was not traced: stage %d", trace.Stage())
	}
}

func TestProbeTraceStopsBeforeTCPWhenNothingListens(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	client := NewClient(plainTransport(dialTo(address)), 1, 0, 0)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, trace := client.DialProbe(ctx)
	defer conn.Close()
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("a closed port answered the probe")
	}
	if trace.Stage() != StageNone || trace.TCP() != 0 {
		t.Fatalf("a refused dial was traced as connected: stage %d", trace.Stage())
	}
}

func TestProbeNeverSharesOrClosesTheLiveTransport(t *testing.T) {
	listener := silentListener(t)
	client := NewClient(plainTransport(dialTo(listener.Addr().String())), 1, 0, 0)
	live, err := client.Dial()
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	if len(client.transports) != 1 {
		t.Fatalf("live transports: %d", len(client.transports))
	}
	liveTransport := client.transports[0]

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	probe, _ := client.DialProbe(ctx)
	_, _ = probe.Read(make([]byte, 1))
	_ = probe.Close()

	if len(client.transports) != 1 || client.transports[0] != liveTransport {
		t.Fatal("the probe joined the live transports")
	}
	if liveTransport.closed.Load() {
		t.Fatal("closing the probe closed the live tunnel")
	}
	if liveTransport.count.Load() != 1 {
		t.Fatalf("live streams: %d", liveTransport.count.Load())
	}
}

func TestClosedProbeReleasesItsConnection(t *testing.T) {
	var made []*Transport
	listener := silentListener(t)
	maker := plainTransport(dialTo(listener.Addr().String()))
	client := NewClient(func() *Transport {
		transport := maker()
		made = append(made, transport)
		return transport
	}, 1, 0, 0)

	probe, _ := client.DialProbe(context.Background())
	_ = probe.Close()

	if len(made) != 1 || !made[0].closed.Load() {
		t.Fatal("a closed probe kept its connection")
	}
}

type frameReader struct{ *bytes.Reader }

func (frameReader) Close() error { return nil }

func grpcFrame(payload []byte) []byte {
	frame := []byte{0, 0, 0, 0, byte(2 + len(payload)), 0x0A, byte(len(payload))}
	return append(frame, payload...)
}

func trackedConn(transport *Transport, reply []byte) *Conn {
	_, writer := io.Pipe()
	conn := &Conn{
		initFn: func(*httputils.NetAddr) (io.ReadCloser, error) {
			return frameReader{bytes.NewReader(reply)}, nil
		},
		writer:   discardWriter{writer},
		activity: &transport.activity,
	}
	transport.count.Add(1)
	return conn
}

type discardWriter struct{ *io.PipeWriter }

func (discardWriter) Write(b []byte) (int, error) { return len(b), nil }

func TestHealthShowsAWriteThatNothingAnswered(t *testing.T) {
	transport := NewTransport(nil, nil, &Config{})
	client := NewClient(func() *Transport { return transport }, 1, 0, 0)
	client.transports = []*Transport{transport}
	conn := trackedConn(transport, grpcFrame([]byte("ok")))

	if health := client.Health(); !health.PendingSince.IsZero() || health.Streams != 1 {
		t.Fatalf("a fresh stream looked stuck: %+v", health)
	}
	if _, err := conn.Write([]byte("request")); err != nil {
		t.Fatal(err)
	}
	pending := client.Health().PendingSince
	if pending.IsZero() {
		t.Fatal("an unanswered write was not tracked")
	}
	if _, err := conn.Write([]byte("more")); err != nil {
		t.Fatal(err)
	}
	if again := client.Health().PendingSince; !again.Equal(pending) {
		t.Fatal("a second write moved the start of the wait")
	}
	if _, err := conn.Read(make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	health := client.Health()
	if !health.PendingSince.IsZero() {
		t.Fatal("an answer did not clear the wait")
	}
	if health.LastRead.Before(health.LastWrite) {
		t.Fatal("the read time was not recorded")
	}
}

func TestHealthIgnoresTransportsWithoutStreams(t *testing.T) {
	transport := NewTransport(nil, nil, &Config{})
	client := NewClient(func() *Transport { return transport }, 1, 0, 0)
	client.transports = []*Transport{transport}
	conn := trackedConn(transport, nil)
	_, _ = conn.Write([]byte("request"))
	transport.count.Add(-1)

	if health := client.Health(); !health.PendingSince.IsZero() {
		t.Fatal("a transport with no streams was reported as waiting")
	}
}

package gun

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/http"
)

// echoServer is an unencrypted HTTP/2 server that sends every request body
// back, like a tunnel endpoint, and counts the connections it accepted.
type echoServer struct {
	net.Listener
	mu       sync.Mutex
	conns    []net.Conn
	accepted atomic.Int32
}

func (s *echoServer) Accept() (net.Conn, error) {
	conn, err := s.Listener.Accept()
	if err == nil {
		s.accepted.Add(1)
		s.mu.Lock()
		s.conns = append(s.conns, conn)
		s.mu.Unlock()
	}
	return conn, err
}

func (s *echoServer) dropAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, conn := range s.conns {
		_ = conn.Close()
	}
	s.conns = nil
}

func newEchoServer(t *testing.T) *echoServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &echoServer{Listener: listener}
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	httpServer := &http.Server{
		Protocols: protocols,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			flusher := w.(http.Flusher)
			w.WriteHeader(http.StatusOK)
			flusher.Flush()
			buf := make([]byte, 1024)
			for {
				n, err := r.Body.Read(buf)
				if n > 0 {
					_, _ = w.Write(buf[:n])
					flusher.Flush()
				}
				if err != nil {
					return
				}
			}
		}),
	}
	go func() { _ = httpServer.Serve(server) }()
	t.Cleanup(func() { _ = httpServer.Close() })
	return server
}

func echoes(client *Client) bool {
	conn, err := client.Dial()
	if err != nil {
		return false
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Write([]byte("hello")); err != nil {
		return false
	}
	_, err = io.ReadFull(conn, make([]byte, 5))
	return err == nil
}

func TestStreamsOfANewTransportShareOneHandshake(t *testing.T) {
	server := newEchoServer(t)
	var dials atomic.Int32
	handshake := func(ctx context.Context, network, _ string) (net.Conn, error) {
		dials.Add(1)
		time.Sleep(200 * time.Millisecond)
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, server.Addr().String())
	}
	client := NewClient(plainTransport(handshake), 0, 0, 0)

	var wg sync.WaitGroup
	var answered atomic.Int32
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if echoes(client) {
				answered.Add(1)
			}
		}()
	}
	wg.Wait()

	if dials.Load() != 1 || server.accepted.Load() != 1 {
		t.Fatalf("30 streams opened %d connections", dials.Load())
	}
	if answered.Load() != 30 {
		t.Fatalf("only %d of 30 streams were answered", answered.Load())
	}
}

func TestTransportOpensANewConnectionAfterTheServerDropsIt(t *testing.T) {
	server := newEchoServer(t)
	client := NewClient(plainTransport(dialTo(server.Addr().String())), 0, 0, 0)
	if !echoes(client) {
		t.Fatal("the first stream was not answered")
	}
	for drop := 1; drop <= 3; drop++ {
		server.dropAll()
		time.Sleep(100 * time.Millisecond)
		if !echoes(client) {
			t.Fatalf("no stream after drop %d", drop)
		}
		if got := server.accepted.Load(); got != int32(drop+1) {
			t.Fatalf("after drop %d the server saw %d connections", drop, got)
		}
	}
}

func TestAWriteWaitingForTheConnectionIsPending(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	opening := func(ctx context.Context, _, _ string) (net.Conn, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, context.Canceled
	}
	client := NewClient(plainTransport(opening), 0, 0, 0)
	conn, err := client.Dial()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	go func() { _, _ = conn.Write([]byte("hello")) }()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if health := client.Health(); health.Streams == 1 && !health.PendingSince.IsZero() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("a write held up by the opening connection was not pending: %+v", client.Health())
}

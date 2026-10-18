package raftstore

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// streamLayer runs the Raft transport over a pre-bound listener, so callers (and tests) bind the
// port first and never race for it (plan R9). It tracks the connections it accepted and closes
// them in Close: hashicorp's NetworkTransport.Close leaves accepted connections (and their
// handler goroutines) open until the remote side hangs up.
type streamLayer struct {
	net.Listener

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
}

func newStreamLayer(lis net.Listener) *streamLayer {
	return &streamLayer{Listener: lis, conns: map[net.Conn]struct{}{}}
}

// Accept implements net.Listener, tracking the connection.
func (s *streamLayer) Accept() (net.Conn, error) {
	conn, err := s.Listener.Accept()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		_ = conn.Close()
		return nil, net.ErrClosed
	}
	tc := &trackedConn{Conn: conn, layer: s}
	s.conns[tc] = struct{}{}
	return tc, nil
}

// Close implements net.Listener: it stops accepting and closes every accepted connection.
func (s *streamLayer) Close() error {
	s.mu.Lock()
	s.closed = true
	conns := s.conns
	s.conns = map[net.Conn]struct{}{}
	s.mu.Unlock()
	err := s.Listener.Close()
	for c := range conns {
		_ = c.(*trackedConn).Conn.Close()
	}
	return err
}

// Dial implements raft.StreamLayer.
func (s *streamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	d := net.Dialer{Timeout: timeout}
	conn, err := d.Dial("tcp", string(address))
	if err != nil {
		return nil, fmt.Errorf("dial raft peer %s: %w", address, err)
	}
	return conn, nil
}

// trackedConn removes itself from the layer when closed.
type trackedConn struct {
	net.Conn
	layer *streamLayer
}

func (c *trackedConn) Close() error {
	c.layer.mu.Lock()
	delete(c.layer.conns, c)
	c.layer.mu.Unlock()
	return c.Conn.Close()
}

// slogWriter adapts Raft's line-oriented hclog output to slog.
type slogWriter struct{ logger *slog.Logger }

func (w slogWriter) Write(p []byte) (int, error) {
	line := strings.TrimSpace(string(p))
	level := slog.LevelInfo
	switch {
	case strings.Contains(line, "[ERROR]"):
		level = slog.LevelError
	case strings.Contains(line, "[WARN]"):
		level = slog.LevelWarn
	case strings.Contains(line, "[DEBUG]"), strings.Contains(line, "[TRACE]"):
		level = slog.LevelDebug
	}
	w.logger.Log(context.Background(), level, line)
	return len(p), nil
}

func hclogger(logger *slog.Logger, name string) hclog.Logger {
	return hclog.New(&hclog.LoggerOptions{Name: name, Level: hclog.Info, Output: io.Writer(slogWriter{logger})})
}

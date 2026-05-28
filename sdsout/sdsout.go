package sdsout

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

const (
	commandIO  = "io"
	commandEOF = "eof"
)

// SDSOut subscribes to SDSIn messages and writes received rows to an io.Writer.
type SDSOut struct {
	mu      sync.RWMutex
	config  *config.Handler
	writer  io.Writer
	socket  *zmq.Socket
	done    chan struct{}
	stopped chan struct{}
}

// New creates an SDSOut subscriber that writes to os.Stdout by default.
func New() *SDSOut {
	return &SDSOut{
		writer: os.Stdout,
	}
}

// Config returns the SDSIn handler configuration.
func (out *SDSOut) Config() *config.Handler {
	out.mu.RLock()
	defer out.mu.RUnlock()

	return out.config
}

// SetConfig sets the SDSIn handler to subscribe to and, optionally, the output writer.
func (out *SDSOut) SetConfig(handler *config.Handler, writers ...io.Writer) {
	writer := io.Writer(os.Stdout)
	if len(writers) > 0 && writers[0] != nil {
		writer = writers[0]
	}

	out.mu.Lock()
	defer out.mu.Unlock()

	out.config = handler
	out.writer = writer
}

// StartInBg starts the subscriber in a goroutine and waits until the socket is ready.
func (out *SDSOut) StartInBg() error {
	out.mu.Lock()
	if out.config == nil {
		out.mu.Unlock()
		return fmt.Errorf("configuration not set")
	}
	if out.done != nil {
		out.mu.Unlock()
		return fmt.Errorf("sdsout already running")
	}

	done := make(chan struct{})
	stopped := make(chan struct{})
	ready := make(chan error, 1)
	url := config.ConnectUrl(out.config.Id, out.config.Port)
	writer := out.writer
	if writer == nil {
		writer = os.Stdout
	}

	out.done = done
	out.stopped = stopped
	out.mu.Unlock()

	go out.run(url, writer, done, ready, stopped)

	if err := <-ready; err != nil {
		out.mu.Lock()
		out.done = nil
		out.stopped = nil
		out.mu.Unlock()
		return err
	}

	return nil
}

func (out *SDSOut) run(url string, writer io.Writer, done <-chan struct{}, ready chan<- error, stopped chan<- struct{}) {
	defer close(stopped)

	socket, err := zmq.NewSocket(zmq.SUB)
	if err != nil {
		ready <- fmt.Errorf("zmq.NewSocket('%s'): %w", zmq.SUB, err)
		return
	}

	if err := socket.SetSubscribe(""); err != nil {
		_ = socket.Close()
		ready <- fmt.Errorf("socket.SetSubscribe(''): %w", err)
		return
	}
	if err := socket.Connect(url); err != nil {
		_ = socket.Close()
		ready <- fmt.Errorf("socket.Connect('%s'): %w", url, err)
		return
	}

	poller := zmq.NewPoller()
	poller.Add(socket, zmq.POLLIN)

	out.mu.Lock()
	out.socket = socket
	out.mu.Unlock()

	ready <- nil

	for {
		select {
		case <-done:
			out.closeSocket(socket)
			return
		default:
		}

		polled, err := poller.Poll(time.Millisecond)
		if err != nil {
			continue
		}
		if len(polled) == 0 {
			continue
		}

		raw, err := socket.RecvMessage(0)
		if err != nil {
			continue
		}
		if !out.handleMessage(writer, raw) {
			out.closeSocket(socket)
			out.clearRunState()
			return
		}
	}
}

func (out *SDSOut) handleMessage(writer io.Writer, raw []string) bool {
	req, err := message.NewReq(raw)
	if err != nil {
		return true
	}

	switch req.CommandName() {
	case commandIO:
		out.writeRow(writer, req)
	case commandEOF:
		return false
	}

	return true
}

func (out *SDSOut) writeRow(writer io.Writer, req message.RequestInterface) {

	row, err := req.RouteParameters().StringValue("row")
	if err != nil {
		return
	}

	_, _ = writer.Write([]byte(row))
}

func (out *SDSOut) closeSocket(socket *zmq.Socket) {
	if err := socket.Close(); err != nil {
		return
	}

	out.mu.Lock()
	defer out.mu.Unlock()

	out.socket = nil
}

func (out *SDSOut) clearRunState() {
	out.mu.Lock()
	defer out.mu.Unlock()

	out.done = nil
	out.stopped = nil
}

// Close stops the subscriber and closes the ZMQ socket.
func (out *SDSOut) Close() error {
	out.mu.Lock()
	if out.socket == nil || out.done == nil {
		out.mu.Unlock()
		return nil
	}

	done := out.done
	stopped := out.stopped
	out.done = nil
	out.stopped = nil
	out.mu.Unlock()

	close(done)
	<-stopped

	return nil
}

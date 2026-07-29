package sdsin

import (
	"fmt"
	"io"
	"sync"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

const (
	queueSize        = 1024
	publisherIdle    = "idle"
	publisherRunning = "running"
	commandIO        = "io"
	commandEOF       = "eof"
)

// SDSIn publishes data written through io.Writer as SDS request messages to a ZMQ PUB socket.
type SDSIn struct {
	*handler.Handler
	Control *handler.Control

	logger *log.Logger

	mu      sync.RWMutex
	socket  *zmq.Socket
	queue   chan message.RequestInterface
	done    chan struct{}
	ready   chan error
	stopped chan struct{}
}

var _ io.Writer = (*SDSIn)(nil)

// New creates an io.Writer publisher.
func New() *SDSIn {
	return &SDSIn{
		Handler: handler.New(),
		Control: handler.NewControl(),
	}
}

// SetConfig adds the parameters of the handler from the endpoint config.
func (publisher *SDSIn) SetConfig(endpoint message.Endpoint) {
	publisher.Handler.SetEndpoint(endpoint)
	publisher.Control.SetEndpoint(handler.NewInternalControlEndpoint(endpoint))
}

// SetLogger sets the logger for this publisher.
func (publisher *SDSIn) SetLogger(parent *log.Logger) error {
	if err := publisher.Handler.SetLogger(parent); err != nil {
		return err
	}
	if parent == nil {
		publisher.mu.Lock()
		publisher.logger = nil
		publisher.mu.Unlock()
		return publisher.Control.SetLogger(nil)
	}
	if publisher.Endpoint() == (message.Endpoint{}) {
		return fmt.Errorf("configuration not set")
	}

	publisher.mu.Lock()
	defer publisher.mu.Unlock()

	publisher.logger = parent.Child(publisher.Endpoint().ZapDomain())
	return publisher.Control.SetLogger(parent.Child(handler.ControlCategory))
}

// Type returns the handler type.
func (publisher *SDSIn) Type() handler.HandlerType {
	return handler.PublisherType
}

// Route returns an error because SDSIn publishes io.Writer messages and has no request routes.
func (publisher *SDSIn) Route(_ string, _ handler.HandleFunc) error {
	return fmt.Errorf("sdsin doesn't support routing")
}

func (publisher *SDSIn) publisherStatus() string {
	publisher.mu.RLock()
	defer publisher.mu.RUnlock()

	if publisher.socket == nil {
		return publisherIdle
	}
	return publisherRunning
}

func (publisher *SDSIn) startPublisher() error {
	publisher.mu.Lock()
	if publisher.Endpoint() == (message.Endpoint{}) {
		publisher.mu.Unlock()
		return fmt.Errorf("configuration not set")
	}
	if publisher.logger == nil {
		publisher.mu.Unlock()
		return fmt.Errorf("logger not set")
	}
	if publisher.socket != nil || publisher.queue != nil {
		publisher.mu.Unlock()
		return fmt.Errorf("publisher already running")
	}

	queue := make(chan message.RequestInterface, queueSize)
	done := make(chan struct{})
	ready := make(chan error, 1)
	stopped := make(chan struct{})

	publisher.queue = queue
	publisher.done = done
	publisher.ready = ready
	publisher.stopped = stopped
	handlerEndpoint := publisher.Endpoint()
	publisher.mu.Unlock()

	go publisher.run(handlerEndpoint, queue, done, ready, stopped)

	if err := <-ready; err != nil {
		publisher.mu.Lock()
		publisher.queue = nil
		publisher.done = nil
		publisher.ready = nil
		publisher.stopped = nil
		publisher.mu.Unlock()
		return err
	}

	return nil
}

// Start starts the publisher socket and the control handler.
func (publisher *SDSIn) Start() error {
	if publisher.Endpoint() == (message.Endpoint{}) {
		return fmt.Errorf("configuration not set")
	}
	if publisher.Control == nil {
		return fmt.Errorf("control not set. call SetConfig and SetLogger first")
	}

	if err := publisher.setControlRoutes(); err != nil {
		return err
	}
	if err := publisher.startPublisher(); err != nil {
		return fmt.Errorf("sdsin.startPublisher: %w", err)
	}
	if publisher.Control.Status() != handler.SocketReady {
		if err := publisher.Control.Start(); err != nil {
			_ = publisher.Close()
			return fmt.Errorf("control.Start: %w", err)
		}
	}

	return nil
}

// StartInBg starts the publisher in a goroutine and waits until startup finishes.
func (publisher *SDSIn) StartInBg() error {
	ready := make(chan error, 1)

	go func() {
		ready <- publisher.Start()
	}()

	return <-ready
}

func (publisher *SDSIn) setControlRoutes() error {
	onStatus := func(req message.RequestInterface) message.ReplyInterface {
		return req.Ok(datatype.New().Set("status", publisher.publisherStatus()))
	}

	onClose := func(req message.RequestInterface) message.ReplyInterface {
		if err := publisher.Close(); err != nil {
			return req.Fail(err.Error())
		}
		return req.Ok(datatype.New())
	}

	onStart := func(req message.RequestInterface) message.ReplyInterface {
		if err := publisher.startPublisher(); err != nil {
			return req.Fail(fmt.Sprintf("sdsin.startPublisher: %v", err))
		}
		return req.Ok(datatype.New().Set("status", publisher.publisherStatus()))
	}

	onMessageAmount := func(req message.RequestInterface) message.ReplyInterface {
		publisher.mu.RLock()
		queueLength := 0
		if publisher.queue != nil {
			queueLength = len(publisher.queue)
		}
		publisher.mu.RUnlock()

		return req.Ok(datatype.New().Set("queue_length", queueLength))
	}

	if err := publisher.Control.Route(handler.HandlerStatus, onStatus); err != nil {
		return fmt.Errorf("overwriting control 'status' failed: %w", err)
	}
	if err := publisher.Control.Route(handler.HandlerClose, onClose); err != nil {
		return fmt.Errorf("overwriting control 'close' failed: %w", err)
	}
	if err := publisher.Control.Route(handler.HandlerStart, onStart); err != nil {
		return fmt.Errorf("overwriting control 'start' failed: %w", err)
	}
	if err := publisher.Control.Route("message-amount", onMessageAmount); err != nil {
		return fmt.Errorf("overwriting control 'message-amount' failed: %w", err)
	}
	if err := publisher.Control.Route(handler.HandlerConfig, func(req message.RequestInterface) message.ReplyInterface {
		return req.Ok(datatype.New().Set("config", publisher.Endpoint()))
	}); err != nil {
		return fmt.Errorf("overwriting control 'config' failed: %w", err)
	}

	return nil
}

func (publisher *SDSIn) run(handlerEndpoint message.Endpoint, queue <-chan message.RequestInterface, done <-chan struct{}, ready chan<- error, stopped chan<- struct{}) {
	defer close(stopped)

	socket, err := zmq.NewSocket(zmq.PUB)
	if err != nil {
		ready <- fmt.Errorf("new_socket('%s'): %v", handler.PublisherType, err)
		return
	}

	url := handlerEndpoint.HandlerUrl()
	if err := socket.Bind(url); err != nil {
		_ = socket.Close()
		ready <- fmt.Errorf("socket.Bind('%s'): %v", url, err)
		return
	}

	publisher.mu.Lock()
	publisher.socket = socket
	publisher.mu.Unlock()

	ready <- nil

	for {
		select {
		case <-done:
			publisher.sendRequest(socket, &message.Request{Command: commandEOF, Parameters: datatype.New()})
			publisher.closeSocket(socket)
			return
		case req := <-queue:
			publisher.sendRequest(socket, req)
		}
	}
}

func (publisher *SDSIn) sendRequest(socket *zmq.Socket, req message.RequestInterface) {
	reqStr, err := publisher.Packer().SerializeRequest(req)
	if err != nil {
		publisher.logger.Error("Packer.SerializeRequest", "error", err)
		return
	}
	if _, err := socket.SendMessageDontwait(reqStr); err != nil {
		publisher.logger.Error("socket.SendMessageDontwait", "request", reqStr, "error", err)
	}
}

func (publisher *SDSIn) closeSocket(socket *zmq.Socket) {
	if err := socket.Close(); err != nil {
		publisher.logger.Error("socket.Close", "error", err)
	}

	publisher.mu.Lock()
	defer publisher.mu.Unlock()

	publisher.socket = nil
}

// Close stops the publisher and closes the PUB socket.
func (publisher *SDSIn) Close() error {
	publisher.mu.Lock()
	if publisher.socket == nil || publisher.done == nil {
		publisher.mu.Unlock()
		return fmt.Errorf("publisher not running")
	}

	done := publisher.done
	stopped := publisher.stopped
	publisher.done = nil
	publisher.queue = nil
	publisher.ready = nil
	publisher.stopped = nil
	publisher.mu.Unlock()

	close(done)
	<-stopped

	return nil
}

// Write publishes p as an SDS Request with command "io" and parameter "row".
func (publisher *SDSIn) Write(p []byte) (int, error) {
	req := &message.Request{
		Command:    commandIO,
		Parameters: datatype.New().Set("row", string(p)),
	}

	publisher.mu.RLock()
	queue := publisher.queue
	done := publisher.done
	running := publisher.socket != nil && queue != nil && done != nil
	publisher.mu.RUnlock()

	if !running {
		return 0, fmt.Errorf("publisher not running")
	}

	select {
	case queue <- req:
		return len(p), nil
	case <-done:
		return 0, fmt.Errorf("publisher not running")
	}
}

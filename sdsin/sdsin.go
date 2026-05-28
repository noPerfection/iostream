package sdsin

import (
	"fmt"
	"io"
	"sync"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/base"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/handler/handler_manager"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

const (
	queueSize        = 1024
	partPublisher    = "publisher"
	publisherIdle    = "idle"
	publisherRunning = "running"
	commandIO        = "io"
	commandEOF       = "eof"
)

// SDSIn publishes data written through io.Writer as SDS request messages to a ZMQ PUB socket.
type SDSIn struct {
	*base.Handler

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
		Handler: base.New(),
	}
}

// SetConfig adds the parameters of the handler from the config.
func (publisher *SDSIn) SetConfig(handler *config.Handler) {
	handler.Type = config.PublisherType
	publisher.Handler.SetConfig(handler)
}

// SetLogger sets the logger for this publisher.
func (publisher *SDSIn) SetLogger(parent *log.Logger) error {
	if err := publisher.Handler.SetLogger(parent); err != nil {
		return err
	}

	publisher.mu.Lock()
	defer publisher.mu.Unlock()

	publisher.logger = parent.Child(publisher.Config().Id)
	return nil
}

// Type returns the handler type.
func (publisher *SDSIn) Type() config.HandlerType {
	return config.PublisherType
}

// Route returns an error because SDSIn publishes io.Writer messages and has no request routes.
func (publisher *SDSIn) Route(_ string, _ interface{}) error {
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
	if publisher.Config() == nil {
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
	handlerConfig := publisher.Config()
	publisher.mu.Unlock()

	go publisher.run(handlerConfig, queue, done, ready, stopped)

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

// Start starts the publisher socket and the handler manager.
func (publisher *SDSIn) Start() error {
	if publisher.Config() == nil {
		return fmt.Errorf("configuration not set")
	}
	if publisher.Manager == nil {
		return fmt.Errorf("handler manager not set. call SetConfig and SetLogger first")
	}

	if err := publisher.setManagerRoutes(); err != nil {
		return err
	}
	if err := publisher.startPublisher(); err != nil {
		return fmt.Errorf("sdsin.startPublisher: %w", err)
	}
	if err := publisher.Manager.Start(); err != nil {
		_ = publisher.Close()
		return fmt.Errorf("handler_manager.Start: %w", err)
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

func (publisher *SDSIn) setManagerRoutes() error {
	onStatus := func(req message.RequestInterface) message.ReplyInterface {
		status := publisher.publisherStatus()
		params := datatype.New()
		if status == publisherRunning {
			params.Set("status", handler_manager.Ready)
		} else {
			params.Set("status", handler_manager.Incomplete).
				Set("parts", datatype.New().Set(partPublisher, status))
		}
		return req.Ok(params)
	}

	onClose := func(req message.RequestInterface) message.ReplyInterface {
		part, err := req.RouteParameters().StringValue("part")
		if err != nil {
			return req.Fail(fmt.Sprintf("req.Parameters.GetString('part'): %v", err))
		}
		if part != partPublisher {
			return req.Fail(fmt.Sprintf("unknown part '%s' to stop", part))
		}
		if err := publisher.Close(); err != nil {
			return req.Fail(err.Error())
		}
		return req.Ok(datatype.New())
	}

	onRunPart := func(req message.RequestInterface) message.ReplyInterface {
		part, err := req.RouteParameters().StringValue("part")
		if err != nil {
			return req.Fail(fmt.Sprintf("req.Parameters.GetString('part'): %v", err))
		}
		if part != partPublisher {
			return req.Fail(fmt.Sprintf("unknown part '%s' to start", part))
		}
		if err := publisher.startPublisher(); err != nil {
			return req.Fail(fmt.Sprintf("sdsin.startPublisher: %v", err))
		}
		return req.Ok(datatype.New())
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

	onParts := func(req message.RequestInterface) message.ReplyInterface {
		return req.Ok(datatype.New().
			Set("parts", []string{partPublisher}).
			Set("message_types", []string{"queue_length"}))
	}

	onInstanceAmount := func(req message.RequestInterface) message.ReplyInterface {
		return req.Ok(datatype.New().Set("instance_amount", 0))
	}

	onUnsupported := func(req message.RequestInterface) message.ReplyInterface {
		return req.Fail("sdsin doesn't use instances")
	}

	if err := publisher.Manager.Route(config.HandlerStatus, onStatus); err != nil {
		return fmt.Errorf("overwriting handler manager 'status' failed: %w", err)
	}
	if err := publisher.Manager.Route(config.ClosePart, onClose); err != nil {
		return fmt.Errorf("overwriting handler manager 'close' failed: %w", err)
	}
	if err := publisher.Manager.Route(config.RunPart, onRunPart); err != nil {
		return fmt.Errorf("overwriting handler manager 'run' failed: %w", err)
	}
	if err := publisher.Manager.Route(config.MessageAmount, onMessageAmount); err != nil {
		return fmt.Errorf("overwriting handler manager 'message_amount' failed: %w", err)
	}
	if err := publisher.Manager.Route(config.Parts, onParts); err != nil {
		return fmt.Errorf("overwriting handler manager 'parts' failed: %w", err)
	}
	if err := publisher.Manager.Route(config.InstanceAmount, onInstanceAmount); err != nil {
		return fmt.Errorf("overwriting handler manager 'instance_amount' failed: %w", err)
	}
	if err := publisher.Manager.Route(config.AddInstance, onUnsupported); err != nil {
		return fmt.Errorf("overwriting handler manager 'add_instance' failed: %w", err)
	}
	if err := publisher.Manager.Route(config.DeleteInstance, onUnsupported); err != nil {
		return fmt.Errorf("overwriting handler manager 'delete_instance' failed: %w", err)
	}

	return nil
}

func (publisher *SDSIn) run(handlerConfig *config.Handler, queue <-chan message.RequestInterface, done <-chan struct{}, ready chan<- error, stopped chan<- struct{}) {
	defer close(stopped)

	socket, err := zmq.NewSocket(config.SocketType(config.PublisherType))
	if err != nil {
		ready <- fmt.Errorf("new_socket('%s'): %v", config.PublisherType, err)
		return
	}

	url := config.ExternalUrl(handlerConfig.Id, handlerConfig.Port)
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
	reqStr, err := req.ZmqEnvelope()
	if err != nil {
		publisher.logger.Error("req.ZmqEnvelope", "error", err)
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

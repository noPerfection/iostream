package sdsin

import (
	"strings"
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/log"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/suite"
)

type TestSDSInSuite struct {
	suite.Suite
	publisher *SDSIn
	config    *config.Handler
	logger    *log.Logger
	sub       *zmq.Socket
	poller    *zmq.Poller
}

func (test *TestSDSInSuite) SetupTest() {
	s := &test.Suite

	logger, err := log.New("sdsin", false)
	s.Require().NoError(err, "failed to create logger")
	test.logger = logger

	test.publisher = New()
	category := strings.ReplaceAll(s.T().Name(), "/", "_")
	test.config = config.NewInternalHandler(config.PublisherType, category, category)

	s.Require().Error(test.publisher.SetLogger(test.logger))
	test.publisher.SetConfig(test.config)
	s.Require().NoError(test.publisher.SetLogger(test.logger))
}

func (test *TestSDSInSuite) TearDownTest() {
	s := &test.Suite

	if test.sub != nil {
		s.Require().NoError(test.sub.Close())
	}
	if test.publisher != nil && test.publisher.socket != nil {
		s.Require().NoError(test.publisher.Close())
	}
	if test.publisher != nil && test.publisher.Manager != nil {
		req := &message.Request{Command: config.HandlerClose, Parameters: datatype.New()}
		test.publisher.Manager.SetClose(req)
		time.Sleep(time.Millisecond * 20)
	}
}

func (test *TestSDSInSuite) subscribe() {
	s := &test.Suite

	sub, err := zmq.NewSocket(zmq.SUB)
	s.Require().NoError(err)
	s.Require().NoError(sub.SetSubscribe(""))
	s.Require().NoError(sub.Connect(config.ExternalUrl(test.config.Id, test.config.Port)))

	test.sub = sub
	test.poller = zmq.NewPoller()
	test.poller.Add(test.sub, zmq.POLLIN)
}

func (test *TestSDSInSuite) Test_10_WritePublishesRequest() {
	s := &test.Suite

	s.Require().NoError(test.publisher.Start())
	test.subscribe()
	time.Sleep(time.Millisecond * 100)

	payload := []byte("hello subscribers")
	written, err := test.publisher.Write(payload)
	s.Require().NoError(err)
	s.Require().Equal(len(payload), written)

	polled, err := test.poller.Poll(time.Second * 2)
	s.Require().NoError(err)
	s.Require().Len(polled, 1, "timeout for subscribing")

	received, err := test.sub.RecvMessage(0)
	s.Require().NoError(err)
	req, err := message.NewReq(received)
	s.Require().NoError(err)
	s.Require().Equal("io", req.CommandName())

	row, err := req.RouteParameters().StringValue("row")
	s.Require().NoError(err)
	s.Require().Equal(string(payload), row)
}

func (test *TestSDSInSuite) Test_20_WriteRequiresRunningPublisher() {
	s := &test.Suite

	written, err := test.publisher.Write([]byte("not running"))
	s.Require().Error(err)
	s.Require().Zero(written)
}

func (test *TestSDSInSuite) Test_30_StartInBgWaitsUntilPublisherReady() {
	s := &test.Suite

	s.Require().NoError(test.publisher.StartInBg())

	payload := []byte("ready after background start")
	written, err := test.publisher.Write(payload)
	s.Require().NoError(err)
	s.Require().Equal(len(payload), written)
}

func (test *TestSDSInSuite) Test_40_CloseStopsPublisher() {
	s := &test.Suite

	s.Require().NoError(test.publisher.Start())
	s.Require().NoError(test.publisher.Close())
	s.Require().Nil(test.publisher.socket)

	written, err := test.publisher.Write([]byte("closed"))
	s.Require().Error(err)
	s.Require().Zero(written)
}

func (test *TestSDSInSuite) Test_50_ClosePublishesEOF() {
	s := &test.Suite

	s.Require().NoError(test.publisher.Start())
	test.subscribe()
	time.Sleep(time.Millisecond * 100)

	s.Require().NoError(test.publisher.Close())
	s.Require().Nil(test.publisher.socket)

	polled, err := test.poller.Poll(time.Second * 2)
	s.Require().NoError(err)
	s.Require().Len(polled, 1, "timeout for eof")

	received, err := test.sub.RecvMessage(0)
	s.Require().NoError(err)
	req, err := message.NewReq(received)
	s.Require().NoError(err)
	s.Require().Equal("eof", req.CommandName())
}

func TestSDSIn(t *testing.T) {
	suite.Run(t, new(TestSDSInSuite))
}

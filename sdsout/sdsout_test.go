package sdsout

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/noPerfection/datatype"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/suite"
)

type TestSDSOutSuite struct {
	suite.Suite
	out    *SDSOut
	pub    *zmq.Socket
	config *config.Handler
}

func (test *TestSDSOutSuite) SetupTest() {
	category := strings.ReplaceAll(test.T().Name(), "/", "_")
	test.config = config.NewInternalHandler(config.PublisherType, category, category)
	test.out = New()
}

func (test *TestSDSOutSuite) TearDownTest() {
	s := &test.Suite

	if test.out != nil && test.out.socket != nil {
		s.Require().NoError(test.out.Close())
	}
	if test.pub != nil {
		s.Require().NoError(test.pub.Close())
	}
}

func (test *TestSDSOutSuite) startPublisher() {
	s := &test.Suite

	pub, err := zmq.NewSocket(zmq.PUB)
	s.Require().NoError(err)
	s.Require().NoError(pub.Bind(config.ExternalUrl(test.config.Id, test.config.Port)))
	test.pub = pub
}

func (test *TestSDSOutSuite) publish(command string, params datatype.KeyValue) {
	s := &test.Suite

	req := &message.Request{Command: command, Parameters: params}
	envelope, err := req.ZmqEnvelope()
	s.Require().NoError(err)
	_, err = test.pub.SendMessage(envelope)
	s.Require().NoError(err)
}

func (test *TestSDSOutSuite) Test_10_WritesIoRowsToConfiguredWriter() {
	s := &test.Suite

	test.startPublisher()

	var output bytes.Buffer
	test.out.SetConfig(test.config, &output)
	s.Require().NoError(test.out.StartInBg())

	time.Sleep(time.Millisecond * 100)
	test.publish("io", datatype.New().Set("row", "hello from sdsin"))

	s.Eventually(func() bool {
		return output.String() == "hello from sdsin"
	}, time.Second*2, time.Millisecond*10)
}

func (test *TestSDSOutSuite) Test_20_IgnoresNonIoMessages() {
	s := &test.Suite

	test.startPublisher()

	var output bytes.Buffer
	test.out.SetConfig(test.config, &output)
	s.Require().NoError(test.out.StartInBg())

	time.Sleep(time.Millisecond * 100)
	test.publish("other", datatype.New().Set("row", "ignored"))

	time.Sleep(time.Millisecond * 50)
	s.Require().Empty(output.String())
}

func (test *TestSDSOutSuite) Test_30_StartRequiresConfig() {
	s := &test.Suite

	s.Require().Error(test.out.StartInBg())
}

func (test *TestSDSOutSuite) Test_40_CloseStopsSubscriber() {
	s := &test.Suite

	test.startPublisher()

	var output bytes.Buffer
	test.out.SetConfig(test.config, &output)
	s.Require().NoError(test.out.StartInBg())
	s.Require().NoError(test.out.Close())
	s.Require().Nil(test.out.socket)
}

func (test *TestSDSOutSuite) Test_50_EOFCloseStopsSubscriber() {
	s := &test.Suite

	test.startPublisher()

	var output bytes.Buffer
	test.out.SetConfig(test.config, &output)
	s.Require().NoError(test.out.StartInBg())

	time.Sleep(time.Millisecond * 100)
	test.publish("eof", datatype.New())

	s.Eventually(func() bool {
		return test.out.socket == nil
	}, time.Second*2, time.Millisecond*10)

	s.Require().NoError(test.out.Close())
}

func TestSDSOut(t *testing.T) {
	suite.Run(t, new(TestSDSOutSuite))
}

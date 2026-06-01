# iostream

`iostream` lets Go programs send `io.Writer` output to another process, component, terminal, file, or buffer without changing the code that produced it.

## Requirements

- Go 1.19 or newer.
- ZeroMQ runtime and development headers for `github.com/pebbe/zmq4`.
- CGO enabled, because `zmq4` links against the system ZeroMQ library.

On Debian or Ubuntu:

```bash
sudo apt update
sudo apt install libzmq3-dev pkg-config
```

In other OS, download the Zmq library, since this module requires zeromq.

Run commands that build or test the project with CGO enabled:

```bash
CGO_ENABLED=1 go test ./...
CGO_ENABLED=1 go run ./...
```

> For other OS, refer to zmq, to set it up.

## Installation

```bash
go get github.com/noPerfection/iostream
```

Use the producer and consumer packages:

```go
import (
	"github.com/noPerfection/iostream/in"
	"github.com/noPerfection/iostream/out"
)
```

## Why

Many Go libraries already write useful output to an `io.Writer`: logs, command output, progress messages, reports, test output, and text events.

`iostream` gives that output a path to another place. The producer writes as usual; the consumer receives the stream and writes it wherever it needs to go.

## What It Does

- Accepts writes from ordinary Go code.
- Streams those writes across an application boundary.
- Delivers the received data to an `io.Writer`.
- Keeps stream plumbing separate from the code producing the output.

## Example Uses

- Send background worker logs to a live terminal.
- Pipe command output into a UI.
- Forward progress messages from one process to another.
- Collect streamed text into a file or buffer.
- Connect existing `io.Writer` code to a remote consumer.

## Mental Model

```text
producer writes bytes -> iostream carries them -> consumer writes bytes
```

The producer only needs something that behaves like an `io.Writer`. The consumer only needs somewhere to write the received data.

## Packages

- `in` publishes bytes written through `io.Writer`.
- `out` subscribes to an `in` stream and writes received bytes to another `io.Writer`.

## Tutorial

Use `iostream` when Go code wants an `io.Writer`, but the output needs to be consumed somewhere else instead of staying on stdout. For example, write terminal logs into `in`, then receive them from another process with `out`.

Each `Write(p)` broadcasts an iostream protocol request:

- `Command`: `"io"`
- `Parameters["row"]`: `string(p)`

```go
import (
	"fmt"
	"log"

	"github.com/noPerfection/iostream/in"
	"github.com/noPerfection/protocol/handler/config"
	loglib "github.com/noPerfection/log"
)

logger, err := loglib.New("events", false)
if err != nil {
	log.Fatal(err)
}

input := in.New()
cfg := config.New(config.PublisherType, "tmp/events_iostream", "events", 0)
// Port 0 + "tmp" prefix binds ipc:///tmp/events_iostream.

// TCP version, for subscribers in another process or host:
// cfg := config.New(config.PublisherType, "localhost", "events", 5555)
//
// In-process version, for subscribers in the same process:
// cfg := config.New(config.PublisherType, "events", "events", 0)
// This binds inproc://events when the ID does not start with "tmp".

input.SetConfig(cfg)
if err := input.SetLogger(logger); err != nil {
	log.Fatal(err)
}

// StartInBg returns only after the publisher socket and control handler are ready.
if err := input.StartInBg(); err != nil {
	log.Fatal(err)
}
defer input.Close()

fmt.Fprintln(input, "terminal log line")
```

Use `out` to receive those rows and write them somewhere else. If no writer is passed, it writes to `os.Stdout`.

```go
import (
	"log"

	"github.com/noPerfection/iostream/out"
)

output := out.New()
output.SetConfig(cfg) // optional second arg: any io.Writer; defaults to os.Stdout
if err := output.StartInBg(); err != nil {
	log.Fatal(err)
}
defer output.Close()
```

Under the hood, subscribers connect to the publisher URL and parse the received message as an iostream protocol request:

```go
import (
	"fmt"
	"log"

	"github.com/noPerfection/protocol/message"
	zmq "github.com/pebbe/zmq4"
)

sub, err := zmq.NewSocket(zmq.SUB)
if err != nil {
	log.Fatal(err)
}
defer sub.Close()

if err := sub.SetSubscribe(""); err != nil {
	log.Fatal(err)
}

subscriberURL := cfg.ClientUrl()
// IPC example from above: ipc:///tmp/events_iostream
// TCP subscriber URL: tcp://{cfg.Id}:{cfg.Port}
// In-process subscriber URL: inproc://{cfg.Id}, and it must be in the same process.
if err := sub.Connect(subscriberURL); err != nil {
	log.Fatal(err)
}

raw, err := sub.RecvMessage(0)
if err != nil {
	log.Fatal(err)
}

req, err := (&message.MessagePacker{}).DeserializeRequest(raw)
if err != nil {
	log.Fatal(err)
}

row, err := req.RouteParameters().StringValue("row")
if err != nil {
	log.Fatal(err)
}
fmt.Println(row)
```

For TCP, publishers bind with `cfg.HandlerUrl()`. Local IDs (`localhost`, `127.0.0.*`, or an empty host) bind to `tcp://*:{port}`; other IDs bind to `tcp://{id}:{port}`. Subscribers connect with `cfg.ClientUrl()` (`tcp://{id}:{port}`). For in-process subscribers, keep `Port` as `0` and use an ID without the `tmp` prefix so the URL is `inproc://{id}`.

---

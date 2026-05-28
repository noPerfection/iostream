# iostream

`iostream` lets Go programs send `io.Writer` output to another process, component, terminal, file, or buffer without changing the code that produced it.

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

### Tutorial

Use **iostream** when some Go code wants an `io.Writer`, but you want the output to go to another SDS place instead of stdout. For example, write terminal logs into `sdsin`, then read them in another terminal with `sdsout`.

Each `Write(p)` broadcasts an SDS `message.Request`:

- `Command`: `"io"`
- `Parameters["row"]`: `string(p)`

```go
import (
	"fmt"
	"log"

	zmq "github.com/pebbe/zmq4"
	"github.com/noPerfection/protocol/message"
	"github.com/noPerfection/protocol/handler/config"
	"github.com/noPerfection/iostream/sdsin"
	"github.com/noPerfection/iostream/sdsout"
	loglib "github.com/noPerfection/log"
)

logger, err := loglib.New("events", false)
if err != nil {
	log.Fatal(err)
}

in := sdsin.New()
cfg := config.NewInternalHandler(config.PublisherType, "events", "events")
cfg.Id = "tmp/events_sdsin" // Port 0 + "tmp" prefix binds ipc:///tmp/events_sdsin.

// TCP version, for subscribers in another process or host:
// cfg := config.NewHandler(config.PublisherType, "events", "events", 5555)
//
// In-process version, for subscribers in the same process:
// cfg := config.NewInternalHandler(config.PublisherType, "events", "events")
// This binds inproc://events when the ID does not start with "tmp".

in.SetConfig(cfg)
if err := in.SetLogger(logger); err != nil {
	log.Fatal(err)
}

// StartInBg returns only after the PUB socket and manager are ready.
if err := in.StartInBg(); err != nil {
	log.Fatal(err)
}

fmt.Fprintln(in, "terminal log line")
```

Use **SDSOut** to receive those rows and write them somewhere else. If no writer is passed, it writes to `os.Stdout`.

```go
out := sdsout.New()
out.SetConfig(cfg) // optional second arg: any io.Writer; defaults to os.Stdout
if err := out.StartInBg(); err != nil {
	log.Fatal(err)
}
defer out.Close()
```

Under the hood, subscribers connect to the publisher URL and parse the received message as an SDS request:

```go
sub, err := zmq.NewSocket(zmq.SUB)
if err != nil {
	log.Fatal(err)
}
defer sub.Close()

if err := sub.SetSubscribe(""); err != nil {
	log.Fatal(err)
}

subscriberURL := config.ConnectUrl(cfg.Id, cfg.Port)
// IPC example from above: ipc:///tmp/events_sdsin
// TCP subscriber URL: tcp://{cfg.Id}:{cfg.Port}
// In-process subscriber URL: inproc://{cfg.Id}, and it must be in the same process.
if err := sub.Connect(subscriberURL); err != nil {
	log.Fatal(err)
}

raw, err := sub.RecvMessage(0)
if err != nil {
	log.Fatal(err)
}

req, err := message.NewReq(raw)
if err != nil {
	log.Fatal(err)
}

row, err := req.RouteParameters().StringValue("row")
if err != nil {
	log.Fatal(err)
}
fmt.Println(row)
```

For TCP, publishers bind with `config.ExternalUrl(cfg.Id, cfg.Port)`. Local IDs (`localhost` or `127.0.0.*`) bind to `tcp://*:{port}`; other IDs bind to `tcp://{id}:{port}`. Subscribers connect with `config.ConnectUrl(cfg.Id, cfg.Port)` (`tcp://{id}:{port}`). For in-process subscribers, keep `Port` as `0` and use an ID without the `tmp` prefix so the URL is `inproc://{id}`.

---

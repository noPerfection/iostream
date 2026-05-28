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


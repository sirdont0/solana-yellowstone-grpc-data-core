// corectl captures, verifies and exports source-local Yellowstone raw WAL data.
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
)

const usage = `corectl — Yellowstone raw WAL utility

Usage:
  corectl capture [flags]     Capture Yellowstone updates into a durable WAL
  corectl probe [flags]       Sample provider RPCs/filters and emit redacted JSON evidence
  corectl inspect [flags]     Summarize a closed WAL
  corectl verify [flags]      Validate a closed WAL (CRC, sequence, identity, protobuf)
  corectl replay [flags]      Export WAL records as NDJSON in source append order
  corectl migrate [flags]     Verify v1 WAL and optionally publish a v2 copy

Run "corectl <command> -h" for command-specific flags.
Guide: docs/CLI.md
`

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "corectl:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		_, _ = io.WriteString(stderr, usage)
		return errors.New("command required")
	}
	switch args[0] {
	case "help", "-h", "--help":
		_, _ = io.WriteString(stdout, usage)
		return nil
	case "capture":
		return capture(args[1:], stdout, stderr)
	case "probe":
		return probe(args[1:], stdout, stderr)
	case "inspect", "verify", "replay", "migrate":
		return offline(args[0], args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown command %q (run corectl help)", args[0])
	}
}

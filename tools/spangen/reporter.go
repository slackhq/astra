package main

import (
	"fmt"
	proto "spangen/spangen/generated"
)

// Interface for reporting a span
type Reporter interface {
	Report(span *proto.Span) error
}

// does nothing
type noopReporter struct{}

func (x *noopReporter) Report(span *proto.Span) error { return nil }

// does nothing
type consoleReporter struct{}

func (x *consoleReporter) Report(span *proto.Span) error {
	fmt.Printf("{}", span)
	return nil
}

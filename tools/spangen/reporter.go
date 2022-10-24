package main

import (
	"spangen/spangen/proto"
)

// Interface for reporting a span
type Reporter interface {
	Report(span *proto.Span) error
}

// does nothing
type noopReporter struct{}

func (x *noopReporter) Report(span *proto.Span) error { return nil }

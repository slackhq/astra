package main

import proto "spangen/spangen/generated"

// Interface for reporting a span
type Reporter interface {
	Report(span *proto.Span) error
}

// does nothing
type noopReporter struct{}

func (x *noopReporter) Report(span *proto.Span) error { return nil }

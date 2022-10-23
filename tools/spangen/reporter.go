package main

import (
	"bytes"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"io/ioutil"
	"net/http"
	"slack-github.com/slack/murron/gen/proto/tracepb"
	client "slack-github.com/slack/murron/pkg/clients/go"
	"slack-github.com/slack/murron/pkg/core/lib"
	"slack-github.com/slack/murron/pkg/traces"
)

// Interface for reporting a span
type Reporter interface {
	Report(span *tracepb.Span) error
}

// does nothing
type noopReporter struct{}

func (x *noopReporter) Report(span *tracepb.Span) error { return nil }

// reports to murron-agent
type agentReporter struct{}

func (x *agentReporter) Report(span *tracepb.Span) error {
	// read the agent config
	config, err := lib.ReadConfig(configFile)
	if err != nil {
		return fmt.Errorf("lib.ReadConfig() failed: %v", err)
	}

	// create the murron writer config
	writerConfig := client.NewDefaultConfig()
	writerConfig.AsyncEnabled = false
	writerConfig.SocketPath = config.BinaryInputSocket
	writerConfig.BacklogPath = config.BinaryBacklog
	writerConfig.Logger = nil // suppress logging in the writer

	// create the murron writer
	murronWriter := client.NewMurronWriter(writerConfig)

	// format as binary murron message
	// this method returns a slice, but it'll always only include a singe message since
	// we're formatting a single span.
	messages, err := (traces.Spans)([]*tracepb.Span{span}).FormatBinaryMurronMessage()
	if err != nil {
		return fmt.Errorf("span.FormatBinaryMurronMessage() failed: %v", err)
	}

	// write the messages to murron-agent
	for _, message := range messages {
		err := murronWriter.WriteMessage(message)
		if err != nil {
			return fmt.Errorf("murronWriter.WriteMessage() failed: %v", err)
		}
	}
	return nil
}

// reports to wallace http endpoint
type wallaceReporter struct{ endpoint string }

const wallaceRetries = 2

func (x *wallaceReporter) Report(span *tracepb.Span) error {
	// serialize the span as tracepb.ListOfSpans
	payload, err := proto.Marshal(&tracepb.ListOfSpans{Spans: []*tracepb.Span{span}})
	if err != nil {
		return fmt.Errorf("proto.Marshal failed: %v", err)
	}

	// build the http request
	req, err := http.NewRequest(http.MethodPost,
		x.endpoint+"/traces/v1/list_of_spans/proto",
		bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("http.NewRequest failed: %v", err)
	}

	// make the request and retry on failure
	for i := 0; i < wallaceRetries; i++ {
		if err = x.do(req); err != nil {
			continue
		}
		return nil
	}
	return err
}

func (x *wallaceReporter) do(req *http.Request) error {
	// make the request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("httpClient.Do() failed: %v", err)
	}

	// check the response
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("non 200 response: %d %s", resp.StatusCode, string(body))
	}
	return nil
}

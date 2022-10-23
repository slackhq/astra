package main

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net"
	"net/http/httptest"
	"os"
	"slack-github.com/slack/murron/pkg/core/format/binary"
	"slack-github.com/slack/murron/pkg/inputs/wallace"
	"slack-github.com/slack/murron/pkg/traces"
	"testing"

	"slack-github.com/slack/murron/gen/proto/tracepb"
)

func Test_noopReporter_Report(t *testing.T) {
	type args struct {
		span *tracepb.Span
	}
	for _, tt := range []struct {
		name    string
		x       *noopReporter
		args    args
		wantErr bool
	}{
		{
			name:    "success",
			x:       new(noopReporter),
			args:    args{},
			wantErr: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			x := &noopReporter{}
			if err := x.Report(tt.args.span); (err != nil) != tt.wantErr {
				t.Errorf("noopReporter.Report() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_agentReporter_Report(t *testing.T) {
	require.NoError(t, os.MkdirAll("tmp", 0700))
	defer os.RemoveAll("tmp")

	// start a socket listener to receive the spans
	socketPath := "tmp/test_socket"
	os.Remove(socketPath) // Try to remove it if it exists
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err, "net.Listen()")
	defer listener.Close()

	// make a mock config that uses our test socket
	configFile = "tmp/config.json"
	require.NoError(t, ioutil.WriteFile(
		configFile,
		[]byte(`{"BinaryInputSocket": "tmp/test_socket"}`),
		0600,
	), "ioutil.WriteFile()")

	type args struct {
		span *tracepb.Span
	}
	for _, tt := range []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "success",
			args: args{
				span: &tracepb.Span{
					Id:                   []byte("mock span id"),
					ParentId:             []byte("mock parent id"),
					TraceId:              []byte("mock trace id"),
					Name:                 "test span",
					StartTimestampMicros: 15,
					DurationMicros:       25,
				},
			},
			wantErr: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)
			gotErr := new(agentReporter).Report(tt.args.span)

			if tt.wantErr {
				assert.Error(gotErr, "agentReporter.Report()")
			} else {
				require.NoError(gotErr, "agentReporter.Report()")

				// read the message from the connection
				connection, err := listener.Accept()
				require.NoError(err, "listener.Accept()")
				defer connection.Close()
				var gotMessage binary.BinaryMurronMessage
				_, err = gotMessage.ReadFrom(connection)
				require.NoError(err, "BinaryMurronMessage.ReadFrom()")

				// read the spans from the message
				gotSpans, err := traces.UnpackSpansFromMurronMessage(gotMessage.ToMurronMessage())
				require.NoError(err, "traces.UnpackSpansFromMurronMessage()")
				require.Equal(1, len(gotSpans), "len(gotSpans)")

				// validate each span field
				expectedSpan, gotSpan := tt.args.span, gotSpans[0]
				assert.Equal(string(expectedSpan.Id), string(gotSpan.Id), "span.Id")
				assert.Equal(string(expectedSpan.ParentId), string(gotSpan.ParentId), "span.ParentId")
				assert.Equal(string(expectedSpan.TraceId), string(gotSpan.TraceId), "span.TraceId")
				assert.Equal(expectedSpan.Name, gotSpan.Name, "span.Name")
				assert.Equal(expectedSpan.StartTimestampMicros, gotSpan.StartTimestampMicros, "span.StartTimestampMicros")
				assert.Equal(expectedSpan.DurationMicros, gotSpan.DurationMicros, "span.DurationMicros")

			}
		})
	}
}

func Test_wallaceReporter_Report(t *testing.T) {
	type args struct {
		span *tracepb.Span
	}
	for _, tt := range []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "bad span",
			args: args{
				span: new(tracepb.Span),
			},
			wantErr: true,
		},
		{
			name: "success",
			args: args{
				span: &tracepb.Span{
					Id:                   []byte("mock span id"),
					ParentId:             []byte("mock parent id"),
					TraceId:              []byte("mock trace id"),
					Name:                 "test span",
					StartTimestampMicros: 15,
					DurationMicros:       25,
				},
			},
			wantErr: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			require := require.New(t)
			wallaceCollector := wallace.NewService("none", false)
			testServer := httptest.NewServer(wallaceCollector.Handler())
			x := &wallaceReporter{endpoint: testServer.URL}
			gotErr := x.Report(tt.args.span)
			if tt.wantErr {
				assert.Error(gotErr, "wallaceReporter.Report()")
				wallaceCollector.Close()
			} else {
				require.NoError(gotErr, "wallaceReporter.Report()")
				wallaceCollector.Close()
				// get a murron message from wallace collector
				gotMessage, ok := <-wallaceCollector.Output()
				if assert.True(ok, "wallaceCollector.Output()") {
					// read the span from the murron message
					gotSpans, err := traces.UnpackSpansFromMurronMessage(gotMessage)
					require.NoError(err, "traces.UnpackSpansFromMurronMessage()")
					require.Equal(1, len(gotSpans), "len(gotSpans)")
					// validate each span field
					expectedSpan, gotSpan := tt.args.span, gotSpans[0]
					assert.Equal(string(expectedSpan.Id), string(gotSpan.Id), "span.Id")
					assert.Equal(string(expectedSpan.ParentId), string(gotSpan.ParentId), "span.ParentId")
					assert.Equal(string(expectedSpan.TraceId), string(gotSpan.TraceId), "span.TraceId")
					assert.Equal(expectedSpan.Name, gotSpan.Name, "span.Name")
					assert.Equal(expectedSpan.StartTimestampMicros, gotSpan.StartTimestampMicros, "span.StartTimestampMicros")
					assert.Equal(expectedSpan.DurationMicros, gotSpan.DurationMicros, "span.DurationMicros")
				}
			}
		})
	}
}

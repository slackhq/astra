package main

import (
	proto "spangen/spangen/generated"
	"testing"
)

func Test_noopReporter_Report(t *testing.T) {
	type args struct {
		span *proto.Span
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

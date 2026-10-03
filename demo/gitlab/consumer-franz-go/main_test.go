package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestPrintsAllEventFieldsWithoutChangingLargeIDs(t *testing.T) {
	values := []string{
		`{"object_kind":"merge_request","id":9007199254740993,"future":{"unknown":[true,null,"value"]}}`,
		`{"object_kind":"push","commits":[{"message":"keep me","id":"abc"}]}`,
		`{"object_kind":"issue","id":9223372036854775807,"labels":["bug"],"unknown":1.234567890123456789}`,
		`["future-event",{"unknown":18446744073709551615}]`,
	}
	var records []*kgo.Record
	for _, value := range values {
		records = append(records, &kgo.Record{Value: []byte(value)})
	}
	var output bytes.Buffer
	committed := false
	var printedAtCommit []byte
	err := processBatch(records, &output, func() error {
		committed = true
		printedAtCommit = bytes.Clone(output.Bytes())
		return nil
	})
	if err != nil || !committed {
		t.Fatalf("error = %v, committed = %v", err, committed)
	}
	if !bytes.Contains(printedAtCommit, []byte("\n  \"object_kind\"")) {
		t.Fatalf("output is not indented: %s", printedAtCommit)
	}
	decoder := json.NewDecoder(bytes.NewReader(printedAtCommit))
	for _, value := range values {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			t.Fatal(err)
		}
		var compact bytes.Buffer
		if err := json.Compact(&compact, raw); err != nil || compact.String() != value {
			t.Fatalf("JSON changed: %s, want %s, error = %v", compact.String(), value, err)
		}
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("unexpected extra output: %s, error = %v", extra, err)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestFailuresDoNotCommit(t *testing.T) {
	for _, test := range []struct {
		name   string
		values []string
		output io.Writer
	}{
		{"malformed JSON", []string{"{"}, &bytes.Buffer{}},
		{"later malformed JSON", []string{`{"event":"push"}`, "{"}, &bytes.Buffer{}},
		{"write failure", []string{`{"event":"push"}`}, failingWriter{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var records []*kgo.Record
			for _, value := range test.values {
				records = append(records, &kgo.Record{Value: []byte(value)})
			}
			committed := false
			err := processBatch(records, test.output, func() error { committed = true; return nil })
			if err == nil || committed {
				t.Fatalf("error = %v, committed = %v", err, committed)
			}
		})
	}
}

func TestCommitFailurePropagates(t *testing.T) {
	want := errors.New("commit failed")
	err := processBatch([]*kgo.Record{{Value: []byte(`{"event":"push"}`)}}, &bytes.Buffer{}, func() error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/krabka-io/krabka-streams-go/columnar"
	"github.com/twmb/franz-go/pkg/kgo"
)

const validMR = `{"object_kind":"merge_request","project":{"id":9},"object_attributes":{"id":17,"iid":3,"title":"Fix example","state":"opened","action":"update"}}`

func record(body string) *kgo.Record {
	return &kgo.Record{
		Topic: "input", Partition: 2, Offset: 7, Timestamp: time.UnixMilli(1000), Key: []byte("17"), Value: []byte(body),
		Headers: []kgo.RecordHeader{{Key: "webhook-id", Value: []byte("delivery-42")}},
	}
}

func TestTopologyPreservesMetadataAndExtractsSummary(t *testing.T) {
	topology, err := buildTopology("input")
	if err != nil {
		t.Fatal(err)
	}
	defer topology.Close()
	r := record(validMR)
	produced, err := topology.RunBatch("input", []columnar.ConsumedRecord{
		columnar.NewConsumedRecord(r.Key, r.Value, r.Timestamp.UnixMilli(), int(r.Partition), r.Offset,
			columnar.RecordHeader{Key: "webhook-id", Value: []byte("delivery-42")}),
	})
	if err != nil || len(produced) != 1 {
		t.Fatalf("outputs = %v, error = %v", produced, err)
	}
	var summary mrSummary
	if err := json.Unmarshal(produced[0].Record.Value, &summary); err != nil {
		t.Fatal(err)
	}
	want := mrSummary{MRID: 17, ProjectID: 9, IID: 3, Title: "Fix example", State: "opened", Action: "update"}
	if summary != want {
		t.Fatalf("summary = %+v, want %+v", summary, want)
	}
	if !bytes.Equal(produced[0].Record.Key, r.Key) || !reflect.DeepEqual(produced[0].Record.Headers, []columnar.RecordHeader{{Key: "webhook-id", Value: []byte("delivery-42")}}) {
		t.Fatalf("metadata changed: %+v", produced[0].Record)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestBatchCommitsOnlyAfterValidOutput(t *testing.T) {
	for _, test := range []struct {
		name    string
		records []*kgo.Record
		output  io.Writer
		wantErr bool
	}{
		{"valid", []*kgo.Record{record(validMR)}, &bytes.Buffer{}, false},
		{"malformed", []*kgo.Record{record("{")}, &bytes.Buffer{}, true},
		{"not MR", []*kgo.Record{record(`{"object_kind":"issue"}`)}, &bytes.Buffer{}, true},
		{"missing ID", []*kgo.Record{record(`{"object_kind":"merge_request","project":{"id":9},"object_attributes":{"iid":3}}`)}, &bytes.Buffer{}, true},
		{"missing project", []*kgo.Record{record(`{"object_kind":"merge_request","object_attributes":{"id":17,"iid":3}}`)}, &bytes.Buffer{}, true},
		{"missing IID", []*kgo.Record{record(`{"object_kind":"merge_request","project":{"id":9},"object_attributes":{"id":17}}`)}, &bytes.Buffer{}, true},
		{"writer failure", []*kgo.Record{record(validMR)}, failingWriter{}, true},
		{"later decode failure", []*kgo.Record{record(validMR), record("{")}, &bytes.Buffer{}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			topology, err := buildTopology("input")
			if err != nil {
				t.Fatal(err)
			}
			defer topology.Close()
			committed := false
			err = processBatch(topology, "input", test.records, test.output, func() error { committed = true; return nil })
			if (err != nil) != test.wantErr || committed == test.wantErr {
				t.Fatalf("error = %v, committed = %v", err, committed)
			}
			if !test.wantErr {
				var output struct {
					mrSummary
					DeliveryID string `json:"delivery_id"`
				}
				if err := json.Unmarshal(test.output.(*bytes.Buffer).Bytes(), &output); err != nil || output.DeliveryID != "delivery-42" || output.MRID != 17 {
					t.Fatalf("output = %+v, error = %v", output, err)
				}
			}
		})
	}
}

func TestInvalidMetadataAndCommitFailure(t *testing.T) {
	for _, kind := range []string{"missing delivery", "duplicate delivery", "wrong key", "commit failure"} {
		t.Run(kind, func(t *testing.T) {
			topology, err := buildTopology("input")
			if err != nil {
				t.Fatal(err)
			}
			defer topology.Close()
			r := record(validMR)
			switch kind {
			case "missing delivery":
				r.Headers = nil
			case "duplicate delivery":
				r.Headers = append(r.Headers, r.Headers[0])
			case "wrong key":
				r.Key = []byte("18")
			}
			committed := false
			err = processBatch(topology, "input", []*kgo.Record{r}, &bytes.Buffer{}, func() error {
				committed = true
				return errors.New("commit failed")
			})
			if err == nil || committed != (kind == "commit failure") {
				t.Fatalf("error = %v, commit called = %v", err, committed)
			}
		})
	}
}

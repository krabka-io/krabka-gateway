package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/krabka-io/krabka-streams-go/columnar"
	"github.com/twmb/franz-go/pkg/kgo"
	gitlab "gitlab.com/gitlab-org/api/client-go"
)

type mrSummary struct {
	MRID      int64  `json:"mr_id"`
	ProjectID int64  `json:"project_id"`
	IID       int64  `json:"iid"`
	Title     string `json:"title"`
	State     string `json:"state"`
	Action    string `json:"action"`
}

type mrSerde struct{}

func (mrSerde) Deserialize(_ string, data []byte) (mrSummary, error) {
	var payload gitlab.MergeEvent
	if err := json.Unmarshal(data, &payload); err != nil {
		return mrSummary{}, err
	}
	attributes := payload.ObjectAttributes
	if payload.ObjectKind != "merge_request" || attributes.ID <= 0 || payload.Project.ID <= 0 || attributes.IID <= 0 {
		return mrSummary{}, errors.New("expected a merge request with positive MR, project, and internal IDs")
	}
	return mrSummary{
		MRID: attributes.ID, ProjectID: payload.Project.ID, IID: attributes.IID,
		Title: attributes.Title, State: attributes.State, Action: attributes.Action,
	}, nil
}

func (mrSerde) Serialize(_ string, value mrSummary) ([]byte, error) {
	return json.Marshal(value)
}

func buildTopology(topic string) (*columnar.BuiltTopology, error) {
	mem := memory.NewGoAllocator()
	codec := columnar.NewRowCodec[mrSummary](mrSerde{}, columnar.NewJSONRowBridge[mrSummary](), mem)
	topology := columnar.NewTopology(mem)
	source, err := topology.AddSource("merge-requests", []string{topic}, codec)
	if err != nil {
		return nil, err
	}
	if _, err := topology.AddSink("stdout", "stdout", codec, source); err != nil {
		return nil, err
	}
	return topology.Build()
}

// Stdout is a demo effect. A restart can print a delivery again before its offset commits.
func processBatch(topology *columnar.BuiltTopology, topic string, records []*kgo.Record, output io.Writer, commit func() error) error {
	input := make([]columnar.ConsumedRecord, 0, len(records))
	for _, record := range records {
		headers := make([]columnar.RecordHeader, 0, len(record.Headers))
		for _, header := range record.Headers {
			headers = append(headers, columnar.RecordHeader{Key: header.Key, Value: header.Value})
		}
		input = append(input, columnar.NewConsumedRecord(record.Key, record.Value, record.Timestamp.UnixMilli(), int(record.Partition), record.Offset, headers...))
	}
	produced, err := topology.RunBatch(topic, input)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(output)
	for _, produced := range produced {
		var summary mrSummary
		if err := json.Unmarshal(produced.Record.Value, &summary); err != nil {
			return err
		}
		if string(produced.Record.Key) != strconv.FormatInt(summary.MRID, 10) {
			return errors.New("record key does not match the global MR ID")
		}
		deliveryID := ""
		for _, header := range produced.Record.Headers {
			if header.Key == "webhook-id" {
				if deliveryID != "" || len(header.Value) == 0 {
					return errors.New("expected one non-empty webhook-id header")
				}
				deliveryID = string(header.Value)
			}
		}
		if deliveryID == "" {
			return errors.New("missing webhook-id header")
		}
		if err := encoder.Encode(struct {
			mrSummary
			DeliveryID string `json:"delivery_id"`
		}{summary, deliveryID}); err != nil {
			return err
		}
	}
	return commit()
}

func run(ctx context.Context, brokers, topic, group string, output io.Writer) error {
	topology, err := buildTopology(topic)
	if err != nil {
		return err
	}
	defer topology.Close()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.ConsumeTopics(topic), kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(), kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.BlockRebalanceOnPoll(),
		kgo.FetchMaxPartitionBytes(32<<20),
	)
	if err != nil {
		return err
	}
	defer func() {
		client.AllowRebalance()
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.LeaveGroupContext(closeCtx)
		client.Close()
	}()
	for ctx.Err() == nil {
		fetches := client.PollRecords(ctx, 100)
		if ctx.Err() != nil {
			break
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return errs[0].Err
		}
		records := fetches.Records()
		if len(records) > 0 {
			err = processBatch(topology, topic, records, output, func() error {
				commitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				defer cancel()
				return client.CommitRecords(commitCtx, records...)
			})
		}
		client.AllowRebalance()
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	brokers := flag.String("brokers", "127.0.0.1:9092", "comma-separated broker addresses")
	topic := flag.String("topic", "gitlab.merge-requests", "input topic")
	group := flag.String("group", "gitlab-mr-example", "shared consumer group")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *brokers, *topic, *group, os.Stdout); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

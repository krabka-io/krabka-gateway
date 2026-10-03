package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Stdout is a demo effect. A restart can print a delivery again before its offset commits.
func processBatch(records []*kgo.Record, output io.Writer, commit func() error) error {
	for _, record := range records {
		var formatted bytes.Buffer
		if err := json.Indent(&formatted, record.Value, "", "  "); err != nil {
			return err
		}
		if _, err := formatted.WriteTo(output); err != nil {
			return err
		}
		if _, err := io.WriteString(output, "\n"); err != nil {
			return err
		}
	}
	return commit()
}

func run(ctx context.Context, brokers, topic, group string, output io.Writer) error {
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
			err = processBatch(records, output, func() error {
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
	topic := flag.String("topic", "gitlab.events", "input topic")
	group := flag.String("group", "gitlab-json-example", "shared consumer group")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *brokers, *topic, *group, os.Stdout); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

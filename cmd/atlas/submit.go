// cmd/atlas/submit.go — atlas submit subcommand.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	pb "github.com/yourorg/atlas/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func runSubmit(args []string) {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	server    := fs.String("server", "localhost:50051", "gRPC server address")
	queue     := fs.String("queue", "", "queue name (required)")
	handler   := fs.String("handler", "", "handler name (required)")
	key       := fs.String("key", "", "idempotency key (required)")
	payload   := fs.String("payload", "{}", "JSON payload")
	priority  := fs.Int("priority", 0, "job priority (higher = earlier)")
	maxRetries := fs.Int("max-retries", 3, "maximum retry attempts")
	delayStr  := fs.String("delay", "", "delay before running (e.g. 10s, 1m)")
	_ = fs.Parse(args)

	if *queue == "" || *handler == "" || *key == "" {
		fmt.Fprintln(os.Stderr, "submit: --queue, --handler, and --key are required")
		fs.Usage()
		os.Exit(1)
	}

	req := &pb.SubmitJobRequest{
		Queue:          *queue,
		HandlerName:    *handler,
		Payload:        []byte(*payload),
		IdempotencyKey: *key,
		Priority:       int32(*priority),
		MaxRetries:     int32(*maxRetries),
	}

	if *delayStr != "" {
		d, err := time.ParseDuration(*delayStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "submit: invalid --delay %q: %v\n", *delayStr, err)
			os.Exit(1)
		}
		req.Delay = durationpb.New(d)
	}

	conn, err := newConn(*server)
	if err != nil {
		fmt.Fprintf(os.Stderr, "submit: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	resp, err := conn.Client.SubmitJob(context.Background(), req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "submit: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("job_id:   %s\n", resp.JobId)
	fmt.Printf("state:    %s\n", resp.State)
	fmt.Printf("inserted: %v\n", resp.Inserted)
}

// cmd/atlas/watch.go — atlas watch subcommand. Streams job events via gRPC.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/yourorg/atlas/proto"
)

func runWatch(args []string) {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	server := fs.String("server", "localhost:50051", "gRPC server address")
	_ = fs.Parse(args)

	// Optional job_id argument; empty means watch all jobs.
	jobID := ""
	if fs.NArg() > 0 {
		jobID = fs.Arg(0)
	}

	conn, err := newConn(*server)
	if err != nil {
		fmt.Fprintf(os.Stderr, "watch: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	stream, err := conn.Client.StreamJobUpdates(ctx, &pb.StreamRequest{JobId: jobID})
	if err != nil {
		fmt.Fprintf(os.Stderr, "watch: %v\n", err)
		os.Exit(1)
	}

	if jobID != "" {
		fmt.Printf("watching job %s (ctrl-c to stop)\n", jobID)
	} else {
		fmt.Println("watching all job events (ctrl-c to stop)")
	}

	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			fmt.Fprintf(os.Stderr, "watch: stream error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("job_id=%-36s  state=%-10s  version=%d  cancel_requested=%v\n",
			event.JobId, event.State, event.StateVersion, event.CancelRequested)
	}
}

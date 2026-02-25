// cmd/atlas/cancel.go — atlas cancel subcommand.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	pb "github.com/yourorg/atlas/proto"
)

func runCancel(args []string) {
	fs := flag.NewFlagSet("cancel", flag.ExitOnError)
	server := fs.String("server", "localhost:50051", "gRPC server address")
	_ = fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "Usage: atlas cancel [--server addr] <job_id>")
		os.Exit(1)
	}
	jobID := fs.Arg(0)

	conn, err := newConn(*server)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cancel: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	resp, err := conn.Client.CancelJob(context.Background(), &pb.CancelJobRequest{JobId: jobID})
	if err != nil {
		fmt.Fprintf(os.Stderr, "cancel: %v\n", err)
		os.Exit(1)
	}

	if !resp.Found {
		fmt.Printf("job %s not found (already terminal or does not exist)\n", jobID)
		return
	}
	if resp.Immediate {
		fmt.Printf("job %s canceled immediately (was pending)\n", jobID)
	} else {
		fmt.Printf("job %s cancel requested (running — will stop within ~3s)\n", jobID)
	}
}

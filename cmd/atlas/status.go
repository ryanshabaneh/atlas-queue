// cmd/atlas/status.go — atlas status subcommand.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	pb "github.com/yourorg/atlas/proto"
)

func runStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	server := fs.String("server", "localhost:50051", "gRPC server address")
	_ = fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "Usage: atlas status [--server addr] <job_id>")
		os.Exit(1)
	}
	jobID := fs.Arg(0)

	conn, err := newConn(*server)
	if err != nil {
		fmt.Fprintf(os.Stderr, "status: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	resp, err := conn.Client.GetJobStatus(context.Background(), &pb.GetJobStatusRequest{JobId: jobID})
	if err != nil {
		fmt.Fprintf(os.Stderr, "status: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("job_id:               %s\n", resp.JobId)
	fmt.Printf("state:                %s\n", resp.State)
	fmt.Printf("retry_count:          %d\n", resp.RetryCount)
	fmt.Printf("state_version:        %d\n", resp.StateVersion)
	fmt.Printf("cancel_requested:     %v\n", resp.CancelRequested)
	if resp.CurrentExecutionId != "" {
		fmt.Printf("current_execution_id: %s\n", resp.CurrentExecutionId)
	}
	if resp.LastError != "" {
		fmt.Printf("last_error:           %s\n", resp.LastError)
	}
}

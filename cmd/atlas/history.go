// cmd/atlas/history.go — atlas history subcommand.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	pb "github.com/yourorg/atlas/proto"
)

func runHistory(args []string) {
	fs := flag.NewFlagSet("history", flag.ExitOnError)
	server := fs.String("server", "localhost:50051", "gRPC server address")
	_ = fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "Usage: atlas history [--server addr] <job_id>")
		os.Exit(1)
	}
	jobID := fs.Arg(0)

	conn, err := newConn(*server)
	if err != nil {
		fmt.Fprintf(os.Stderr, "history: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	resp, err := conn.Client.GetJobHistory(context.Background(), &pb.GetJobHistoryRequest{JobId: jobID})
	if err != nil {
		fmt.Fprintf(os.Stderr, "history: %v\n", err)
		os.Exit(1)
	}

	if len(resp.Entries) == 0 {
		fmt.Printf("no execution history for job %s\n", jobID)
		return
	}

	fmt.Printf("execution history for job %s (%d attempt(s)):\n\n", jobID, len(resp.Entries))
	for _, e := range resp.Entries {
		fmt.Printf("  attempt:        %d\n", e.Attempt)
		fmt.Printf("  execution_id:   %s\n", e.ExecutionId)
		fmt.Printf("  worker:         %s\n", e.WorkerHostname)
		fmt.Printf("  trace_id:       %s\n", e.TraceId)
		fmt.Printf("  started_at:     %s\n", e.StartedAt)
		if e.FinishedAt != "" {
			fmt.Printf("  finished_at:    %s\n", e.FinishedAt)
		}
		fmt.Printf("  outcome:        %s\n", e.Outcome)
		if e.ErrorMessage != "" {
			fmt.Printf("  error:          %s\n", e.ErrorMessage)
		}
		fmt.Println()
	}
}

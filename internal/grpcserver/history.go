// internal/grpcserver/history.go
package grpcserver

import (
	"context"
	"time"

	"github.com/google/uuid"
	pb "github.com/yourorg/atlas/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) GetJobHistory(ctx context.Context, req *pb.GetJobHistoryRequest) (*pb.JobHistoryResponse, error) {
	jobID, err := uuid.Parse(req.JobId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid job_id: %v", err)
	}

	rows, err := s.Pool.Query(ctx, `
		SELECT id, worker_hostname, attempt,
		       started_at, finished_at,
		       COALESCE(outcome, ''),
		       COALESCE(error_message, ''),
		       trace_id
		FROM execution_log
		WHERE job_id = $1
		ORDER BY attempt ASC, started_at ASC`, jobID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query history: %v", err)
	}
	defer rows.Close()

	var entries []*pb.ExecutionEntry
	for rows.Next() {
		var (
			execID   uuid.UUID
			hostname string
			attempt  int32
			startedAt time.Time
			finishedAt *time.Time
			outcome    string
			errMsg     string
			traceID    string
		)
		if err := rows.Scan(&execID, &hostname, &attempt, &startedAt, &finishedAt, &outcome, &errMsg, &traceID); err != nil {
			return nil, status.Errorf(codes.Internal, "scan history row: %v", err)
		}
		e := &pb.ExecutionEntry{
			ExecutionId:    execID.String(),
			WorkerHostname: hostname,
			Attempt:        attempt,
			StartedAt:      startedAt.UTC().Format(time.RFC3339),
			Outcome:        outcome,
			ErrorMessage:   errMsg,
			TraceId:        traceID,
		}
		if finishedAt != nil {
			e.FinishedAt = finishedAt.UTC().Format(time.RFC3339)
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate history: %v", err)
	}

	return &pb.JobHistoryResponse{Entries: entries}, nil
}


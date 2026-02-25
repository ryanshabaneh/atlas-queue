// internal/grpcserver/status.go
package grpcserver

import (
	"context"
	"time"

	"github.com/google/uuid"
	pb "github.com/yourorg/atlas/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) GetJobStatus(ctx context.Context, req *pb.GetJobStatusRequest) (*pb.JobStatusResponse, error) {
	jobID, err := uuid.Parse(req.JobId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid job_id: %v", err)
	}

	var (
		jobState       string
		currentExecID  *uuid.UUID
		retryCount     int32
		lastError      *string
		stateVersion   int32
		canceledAt     *time.Time
	)

	err = s.Pool.QueryRow(ctx, `
		SELECT state, current_execution_id, retry_count,
		       last_error, state_version, canceled_at
		FROM jobs WHERE id = $1`, jobID,
	).Scan(&jobState, &currentExecID, &retryCount, &lastError, &stateVersion, &canceledAt)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "job not found")
	}

	resp := &pb.JobStatusResponse{
		JobId:           jobID.String(),
		State:           jobState,
		RetryCount:      retryCount,
		StateVersion:    stateVersion,
		CancelRequested: canceledAt != nil && jobState == "running",
	}
	if currentExecID != nil {
		resp.CurrentExecutionId = currentExecID.String()
	}
	if lastError != nil {
		resp.LastError = *lastError
	}
	return resp, nil
}

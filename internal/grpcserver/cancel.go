// internal/grpcserver/cancel.go
package grpcserver

import (
	"context"

	"github.com/google/uuid"
	"github.com/yourorg/atlas/internal/queue"
	pb "github.com/yourorg/atlas/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (*pb.CancelJobResponse, error) {
	jobID, err := uuid.Parse(req.JobId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid job_id: %v", err)
	}

	res, err := queue.CancelJob(ctx, s.Pool, jobID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cancel: %v", err)
	}

	return &pb.CancelJobResponse{
		Found:     res.Found,
		Immediate: res.Immediate,
	}, nil
}

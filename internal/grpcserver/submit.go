// internal/grpcserver/submit.go
package grpcserver

import (
	"context"
	"time"

	pb "github.com/yourorg/atlas/proto"
	"github.com/yourorg/atlas/internal/queue"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	if req.Queue == "" {
		return nil, status.Error(codes.InvalidArgument, "queue is required")
	}
	if req.HandlerName == "" {
		return nil, status.Error(codes.InvalidArgument, "handler_name is required")
	}
	if req.IdempotencyKey == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}

	opts := queue.EnqueueOptions{
		Queue:          req.Queue,
		HandlerName:    req.HandlerName,
		Payload:        req.Payload,
		IdempotencyKey: req.IdempotencyKey,
		Priority:       int(req.Priority),
		MaxRetries:     int(req.MaxRetries),
	}

	if req.Delay != nil {
		d := req.Delay.AsDuration()
		if d > 0 {
			t := time.Now().Add(d)
			opts.RunAt = &t
		}
	}

	res, err := queue.Enqueue(ctx, s.Pool, opts)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "enqueue: %v", err)
	}

	return &pb.SubmitJobResponse{
		JobId:    res.JobID.String(),
		State:    string(res.State),
		Inserted: res.Inserted,
	}, nil
}

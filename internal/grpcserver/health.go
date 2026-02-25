// internal/grpcserver/health.go
package grpcserver

import (
	"context"

	"github.com/yourorg/atlas/internal/ratelimit"
	pb "github.com/yourorg/atlas/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) GetQueueHealth(ctx context.Context, req *pb.QueueHealthRequest) (*pb.QueueHealthResponse, error) {
	if req.Queue == "" {
		return nil, status.Error(codes.InvalidArgument, "queue is required")
	}

	limit, err := ratelimit.ConcurrencyLimit(ctx, s.Redis, req.Queue)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get concurrency limit: %v", err)
	}

	inflight, err := ratelimit.InflightCount(ctx, s.Redis, req.Queue)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get inflight count: %v", err)
	}

	return &pb.QueueHealthResponse{
		Queue:            req.Queue,
		ConcurrencyLimit: limit,
		Inflight:         inflight,
		P99LatencyMs:     0,
		ErrorRate:        0,
	}, nil
}

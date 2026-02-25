// internal/grpcserver/server.go
package grpcserver

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	pb "github.com/yourorg/atlas/proto"
	"google.golang.org/grpc"
)

// Server implements the AtlasQueue gRPC service.
type Server struct {
	pb.UnimplementedAtlasQueueServer
	Pool  *pgxpool.Pool
	Redis *redis.Client
}

// New creates a Server.
func New(pool *pgxpool.Pool, rc *redis.Client) *Server {
	return &Server{Pool: pool, Redis: rc}
}

// NewGRPCServer registers the Server with a new grpc.Server and returns it.
func NewGRPCServer(srv *Server) *grpc.Server {
	s := grpc.NewServer()
	pb.RegisterAtlasQueueServer(s, srv)
	return s
}

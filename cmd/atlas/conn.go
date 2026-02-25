// cmd/atlas/conn.go — shared gRPC connection helper.
package main

import (
	"fmt"

	pb "github.com/yourorg/atlas/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type grpcConn struct {
	cc     *grpc.ClientConn
	Client pb.AtlasQueueClient
}

func newConn(addr string) (*grpcConn, error) {
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return &grpcConn{cc: cc, Client: pb.NewAtlasQueueClient(cc)}, nil
}

func (c *grpcConn) Close() {
	c.cc.Close()
}

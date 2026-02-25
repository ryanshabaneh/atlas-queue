// cmd/server/main.go — gRPC control plane server, listens on :50051.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
	"github.com/yourorg/atlas/internal/db"
	"github.com/yourorg/atlas/internal/grpcserver"
	"github.com/yourorg/atlas/internal/migrate"
)

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	databaseURL := getenv("DATABASE_URL", "postgres://atlas:atlas@localhost:5432/atlas")
	redisURL    := getenv("REDIS_URL", "redis://localhost:6379")
	grpcPort    := getenv("GRPC_PORT", "50051")

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	// Connect to PostgreSQL.
	logger.Info("connecting to database", "url", databaseURL)
	pool, err := db.Connect(ctx, databaseURL)
	if err != nil {
		logger.Error("connect to database failed", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	logger.Info("database connected")

	if err := migrate.Run(ctx, pool); err != nil {
		logger.Error("run migrations failed", "err", err)
		os.Exit(1)
	}

	// Connect to Redis.
	redisOpts, err := redis.ParseURL(redisURL)
	if err != nil {
		logger.Error("parse redis URL failed", "err", err)
		os.Exit(1)
	}
	rc := redis.NewClient(redisOpts)
	defer rc.Close()

	logger.Info("connecting to redis", "url", redisURL)
	if err := rc.Ping(ctx).Err(); err != nil {
		logger.Error("redis ping failed", "err", err)
		os.Exit(1)
	}
	logger.Info("redis connected")

	// Build and start gRPC server.
	srv := grpcserver.New(pool, rc)
	grpcSrv := grpcserver.NewGRPCServer(srv)

	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		logger.Error("listen failed", "port", grpcPort, "err", err)
		os.Exit(1)
	}

	logger.Info("gRPC server listening", "port", grpcPort)
	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			logger.Error("gRPC serve error", "err", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received, stopping gRPC server")
	grpcSrv.GracefulStop()
	logger.Info("gRPC server stopped")
}

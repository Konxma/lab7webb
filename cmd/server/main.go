package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"time"

	"google.golang.org/grpc"

	"media-service/internal/media"
	pb "media-service/proto"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "gRPC listen address")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		slog.Error("Cannot listen", "error", err)
		os.Exit(1)
	}
	key := os.Getenv("MEDIA_API_KEY")
	if key == "" {
		key = media.DemoKey
	}
	server := grpc.NewServer(
		grpc.UnaryInterceptor(media.UnaryInterceptor(key)),
		grpc.StreamInterceptor(media.StreamInterceptor(key)),
	)
	pb.RegisterMediaServiceServer(server, media.NewService(650*time.Millisecond))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	go func() {
		<-ctx.Done()
		server.Stop()
	}()
	slog.Info("Media Processing gRPC service listening", "address", listener.Addr())
	if err := server.Serve(listener); err != nil {
		slog.Error("gRPC server stopped with error", "error", err)
		os.Exit(1)
	}
}

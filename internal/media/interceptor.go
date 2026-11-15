package media

import (
	"context"
	"crypto/subtle"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const DemoKey = "secret-media-token"

func authenticate(ctx context.Context, key string) error {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("x-api-key")
	if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte(key)) != 1 {
		return status.Error(codes.Unauthenticated, "Invalid or missing API key")
	}
	return nil
}

func logCall(ctx context.Context, kind, method string, started time.Time, auth string, err error) {
	addr := "unknown"
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		addr = p.Addr.String()
	}
	slog.Info("gRPC call", "kind", kind, "method", method, "client", addr,
		"auth", auth, "status", status.Code(err).String(), "duration", time.Since(started).String())
}

func UnaryInterceptor(key string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		started := time.Now()
		if err := authenticate(ctx, key); err != nil {
			logCall(ctx, "unary", info.FullMethod, started, "REJECTED", err)
			return nil, err
		}
		response, err := handler(ctx, req)
		logCall(ctx, "unary", info.FullMethod, started, "SUCCESS", err)
		return response, err
	}
}

func StreamInterceptor(key string) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		started := time.Now()
		ctx := stream.Context()
		if err := authenticate(ctx, key); err != nil {
			logCall(ctx, "stream", info.FullMethod, started, "REJECTED", err)
			return err
		}
		err := handler(srv, stream)
		logCall(ctx, "stream", info.FullMethod, started, "SUCCESS", err)
		return err
	}
}

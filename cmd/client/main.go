package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"media-service/internal/media"
	pb "media-service/proto"
)

func main() {
	if err := run(); err != nil {
		if result, ok := status.FromError(err); ok {
			if result.Code() == codes.Unauthenticated {
				fmt.Fprintf(os.Stderr, "Access denied (%s): %s\n", result.Code(), result.Message())
			} else {
				fmt.Fprintf(os.Stderr, "gRPC %s: %s\n", result.Code(), result.Message())
			}
		} else {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(1)
	}
}

func run() error {
	addr := flag.String("addr", "127.0.0.1:50051", "gRPC server address")
	clientID := flag.String("client-id", "demo-client", "calling service ID")
	filename := flag.String("filename", "input_video.mov", "media filename")
	format := flag.String("format", "MP4", "target format (PNG, JPEG, WEBP, MP4)")
	quality := flag.Int("quality", 85, "quality level (1 to 100)")
	flag.Parse()
	formatNumber, found := pb.MediaFormat_value["MEDIA_FORMAT_"+strings.ToUpper(*format)]
	if !found || formatNumber == 0 || *quality < 1 || *quality > 100 {
		return errors.New("choose PNG, JPEG, WEBP or MP4 and a quality level from 1 to 100")
	}
	key := os.Getenv("MEDIA_API_KEY")
	if key == "" {
		key = media.DemoKey
	}
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewMediaServiceClient(conn)
	root, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(root, 15*time.Second)
	defer cancel()
	request := &pb.ProcessMediaRequest{
		ClientId: *clientID, Filename: *filename,
		TargetFormat: pb.MediaFormat(formatNumber), QualityLevel: int32(*quality),
	}

	fmt.Println("Media Processing Service | gRPC client")
	fmt.Println("\n1. SubmitJob without x-api-key")
	_, err = client.SubmitJob(ctx, request)
	if err == nil {
		return errors.New("server unexpectedly accepted an unauthenticated request")
	}
	if status.Code(err) != codes.Unauthenticated {
		return err
	}
	fmt.Println("   Unauthenticated — access denied as expected")

	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("x-api-key", key))
	fmt.Println("\n2. SubmitJob with x-api-key")
	created, err := client.SubmitJob(ctx, request)
	if err != nil {
		return err
	}
	fmt.Printf("   Job:     %s\n   Status:  %s\n   Created: %s\n",
		created.GetJobId(), created.GetStatus(), timestamp(created.GetCreatedAt()))
	query := &pb.GetJobStatusRequest{JobId: created.GetJobId()}

	fmt.Println("\n3. TrackProgress (server streaming)")
	stream, err := client.TrackProgress(ctx, query)
	if err != nil {
		return err
	}
	for {
		update, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		fmt.Printf("   [%3d%%] %s\n", update.GetProgressPercent(), update.GetCurrentStage())
	}

	fmt.Println("\n4. GetJobStatus")
	final, err := client.GetJobStatus(ctx, query)
	if err != nil {
		return err
	}
	if final.GetStatus() != "COMPLETED" || final.GetProgressPercent() != 100 || final.GetCompletedAt() == 0 {
		return fmt.Errorf("unexpected final job state: %s (%d%%)", final.GetStatus(), final.GetProgressPercent())
	}
	fmt.Printf("   Status:    %s\n   Progress:  %d%%\n   Completed: %s\n",
		final.GetStatus(), final.GetProgressPercent(), timestamp(final.GetCompletedAt()))
	fmt.Println("\nPipeline completed successfully.")
	return nil
}

func timestamp(unix int64) string {
	return time.Unix(unix, 0).Format(time.RFC3339)
}

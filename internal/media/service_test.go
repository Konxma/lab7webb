package media

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "media-service/proto"
)

func fixture(t *testing.T, interval time.Duration) (*Service, pb.MediaServiceClient, context.Context, context.Context) {
	t.Helper()
	const key = "test-key"
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(UnaryInterceptor(key)), grpc.StreamInterceptor(StreamInterceptor(key)))
	service := NewService(interval)
	pb.RegisterMediaServiceServer(server, service)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return service, pb.NewMediaServiceClient(conn), ctx, metadata.AppendToOutgoingContext(ctx, "x-api-key", key)
}

func validRequest() *pb.ProcessMediaRequest {
	return &pb.ProcessMediaRequest{
		ClientId: "tests", Filename: "input.mov", TargetFormat: pb.MediaFormat_MEDIA_FORMAT_MP4, QualityLevel: 85,
	}
}

func requireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("Status = %s, want %s (%v)", status.Code(err), want, err)
	}
}

func submit(t *testing.T, client pb.MediaServiceClient, ctx context.Context) *pb.GetJobStatusRequest {
	t.Helper()
	created, err := client.SubmitJob(ctx, validRequest())
	if err != nil {
		t.Fatal(err)
	}
	id, err := uuid.Parse(created.GetJobId())
	if err != nil || id.Version() != 4 || created.GetStatus() != "QUEUED" || created.GetCreatedAt() == 0 {
		t.Fatalf("Unexpected creation response: %v", created)
	}
	return &pb.GetJobStatusRequest{JobId: created.GetJobId()}
}

func progressStream(t *testing.T, client pb.MediaServiceClient, ctx context.Context, req *pb.GetJobStatusRequest) pb.MediaService_TrackProgressClient {
	t.Helper()
	stream, err := client.TrackProgress(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

func TestAuthentication(t *testing.T) {
	_, client, noKey, authorized := fixture(t, time.Second)
	query := submit(t, client, authorized)
	wrong := metadata.AppendToOutgoingContext(noKey, "x-api-key", "wrong-key")
	duplicate := metadata.AppendToOutgoingContext(authorized, "x-api-key", "test-key")
	for _, ctx := range []context.Context{noKey, wrong, duplicate} {
		_, err := client.SubmitJob(ctx, validRequest())
		requireCode(t, err, codes.Unauthenticated)
		_, err = client.GetJobStatus(ctx, query)
		requireCode(t, err, codes.Unauthenticated)
		stream := progressStream(t, client, ctx, query)
		_, err = stream.Recv()
		requireCode(t, err, codes.Unauthenticated)
	}
}

func TestValidationAndNotFound(t *testing.T) {
	_, client, _, ctx := fixture(t, time.Second)
	for _, mutate := range []func(*pb.ProcessMediaRequest){
		func(req *pb.ProcessMediaRequest) { req.Filename = " " },
		func(req *pb.ProcessMediaRequest) { req.ClientId = "" },
		func(req *pb.ProcessMediaRequest) { req.QualityLevel = 0 },
		func(req *pb.ProcessMediaRequest) { req.QualityLevel = 101 },
		func(req *pb.ProcessMediaRequest) { req.TargetFormat = 0 },
		func(req *pb.ProcessMediaRequest) { req.TargetFormat = 99 },
	} {
		req := validRequest()
		mutate(req)
		_, err := client.SubmitJob(ctx, req)
		requireCode(t, err, codes.InvalidArgument)
	}
	query := &pb.GetJobStatusRequest{JobId: "unknown"}
	_, err := client.GetJobStatus(ctx, query)
	requireCode(t, err, codes.NotFound)
	_, err = progressStream(t, client, ctx, query).Recv()
	requireCode(t, err, codes.NotFound)
}

func receiveStages(t *testing.T, stream pb.MediaService_TrackProgressClient, id string) {
	t.Helper()
	for _, percent := range []int32{25, 50, 75, 100} {
		update, err := stream.Recv()
		if err != nil || update.GetJobId() != id || update.GetProgressPercent() != percent ||
			update.GetCurrentStage() == "" || update.GetIsCompleted() != (percent == 100) {
			t.Fatalf("Expected %d%%, received %v (%v)", percent, update, err)
		}
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Expected end of stream, got %v", err)
	}
}

func TestPipelineWithTwoObservers(t *testing.T) {
	_, client, _, ctx := fixture(t, 40*time.Millisecond)
	query := submit(t, client, ctx)
	initial, err := client.GetJobStatus(ctx, query)
	if err != nil || initial.GetStatus() != "QUEUED" || initial.GetProgressPercent() != 0 || initial.GetCompletedAt() != 0 {
		t.Fatalf("Unexpected queued status: %v (%v)", initial, err)
	}
	first := progressStream(t, client, ctx, query)
	second := progressStream(t, client, ctx, query)
	receiveStages(t, first, query.JobId)
	receiveStages(t, second, query.JobId)
	final, err := client.GetJobStatus(ctx, query)
	if err != nil || final.GetStatus() != "COMPLETED" || final.GetProgressPercent() != 100 || final.GetCompletedAt() == 0 {
		t.Fatalf("Unexpected final status: %v (%v)", final, err)
	}
	receiveStages(t, progressStream(t, client, ctx, query), query.JobId)
}

func TestCancellationStopsLastWorkerAndAllowsResume(t *testing.T) {
	const interval = 200 * time.Millisecond
	service, client, _, ctx := fixture(t, interval)
	query := submit(t, client, ctx)
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream := progressStream(t, client, streamCtx, query)
	update, err := stream.Recv()
	if err != nil || update.GetProgressPercent() != 25 {
		t.Fatalf("Expected first update: %v (%v)", update, err)
	}
	cancel()
	_, err = stream.Recv()
	requireCode(t, err, codes.Canceled)
	deadline := time.Now().Add(time.Second)
	var stage int
	for {
		service.mu.RLock()
		job := service.jobs[query.JobId]
		stopped := job.observers == 0 && job.stopWorker == nil
		stage = job.stage
		service.mu.RUnlock()
		if stopped {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Worker still running after cancellation")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(2 * interval)
	paused, err := client.GetJobStatus(ctx, query)
	if err != nil || paused.GetStatus() != "PROCESSING" || paused.GetProgressPercent() != stages[stage-1].percent {
		t.Fatalf("Expected progress to stay paused: %v (%v)", paused, err)
	}
	receiveStages(t, progressStream(t, client, ctx, query), query.JobId)
}

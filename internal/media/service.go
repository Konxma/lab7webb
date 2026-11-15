package media

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "media-service/proto"
)

var stages = [...]struct {
	name    string
	percent int32
}{
	{"Reading media headers", 25},
	{"Transcoding media stream", 50},
	{"Optimizing output size", 75},
	{"Processing completed", 100},
}

type job struct {
	stage       int
	completedAt int64
	observers   int
	changed     chan struct{}
	stopWorker  chan struct{}
}

type Service struct {
	pb.UnimplementedMediaServiceServer
	mu           sync.RWMutex
	jobs         map[string]*job
	stepInterval time.Duration
}

func NewService(stepInterval time.Duration) *Service {
	return &Service{jobs: make(map[string]*job), stepInterval: stepInterval}
}

func (s *Service) SubmitJob(_ context.Context, req *pb.ProcessMediaRequest) (*pb.JobResponse, error) {
	if strings.TrimSpace(req.GetClientId()) == "" || strings.TrimSpace(req.GetFilename()) == "" {
		return nil, status.Error(codes.InvalidArgument, "client_id and filename are required")
	}
	switch req.GetTargetFormat() {
	case pb.MediaFormat_MEDIA_FORMAT_PNG, pb.MediaFormat_MEDIA_FORMAT_JPEG,
		pb.MediaFormat_MEDIA_FORMAT_WEBP, pb.MediaFormat_MEDIA_FORMAT_MP4:
	default:
		return nil, status.Error(codes.InvalidArgument, "supported target formats are PNG, JPEG, WEBP and MP4")
	}
	if req.GetQualityLevel() < 1 || req.GetQualityLevel() > 100 {
		return nil, status.Error(codes.InvalidArgument, "quality_level must be between 1 and 100")
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, status.Error(codes.Internal, "could not generate job ID")
	}
	key := id.String()
	createdAt := time.Now().Unix()
	s.mu.Lock()
	s.jobs[key] = &job{changed: make(chan struct{})}
	s.mu.Unlock()
	slog.Info("Job registered", "job_id", key, "client_id", req.GetClientId(),
		"filename", req.GetFilename(), "format", req.GetTargetFormat().String(), "status", "QUEUED")
	return &pb.JobResponse{JobId: key, Status: "QUEUED", CreatedAt: createdAt}, nil
}

func (s *Service) GetJobStatus(_ context.Context, req *pb.GetJobStatusRequest) (*pb.JobStatusResponse, error) {
	id := req.GetJobId()
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "job %q was not found", id)
	}
	result := &pb.JobStatusResponse{JobId: id, Status: "QUEUED"}
	if j.stage > 0 {
		result.Status = "PROCESSING"
		result.ProgressPercent = stages[j.stage-1].percent
	}
	if j.stage == len(stages) {
		result.Status = "COMPLETED"
		result.CompletedAt = j.completedAt
	}
	return result, nil
}

func (s *Service) TrackProgress(req *pb.GetJobStatusRequest, stream pb.MediaService_TrackProgressServer) error {
	ctx := stream.Context()
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	id := req.GetJobId()
	s.mu.Lock()
	j, exists := s.jobs[id]
	if !exists {
		s.mu.Unlock()
		return status.Errorf(codes.NotFound, "job %q was not found", id)
	}
	j.observers++
	if j.stage < len(stages) && j.stopWorker == nil {
		j.stopWorker = make(chan struct{})
		go s.runJob(j, j.stopWorker)
	}
	s.mu.Unlock()
	defer s.unsubscribe(j)

	// Replay reached milestones and then wait for new ones. The same worker serves all streams for this job.
	for next := 0; next < len(stages); {
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
		s.mu.RLock()
		stage, changed := j.stage, j.changed
		s.mu.RUnlock()
		if next == stage {
			select {
			case <-ctx.Done():
				return status.FromContextError(ctx.Err()).Err()
			case <-changed:
				continue
			}
		}
		update := &pb.ProgressUpdate{
			JobId: id, CurrentStage: stages[next].name, ProgressPercent: stages[next].percent,
			IsCompleted: next == len(stages)-1,
		}
		if err := stream.Send(update); err != nil {
			return err
		}
		next++
	}
	return nil
}

func (s *Service) unsubscribe(j *job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j.observers--
	if j.observers == 0 && j.stopWorker != nil {
		close(j.stopWorker)
		j.stopWorker = nil
	}
}

func (s *Service) runJob(j *job, stop chan struct{}) {
	ticker := time.NewTicker(s.stepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			s.mu.Lock()
			// The last observer may have left while a tick was ready.
			if j.stopWorker != stop {
				s.mu.Unlock()
				return
			}
			j.stage++
			completed := j.stage == len(stages)
			if completed {
				j.completedAt = time.Now().Unix()
				j.stopWorker = nil
			}
			close(j.changed)
			j.changed = make(chan struct{})
			s.mu.Unlock()
			if completed {
				return
			}
		}
	}
}

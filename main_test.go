package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func newTestScheduler(t *testing.T, workers, queueSize int32) *Scheduler {
	t.Helper()

	s, err := NewScheduler(context.Background(), Config{
		WorkersCount:                workers,
		SubmissionBackpressureCount: queueSize,
	})
	if err != nil {
		t.Fatalf("NewScheduler() error = %v", err)
	}

	t.Cleanup(s.Shutdown)

	return s
}

func TestNewScheduler_InvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want error
	}{
		{
			name: "zero queue capacity",
			cfg: Config{
				WorkersCount:                1,
				SubmissionBackpressureCount: 0,
			},
			want: ErrSubmissionBackpressureCount,
		},
		{
			name: "negative queue capacity",
			cfg: Config{
				WorkersCount:                1,
				SubmissionBackpressureCount: -1,
			},
			want: ErrSubmissionBackpressureCount,
		},
		{
			name: "zero workers",
			cfg: Config{
				WorkersCount:                0,
				SubmissionBackpressureCount: 1,
			},
			want: ErrWorkersCount,
		},
		{
			name: "negative workers",
			cfg: Config{
				WorkersCount:                -1,
				SubmissionBackpressureCount: 1,
			},
			want: ErrWorkersCount,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := NewScheduler(context.Background(), tt.cfg)

			if !errors.Is(err, tt.want) {
				t.Fatalf("NewScheduler() error = %v, want %v", err, tt.want)
			}

			if s == nil {
				t.Fatal("expected a non-nil scheduler on configuration error")
			}
		})
	}
}

func TestScheduler_SubmitAfterShutdown(t *testing.T) {
	s := newTestScheduler(t, 1, 2)

	s.Shutdown()

	err := s.Submit(Task{ID: 100})
	if !errors.Is(err, ErrSchedulerHasStopped) {
		t.Fatalf("Submit() error = %v, want %v", err, ErrSchedulerHasStopped)
	}

	if got := s.totalRejectedCount.Load(); got != 1 {
		t.Fatalf("rejected count = %d, want 1", got)
	}
}

func TestScheduler_GracefulShutdown(t *testing.T) {
	s := newTestScheduler(t, 2, 16)

	const taskCount = 10

	for i := range taskCount {
		if err := s.Submit(Task{ID: 100 + i}); err != nil {
			t.Fatalf("Submit(%d) error = %v", i, err)
		}
	}

	// Shutdown closes the task channel and waits for accepted tasks to finish.
	s.Shutdown()

	if got := s.totalCompletedCount.Load(); got != taskCount {
		t.Fatalf("completed count = %d, want %d", got, taskCount)
	}

	if got := s.totalFailedCount.Load(); got != 0 {
		t.Fatalf("failed count = %d, want 0", got)
	}

	if got := s.currentQueuedCount.Load(); got != 0 {
		t.Fatalf("queued count = %d, want 0", got)
	}

	if got := s.currentRunningCount.Load(); got != 0 {
		t.Fatalf("running count = %d, want 0", got)
	}
}

func TestScheduler_TaskFailureTelemetry(t *testing.T) {
	s := newTestScheduler(t, 2, 8)

	tasks := []Task{
		{ID: 1},
		{ID: 2},
		{ID: 100},
		{ID: 101},
	}

	for _, task := range tasks {
		if err := s.Submit(task); err != nil {
			t.Fatalf("Submit(%d) error = %v", task.ID, err)
		}
	}

	s.Shutdown()

	if got := s.totalCompletedCount.Load(); got != 2 {
		t.Fatalf("completed count = %d, want 2", got)
	}

	if got := s.totalFailedCount.Load(); got != 2 {
		t.Fatalf("failed count = %d, want 2", got)
	}

	if got := s.currentQueuedCount.Load(); got != 0 {
		t.Fatalf("queued count = %d, want 0", got)
	}

	if got := s.currentRunningCount.Load(); got != 0 {
		t.Fatalf("running count = %d, want 0", got)
	}
}

func TestScheduler_RepeatedShutdown(t *testing.T) {
	s := newTestScheduler(t, 2, 8)

	for i := range 5 {
		if err := s.Submit(Task{ID: 100 + i}); err != nil {
			t.Fatalf("Submit() error = %v", err)
		}
	}

	for range 5 {
		s.Shutdown()
	}

	if got := s.currentQueuedCount.Load(); got != 0 {
		t.Fatalf("queued count = %d, want 0", got)
	}

	if got := s.currentRunningCount.Load(); got != 0 {
		t.Fatalf("running count = %d, want 0", got)
	}
}

func TestScheduler_ConcurrentShutdown(t *testing.T) {
	s := newTestScheduler(t, 4, 64)

	for i := range 32 {
		if err := s.Submit(Task{ID: 100 + i}); err != nil {
			t.Fatalf("Submit() error = %v", err)
		}
	}

	const callers = 16

	start := make(chan struct{})
	var wg sync.WaitGroup

	for range callers {
		wg.Go(func() {
			<-start
			s.Shutdown()
		})
	}

	close(start)
	wg.Wait()

	if got := s.currentQueuedCount.Load(); got != 0 {
		t.Fatalf("queued count = %d, want 0", got)
	}

	if got := s.currentRunningCount.Load(); got != 0 {
		t.Fatalf("running count = %d, want 0", got)
	}
}

func TestScheduler_ConcurrentSubmitAndShutdown(t *testing.T) {
	s := newTestScheduler(t, 4, 64)

	const submitters = 16
	const tasksPerSubmitter = 20

	start := make(chan struct{})
	var wg sync.WaitGroup

	var accepted atomic.Int64
	var rejected atomic.Int64
	var unexpected atomic.Int64

	for i := range submitters {
		base := i

		wg.Go(func() {
			<-start

			for j := range tasksPerSubmitter {
				err := s.Submit(Task{
					ID: 1000 + base*tasksPerSubmitter + j,
				})

				switch {
				case err == nil:
					accepted.Add(1)

				case errors.Is(err, ErrSchedulerHasStopped),
					errors.Is(err, ErrMaxSubmissionExceeded),
					errors.Is(err, context.Canceled):
					rejected.Add(1)

				default:
					unexpected.Add(1)
				}
			}
		})
	}

	// Release all submitters together. There is no sleep or polling.
	close(start)

	// Submissions and shutdown can now overlap.
	s.Shutdown()
	wg.Wait()

	if got := unexpected.Load(); got != 0 {
		t.Fatalf("unexpected submission errors: %d", got)
	}

	// Every accepted task must have finished by the time Shutdown returns.
	if got := s.currentQueuedCount.Load(); got != 0 {
		t.Fatalf("queued count = %d, want 0", got)
	}

	if got := s.currentRunningCount.Load(); got != 0 {
		t.Fatalf("running count = %d, want 0", got)
	}

	// All attempts must have either succeeded or returned a recognized error.
	if got, want := accepted.Load()+rejected.Load(),
		int64(submitters*tasksPerSubmitter); got != want {
		t.Fatalf("accounted submissions = %d, want %d", got, want)
	}
}

func TestScheduler_ParentCancellation(t *testing.T) {
	parentCtx, cancelParent := context.WithCancel(context.Background())

	s, err := NewScheduler(parentCtx, Config{
		WorkersCount:                2,
		SubmissionBackpressureCount: 8,
	})
	if err != nil {
		t.Fatalf("NewScheduler() error = %v", err)
	}
	t.Cleanup(s.Shutdown)

	for i := range 4 {
		if err := s.Submit(Task{ID: 100 + i}); err != nil {
			t.Fatalf("Submit() error = %v", err)
		}
	}

	cancelParent()

	// The watcher closes shutdownCh when it enters Shutdown.
	// This is an event handshake, not a polling loop.
	<-s.shutdownCh

	// Wait for Shutdown to finish draining tasks.
	// Concurrent sync.Once callers wait for the active Do call to return.
	s.Shutdown()

	if !s.stopped.Load() {
		t.Fatal("scheduler should be stopped after parent cancellation")
	}

	if err := s.Submit(Task{ID: 200}); !errors.Is(err, ErrSchedulerHasStopped) {
		t.Fatalf("Submit() after parent cancellation error = %v", err)
	}

	if got := s.currentQueuedCount.Load(); got != 0 {
		t.Fatalf("queued count = %d, want 0", got)
	}

	if got := s.currentRunningCount.Load(); got != 0 {
		t.Fatalf("running count = %d, want 0", got)
	}

	if got := s.totalCompletedCount.Load(); got != 4 {
		t.Fatalf("completed count = %d, want 4", got)
	}
}

func TestScheduler_Stats(t *testing.T) {
	s := newTestScheduler(t, 1, 8)

	stats := s.Stats()

	for _, field := range []string{
		"Current Queued:",
		"Current Running",
		"Total Completed",
		"Total Failed",
		"Total Rejected",
	} {
		if !strings.Contains(stats, field) {
			t.Errorf("Stats() = %q, missing field %q", stats, field)
		}
	}
}

func BenchmarkScheduler_Submit(b *testing.B) {
	benchmarks := []struct {
		name       string
		submitters int
		queueSize  int32
		workers    int32
	}{
		{
			name:       "LowContention",
			submitters: 1,
			queueSize:  1024,
			workers:    4,
		},
		{
			name:       "HighContention",
			submitters: 16,
			queueSize:  1024,
			workers:    4,
		},
		{
			name:       "HighContentionManySubmitters",
			submitters: 64,
			queueSize:  1024,
			workers:    4,
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			s, err := NewScheduler(context.Background(), Config{
				WorkersCount:                bm.workers,
				SubmissionBackpressureCount: bm.queueSize,
			})
			if err != nil {
				b.Fatalf("NewScheduler() error = %v", err)
			}

			// Keep setup and shutdown outside the measured benchmark interval.
			b.ResetTimer()

			start := make(chan struct{})
			var wg sync.WaitGroup

			for submitter := range bm.submitters {
				submitter := submitter

				wg.Go(func() {
					<-start

					for i := submitter; i < b.N; i += bm.submitters {
						// IDs other than 1 and 2 simulate successful tasks.
						// Queue-full errors are expected under contention.
						_ = s.Submit(Task{ID: 100 + i})
					}
				})
			}

			close(start)
			wg.Wait()

			b.StopTimer()
			s.Shutdown()
		})
	}
}

func BenchmarkScheduler_SubmitParallel(b *testing.B) {
	for _, submitters := range []int{1, 16, 64} {
		b.Run(fmt.Sprintf("Submitters-%d", submitters), func(b *testing.B) {
			s, err := NewScheduler(context.Background(), Config{
				WorkersCount:                4,
				SubmissionBackpressureCount: 1024,
			})
			if err != nil {
				b.Fatalf("NewScheduler() error = %v", err)
			}

			var nextID atomic.Int64

			b.ResetTimer()

			b.SetParallelism(submitters)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					id := nextID.Add(1)
					_ = s.Submit(Task{ID: int(id + 100)})
				}
			})

			b.StopTimer()
			s.Shutdown()
		})
	}
}

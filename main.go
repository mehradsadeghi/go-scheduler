package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

/*
Acceptance criteria:
- Provide a small demo that generates a mixture of: A burst of submissions that creates queue pressure.
	- Include instructions for collecting a CPU profile, a heap profile, and a Go execution trace.
		Use the results to investigate at least one actual bottleneck or scheduling behavior.
	- Benchmarks cover at least low-contention and high-contention submission workloads.
	- A CPU profile, heap profile, and execution trace can be collected from the demo.
*/

var (
	ErrSchedulerHasStopped   = errors.New("scheduler has stopped")
	ErrMaxSubmissionExceeded = errors.New("max submission exceeded")
)

type Scheduler struct {
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	mu          sync.Mutex
	tasks       chan Task
	stopped     atomic.Bool
	stoppedOnce sync.Once

	maxSubmissionCount int32
	workersCount       int32

	currentQueuedCount             atomic.Int32
	currentRunningCount            atomic.Int32
	totalCompletedCount            atomic.Int32
	totalFailedCount               atomic.Int32
	totalCanceledCount             atomic.Int32
	totalRejectedCount             atomic.Int32
	totalExecutionTimeMilliseconds atomic.Int32
}

type Task struct{ ID int }

type Config struct {
	SubmissionBackpressureCount int32
	WorkersCount                int32
}

func NewScheduler(parentCtx context.Context, cfg Config) *Scheduler {
	ctx, cancel := context.WithCancel(parentCtx)

	s := &Scheduler{
		ctx:                ctx,
		cancel:             cancel,
		wg:                 sync.WaitGroup{},
		mu:                 sync.Mutex{},
		tasks:              make(chan Task, cfg.SubmissionBackpressureCount),
		workersCount:       cfg.WorkersCount,
		maxSubmissionCount: cfg.SubmissionBackpressureCount,
	}

	s.startProcessing(ctx)

	go func() {
		<-parentCtx.Done()
		s.Shutdown()
	}()

	return s
}

func (s *Scheduler) startProcessing(ctx context.Context) {
	for range s.workersCount {
		s.wg.Go(func() {
			for {
				select {
				case v, ok := <-s.tasks:
					s.currentQueuedCount.Add(-1)
					if !ok {
						return
					}
					if ctx.Err() != nil {
						s.totalCanceledCount.Add(1)
						return
					}
					s.wg.Add(1)
					if err := s.process(ctx, v); err != nil {
						s.totalFailedCount.Add(1)
					} else {
						s.totalCompletedCount.Add(1)
					}
					s.currentRunningCount.Add(-1)
					s.wg.Done()
				case <-ctx.Done():
					s.totalCanceledCount.Add(1)
					return
				}
			}
		})

	}
}

func (s *Scheduler) process(ctx context.Context, v Task) error {
	s.currentRunningCount.Add(1)

	totalExecutionTime := time.Millisecond * time.Duration(rand.IntN(200))
	select {
	// simulating short/long running task
	case <-time.After(totalExecutionTime):
		// simulating failed task
		if v.ID == 1 || v.ID == 2 {
			return fmt.Errorf("bad id: %d", v.ID)
		}
		s.totalExecutionTimeMilliseconds.Add(int32(totalExecutionTime))
	case <-ctx.Done():
		s.totalCanceledCount.Add(1)
		return ctx.Err()
	}

	return nil
}

func (s *Scheduler) Submit(task Task) error {
	s.mu.Lock()
	if s.stopped.Load() {
		s.mu.Unlock()
		s.totalRejectedCount.Add(1)
		return ErrSchedulerHasStopped
	}

	// admission check
	if len(s.tasks) >= int(s.maxSubmissionCount) {
		s.mu.Unlock()
		s.totalRejectedCount.Add(1)
		return ErrMaxSubmissionExceeded
	}

	s.wg.Add(1)
	s.mu.Unlock()
	defer s.wg.Done()

	select {
	case s.tasks <- task:
		s.currentQueuedCount.Add(1)
	case <-s.ctx.Done():
		s.totalCanceledCount.Add(1)
		return s.ctx.Err()
	}

	return nil
}

func (s *Scheduler) Stats() string {
	return fmt.Sprintf(
		"Current Queued: %d\nCurrent Running %d\nTotal Completed %d\nTotal Failed %d\nTotal Canceled %d\nTotal Rejected %d",
		s.currentQueuedCount.Load(),
		s.currentRunningCount.Load(),
		s.totalCompletedCount.Load(),
		s.totalFailedCount.Load(),
		s.totalCanceledCount.Load(),
		s.totalRejectedCount.Load(),
	)
}

func (s *Scheduler) Shutdown() {
	s.stoppedOnce.Do(func() {
		s.mu.Lock()
		s.stopped.Store(true)
		s.cancel()
		s.mu.Unlock()
		s.wg.Wait()
	})
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s := NewScheduler(ctx, Config{SubmissionBackpressureCount: 1000, WorkersCount: 4})

	var wg sync.WaitGroup

	for i := range 2000 {
		wg.Go(func() { _ = s.Submit(Task{ID: i}) })
	}

	fmt.Println(s.Stats())
	wg.Wait()
	time.Sleep(1 * time.Second)
	fmt.Println(s.Stats())
}

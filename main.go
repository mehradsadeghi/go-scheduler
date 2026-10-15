package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrSchedulerHasStopped         = errors.New("scheduler has stopped")
	ErrMaxSubmissionExceeded       = errors.New("max submission exceeded")
	ErrSubmissionBackpressureCount = errors.New("submission backpressure count must be greater than zero")
	ErrWorkersCount                = errors.New("workers count must be greater than zero")
)

type Scheduler struct {
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	mu          sync.RWMutex
	tasks       chan Task
	stopped     atomic.Bool
	stoppedOnce sync.Once
	shutdownCh  chan struct{}

	maxSubmissionCount int32
	workersCount       int32

	currentQueuedCount             atomic.Int32
	currentRunningCount            atomic.Int32
	totalCompletedCount            atomic.Int32
	totalFailedCount               atomic.Int32
	totalRejectedCount             atomic.Int32
	totalExecutionTimeMilliseconds atomic.Int32
}

type Task struct{ ID int }

type Config struct {
	SubmissionBackpressureCount int32
	WorkersCount                int32
}

func NewScheduler(parentCtx context.Context, cfg Config) (*Scheduler, error) {
	if cfg.SubmissionBackpressureCount <= 0 {
		return &Scheduler{}, ErrSubmissionBackpressureCount
	}
	if cfg.WorkersCount <= 0 {
		return &Scheduler{}, ErrWorkersCount
	}

	ctx, cancel := context.WithCancel(parentCtx)

	s := &Scheduler{
		ctx:                ctx,
		cancel:             cancel,
		wg:                 sync.WaitGroup{},
		mu:                 sync.RWMutex{},
		shutdownCh:         make(chan struct{}),
		tasks:              make(chan Task, cfg.SubmissionBackpressureCount),
		workersCount:       cfg.WorkersCount,
		maxSubmissionCount: cfg.SubmissionBackpressureCount,
	}

	s.startProcessing()

	go func() {
		select {
		case <-parentCtx.Done():
			s.Shutdown()
		case <-s.shutdownCh:
			return
		}
	}()

	return s, nil
}

func (s *Scheduler) startProcessing() {
	for range s.workersCount {
		s.wg.Go(func() {

			for v := range s.tasks {
				s.currentQueuedCount.Add(-1)
				s.wg.Add(1)
				if err := s.process(v); err != nil {
					s.totalFailedCount.Add(1)
				} else {
					s.totalCompletedCount.Add(1)
				}
				s.currentRunningCount.Add(-1)
				s.wg.Done()
			}
		})
	}
}

func (s *Scheduler) process(v Task) error {
	s.currentRunningCount.Add(1)

	totalExecutionTime := time.Millisecond * time.Duration(rand.IntN(200))

	// simulating short/long running task
	<-time.After(totalExecutionTime)

	// simulating failed task
	if v.ID == 1 || v.ID == 2 {
		return fmt.Errorf("bad id: %d", v.ID)
	}
	s.totalExecutionTimeMilliseconds.Add(int32(totalExecutionTime))

	return nil
}

func (s *Scheduler) Submit(task Task) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.stopped.Load() {
		s.totalRejectedCount.Add(1)
		return ErrSchedulerHasStopped
	}

	s.wg.Add(1)
	defer s.wg.Done()

	select {
	case s.tasks <- task:
		s.currentQueuedCount.Add(1)
	case <-s.ctx.Done():
		return s.ctx.Err()
	default:
		s.totalRejectedCount.Add(1)
		return ErrMaxSubmissionExceeded
	}

	return nil
}

func (s *Scheduler) Stats() string {
	return fmt.Sprintf(
		"Current Queued: %d\nCurrent Running %d\nTotal Completed %d\nTotal Failed %d\nTotal Rejected %d",
		s.currentQueuedCount.Load(),
		s.currentRunningCount.Load(),
		s.totalCompletedCount.Load(),
		s.totalFailedCount.Load(),
		s.totalRejectedCount.Load(),
	)
}

func (s *Scheduler) Shutdown() {
	s.stoppedOnce.Do(func() {
		close(s.shutdownCh)
		s.mu.Lock()
		s.stopped.Store(true)
		close(s.tasks)
		s.mu.Unlock()
		s.cancel()
		s.wg.Wait()
	})
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, err := NewScheduler(ctx, Config{SubmissionBackpressureCount: 1000, WorkersCount: 4})
	if err != nil {
		log.Fatal(err)
		return
	}

	var wg sync.WaitGroup

	for i := range 2000 {
		go s.Shutdown()
		wg.Go(func() { _ = s.Submit(Task{ID: i}) })
	}

	fmt.Println(s.Stats())
	wg.Wait()
	time.Sleep(1 * time.Second)
	fmt.Println(s.Stats())
}

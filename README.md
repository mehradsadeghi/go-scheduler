# Concurrent Scheduler

A small Go concurrency project implementing a bounded task scheduler with
multiple workers, backpressure, cancellation, lifecycle management, and
telemetry.

## Features

- Fixed number of worker goroutines.
- Bounded task queue.
- Non-blocking admission with explicit overload rejection.
- Context-aware task execution.
- Graceful shutdown design.
- Concurrent-safe submission and shutdown coordination.
- Atomic telemetry counters.
- Simulated task execution, failures, and variable latency.

## Configuration

```go
type Config struct {
    SubmissionBackpressureCount int32
    WorkersCount                int32
}
```

Example:

```go
scheduler := NewScheduler(context.Background(), Config{
    SubmissionBackpressureCount: 100,
    WorkersCount:                8,
})
defer scheduler.Shutdown()
```

`SubmissionBackpressureCount` determines the task channel's buffer capacity.

`WorkersCount` determines the number of worker goroutines.

Both values should be positive. Configuration validation is not currently
implemented.

## API

### NewScheduler

```go
func NewScheduler(parentCtx context.Context, cfg Config) *Scheduler
```

Creates a scheduler, starts its workers, and associates its lifecycle with
the parent context.

Canceling the parent context initiates scheduler shutdown.

### Submit

```go
func (s *Scheduler) Submit(task Task) error
```

Attempts to submit a task.

Possible errors:

| Error | Meaning |
|---|---|
| `ErrSchedulerHasStopped` | The scheduler has stopped accepting submissions. |
| `ErrMaxSubmissionExceeded` | The task queue is full. |
| `context.Canceled` | The scheduler's context was canceled before the submission completed. |

The submission operation does not create a goroutine to wait for queue
capacity. If the queue is full at the admission check, the submission is
rejected.

A successful return means the task was sent to the task channel. It does not
guarantee that execution has started or completed.

### Stats

```go
func (s *Scheduler) Stats() string
```

Returns a snapshot containing:

- Current queued tasks.
- Current running tasks.
- Total completed tasks.
- Total failed tasks.
- Total rejected submissions.

The implementation also maintains cumulative execution time internally.

### Shutdown

```go
func (s *Scheduler) Shutdown()
```

Stops the scheduler and waits for the scheduler's WaitGroup to finish.

Shutdown is safe to invoke repeatedly.

## Task Model

```go
type Task struct {
    ID int
}
```

The current implementation simulates task execution using a random duration
between zero and approximately 200 milliseconds.

Tasks with IDs `1` and `2` simulate operational failures.

Other task IDs simulate successful execution unless cancellation interrupts
the operation.

## Resource Bounds

The scheduler uses a fixed number of workers and a buffered task channel.

Let:

- `W` be the configured worker count.
- `Q` be the configured queue capacity.

The number of worker goroutines is bounded by `W`.

The task channel holds at most `Q` queued tasks. Up to `W` additional tasks
may be executing concurrently.

Therefore, the intended maximum number of queued and executing tasks is:

`Q + W`

Submissions do not spawn additional goroutines to wait for capacity.

This bound assumes valid, positive configuration values and excludes
goroutines owned by the caller.

## Task Lifecycle and Error Semantics

A task follows this conceptual lifecycle:

```text
Submitted
    |
    v
  Queued
    |
    v
  Running
   /   \
  v     v
Completed Failed
```

A task may also be abandoned before execution if shutdown cancels the
scheduler.

The current telemetry distinguishes these outcomes as follows:

| Outcome | Counter |
|---|---|
| Task returns nil | `Total Completed` |
| Task returns an error | `Total Failed` |
| Submission rejected | `Total Rejected` |

Cancellation errors returned by a running task are counted as failures.

A submission that returns an error because the scheduler context is canceled
is not counted as a rejected submission by the current implementation.

## Shutdown Policy

The intended graceful-shutdown policy is:

1. Stop accepting new tasks.
2. Allow already accepted tasks to finish.
3. Ensure queued tasks are processed according to the documented policy.
4. Wait for all workers to terminate.
5. Return only after scheduler termination is complete.

## Telemetry and Consistency

The scheduler uses atomic counters for its telemetry.

Individual reads are atomic, so concurrent updates do not cause data races
on these counters.

However, `Stats()` reads each counter independently. Consequently, the
returned values are not guaranteed to represent one globally consistent
instant in time.

For example, a task may move from queued to running between two counter
reads. The resulting snapshot may temporarily show values that do not satisfy
the expected relationship between queued and running tasks.

The intended invariant is:

- Queue and running gauges should reflect the current task lifecycle.
- Every completed or failed task should contribute to exactly one terminal
  outcome.
- Rejected submissions should be counted independently from task outcomes.
- Counter updates should occur exactly once per corresponding transition.

The current implementation does not yet guarantee all these invariants under
concurrent submission and shutdown.

## Testing

Run the complete test suite:

```bash
go test ./...
```

Run the race detector:

```bash
go test -race ./...
```

Repeat tests to expose intermittent concurrency defects:

```bash
go test -race -count=100 ./...
```

Run benchmarks:

```bash
go test -bench=. -benchmem ./...
```

## Profiling and Execution Tracing

Use the demo workload to investigate scheduler behavior under load.

### CPU profile

```bash
go test -bench=. -cpuprofile=cpu.prof ./...
go tool pprof cpu.prof
```

### Heap profile

```bash
go test -bench=. -memprofile=heap.prof ./...
go tool pprof heap.prof
```

### Execution trace

Run a workload that exercises concurrent submission, worker execution,
and shutdown:

```bash
go test -trace=trace.out ./...
go tool trace trace.out
```

For meaningful results, use a sufficiently long workload and include both
low-contention and high-contention scenarios.

Investigate:

- Worker utilization.
- Time spent waiting for runnable goroutines.
- Queue pressure and rejected submissions.
- Allocation behavior under bursts.
- Goroutine termination during shutdown.

## Project Goals

This project explores practical Go concurrency engineering:

- Bounded concurrency and backpressure.
- Cancellation propagation.
- Worker lifecycle management.
- Concurrent admission and shutdown.
- Atomic telemetry.
- Concurrency testing with the race detector.
- Performance analysis using benchmarks, profiling, and execution tracing.

The central objective is to make resource usage bounded, lifecycle semantics
explicit, and concurrent behavior measurable.
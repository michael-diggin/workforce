package workforce

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

var heartbeatDuration = time.Duration(1) * time.Second

// TaskResult is a struct holding the result of a worker completing a task
type TaskResult struct {
	Err error
	ID  int
}

// HeartBeat is a struct holding the time and worker id
type HeartBeat struct {
	timestamp *time.Time
	id        int
}

// WorkerPool is the object that manages the workers
type WorkerPool struct {
	taskChan    chan time.Time
	resultChan  chan TaskResult
	healthChan  chan HeartBeat
	mu          sync.RWMutex
	healthMap   map[int]*time.Time
	maxWorkers  int
	cancelFuncs map[int]context.CancelFunc
	function    func() error
}

// New returns a new instance of WorkerPool
func New(maxWorkers int, fn func() error) *WorkerPool {
	tasks := make(chan time.Time, maxWorkers)
	results := make(chan TaskResult, maxWorkers)
	health := make(chan HeartBeat, maxWorkers)
	healthMap := make(map[int]*time.Time)
	startTime := time.Now().UTC()
	for id := 0; id < maxWorkers; id++ {
		healthMap[id] = &startTime
	}
	return &WorkerPool{
		taskChan:    tasks,
		resultChan:  results,
		healthChan:  health,
		healthMap:   healthMap,
		mu:          sync.RWMutex{},
		maxWorkers:  maxWorkers,
		cancelFuncs: make(map[int]context.CancelFunc),
		function:    fn,
	}
}

// Launch spins up maxWorkers number of goroutines to handle tasks
// and returns the task chan to the caller
func (wp *WorkerPool) Launch(ctx context.Context) chan time.Time {
	for i := 0; i < wp.maxWorkers; i++ {
		wctx, cancel := context.WithCancel(ctx)
		wp.cancelFuncs[i] = cancel
		wp.launchWorker(wctx, i)
	}
	go wp.MonitorWorkers(ctx)
	go wp.RestartWorkers(ctx)
	return wp.taskChan
}

func (wp *WorkerPool) launchWorker(ctx context.Context, workerID int) {
	log.Debugf("Launching Worker %d at %v", workerID, time.Now().UTC())
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(heartbeatDuration):
				now := time.Now().UTC()
				wp.healthChan <- HeartBeat{timestamp: &now, id: workerID}
			case taskTime := <-wp.taskChan:
				log.Debugf("Worker %d starting task at %v", workerID, taskTime)
				err := wp.function()
				wp.resultChan <- TaskResult{Err: err, ID: workerID}
				now := time.Now().UTC()
				wp.healthChan <- HeartBeat{timestamp: &now, id: workerID}

			}
		}
	}()
}

// WaitForNTasks will wait until N tasks have been completed and sent to the results channel
func (wp *WorkerPool) WaitForNTasks(n int) []TaskResult {
	results := make([]TaskResult, 0, n)
	for i := 0; i < n; i++ {
		workerResult := <-wp.resultChan
		results = append(results, workerResult)
	}
	return results
}

// MonitorWorkers will monitor the health channel and update the health map to the latest
// heartbeat time reported for workers
func (wp *WorkerPool) MonitorWorkers(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case heartbeat := <-wp.healthChan:
			wp.mu.Lock()
			wp.healthMap[heartbeat.id] = heartbeat.timestamp
			wp.mu.Unlock()
		}
	}
}

// RestartWorkers periodically finds workers that are out of date, and restarts them
func (wp *WorkerPool) RestartWorkers(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * heartbeatDuration): // TODO: These should be configurable by some Opts
			wp.mu.Lock()
			now := time.Now().UTC()
			for id := 0; id < wp.maxWorkers; id++ {
				hbTime := wp.healthMap[id] // need to initialise healthmap then
				if now.Sub(*hbTime) > heartbeatDuration {
					go wp.restartWorker(ctx, id)
				}
			}
		}
	}
}

// restartWorker kills a worker by ID and launches a new worker with the same ID
func (wp *WorkerPool) restartWorker(ctx context.Context, id int) {
	log.Infof("Restarting worker %d", id)
	cancel := wp.cancelFuncs[id]
	cancel()

	wctx, newCancel := context.WithCancel(ctx)
	wp.cancelFuncs[id] = newCancel
	wp.launchWorker(wctx, id)
}

// CombineWorkerErrors is a utility function to add multiple errors from workers together
func CombineWorkerErrors(results []TaskResult) error {
	errString := ""
	var err error
	for _, res := range results {
		if res.Err != nil {
			errString += fmt.Sprintf("Worker %d encountered error %v", res.ID, res.Err)
		}
	}
	if errString != "" {
		err = errors.New(errString)
	}
	return err
}

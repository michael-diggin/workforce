package workforce

import (
	"context"
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

// TaskResult is a struct holding the result of a worker completing a task
type TaskResult struct {
	Err error
	ID  int
}

// WorkerPool is the object that manages the workers
type WorkerPool struct {
	taskChan   chan time.Time
	resultChan chan TaskResult
	maxWorkers int
}

// New returns a new instance of WorkerPool
func New(maxWorkers int) *WorkerPool {
	tasks := make(chan time.Time, maxWorkers)
	results := make(chan TaskResult, maxWorkers)
	return &WorkerPool{
		taskChan:   tasks,
		resultChan: results,
		maxWorkers: maxWorkers,
	}
}

// Launch spins up maxWorkers number of goroutines to handle tasks
// and returns the task chan so the caller and
func (wp *WorkerPool) Launch(ctx context.Context, fn func() error) chan time.Time {
	for i := 0; i < wp.maxWorkers; i++ {
		wp.launchWorker(ctx, i, fn)
	}
	return wp.taskChan
}

func (wp *WorkerPool) launchWorker(ctx context.Context, workerID int, fn func() error) {
	log.Debugf("Launching Worker %d at %v", workerID, time.Now().UTC())
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case taskTime := <-wp.taskChan:
				log.Debugf("Worker %d starting task at %v", workerID, taskTime)
				err := fn()
				wp.resultChan <- TaskResult{Err: err, ID: workerID}

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

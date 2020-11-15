package workforce

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCombineWorkerErrors(t *testing.T) {
	t.Run("With no errors", func(t *testing.T) {
		results := []TaskResult{
			TaskResult{nil, 0},
			TaskResult{nil, 1},
		}
		err := CombineWorkerErrors(results)
		require.NoError(t, err, "no errors in either TaskResult")
	})

	t.Run("with multiple errors", func(t *testing.T) {
		results := []TaskResult{
			TaskResult{errors.New("First Error"), 0},
			TaskResult{errors.New("Second Error"), 1},
		}
		err := CombineWorkerErrors(results)
		require.Error(t, err, "should return combined error")
		for _, substr := range []string{"Worker 0", "Worker 1", "First Error", "Second Error"} {
			require.Contains(t, err.Error(), substr)
		}

	})
}

func TestWorkerPool(t *testing.T) {
	numWorkers := 5
	intChan := make(chan int, numWorkers)

	fn := func() error {
		intChan <- 1
		return nil
	}

	wp := New(numWorkers)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() //shuts down worker goroutines when test finishes
	taskChan := wp.Launch(ctx, fn)

	for i := 0; i < numWorkers; i++ {
		taskChan <- time.Now().UTC()
	}
	workerResults := wp.WaitForNTasks(numWorkers)
	require.Len(t, workerResults, numWorkers)

	err := CombineWorkerErrors(workerResults)
	require.NoError(t, err)

	// read from the intChan to assert all workers sent value 1
	sum := 0
	for i := 0; i < numWorkers; i++ {
		sum += <-intChan
	}
	require.Equal(t, numWorkers, sum, "incorrect values sent to channel")
	require.Len(t, intChan, 0, "intChan still has values")
}

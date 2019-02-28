package models

import (
	"encoding/json"
	"math"
	"math/rand"
	"runtime/debug"
	"time"

	"github.com/netlify/gocommerce/conf"

	"github.com/jinzhu/gorm"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Task is the model for tasks that are coordinated through the database
type Task struct {
	UID   string `json:"name" gorm:"primary_key"`
	State string `json:"state" sql:"text"`

	LastExecutionAt time.Time `json:"last_execution_at"`
	CreatedAt       time.Time `json:"created_at"`
}

// TaskExecutor is the interface for implementations of task-specific logic
type TaskExecutor interface {
	Enabled() bool
	Execute(db *gorm.DB, log *logrus.Entry, inputJSON []byte) (time.Duration, interface{}, error)
}

// TaskRunner allows running distributed background tasks
// task execution is coordinated via the database
type TaskRunner string

const (
	DownloadRefreshTask TaskRunner = "refreshDownloads"
)

func backoffLinear(interval time.Duration) time.Duration {
	if interval < time.Minute {
		return time.Minute
	}
	return interval * 2
}

var failureCounts map[TaskRunner]uint64

// failures in succession to stop the task execution entirely
const circuitBreakThreshold = 10

// RunBackground implements the distributed scheduling logic for TaskRunner
func (t TaskRunner) RunBackground(db *gorm.DB, log *logrus.Entry, config *conf.Configuration) error {
	var executor TaskExecutor
	switch t {
	case DownloadRefreshTask:
		executor = &downloadRefreshExecutor{
			Config: config,
		}
	default:
		return errors.New("Invalid task")
	}

	if !executor.Enabled() {
		log.Debugf("Task %s is not enabled. Skipping.", t)
		return nil
	}

	failureCounts[t] = 0

	log = log.WithField("task", string(t))
	go func() {
		interval := time.Minute
		stop := make(chan struct{})
		for {
			func() {
				defer func() {
					if r := recover(); r != nil {
						interval = backoffLinear(interval)
						log.Panic(r, debug.Stack())
						failureCounts[t]++
					}
				}()

				time.Sleep(interval)

				task := Task{
					UID: string(t),
				}
				if err := db.FirstOrInit(&task, task).Error; err != nil {
					interval = backoffLinear(interval)
					log.WithError(err).
						Warningf("Finding last execution failed. Retrying in %.0f seconds", interval.Seconds())
					return
				}

				if !task.LastExecutionAt.IsZero() && task.LastExecutionAt.Add(interval).After(time.Now()) {
					return
				}

				task.LastExecutionAt = time.Now()
				if err := db.Save(&task).Error; err != nil {
					interval = backoffLinear(interval)
					log.WithError(err).
						Warningf("Saving execution time failed. Retrying in %.0f seconds", interval.Seconds())
					return
				}

				log.Debug("Starting task execution")
				taskInterval, state, taskErr := executor.Execute(db, log, []byte(task.State))
				if taskErr != nil {
					log.WithError(taskErr).
						Warningf("Task execution failed")
					interval = backoffLinear(interval)
					failureCounts[t]++
				} else {
					failureCounts[t] = 0
				}

				var stateErr error
				var stateJSON []byte
				if stateJSON, stateErr = json.Marshal(state); stateErr == nil {
					task.State = string(stateJSON)
					stateErr = db.Save(&task).Error
				}
				if stateErr != nil {
					log.WithError(stateErr).Warning("Failed to save task state")
				}

				if failureCounts[t] > circuitBreakThreshold {
					log.WithField("failures", failureCounts[t]).
						Errorf("Task terminated by circuit breaker")
					close(stop)
				}

				if taskErr == nil && stateErr == nil {
					// add random backoff to minimize collisions with other instances
					backoffTime := time.Duration(math.Floor(float64(taskInterval) * rand.Float64()))
					interval = taskInterval + backoffTime
					log.WithField("waiting", interval).Debug("Task execution finished")
				}
			}()

			select {
			case <-stop:
				return
			default:
			}
		}
	}()

	return nil
}

// RunTasks starts the scheduling of all tasks
func RunTasks(db *gorm.DB, log *logrus.Entry, config *conf.Configuration) {
	DownloadRefreshTask.RunBackground(db, log, config)
}

package workflow

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jing2uo/tdx2db/database"
)

// TaskState represents the state of a task execution
type TaskState string

const (
	StatePending   TaskState = "pending"
	StateRunning   TaskState = "running"
	StateCompleted TaskState = "completed"
	StateSkipped   TaskState = "skipped"
	StateFailed    TaskState = "failed"
)

// TaskResult holds the execution result of a task
type TaskResult struct {
	State   TaskState
	Rows    int
	Message string
	Error   error
}

type ErrorMode int

const (
	ErrorModeStop ErrorMode = iota
	ErrorModeSkip
)

// TaskFunc is the function that executes a task
type TaskFunc func(ctx context.Context, db database.DataRepository, args *TaskArgs) (*TaskResult, error)

// SkipCondition determines if a task should be skipped
type SkipCondition func(ctx context.Context, db database.DataRepository, args *TaskArgs) bool

// Task represents a unit of work with dependencies
type Task struct {
	Name      string
	DependsOn []string
	Executor  TaskFunc
	SkipIf    SkipCondition
	OnError   ErrorMode
}

type TaskArgs struct {
	Minline       string
	TdxHome       string
	TempDir       string
	VipdocDir     string
	DayFileDir    string
	ValidPrefixes []string
	Today         time.Time
	Extra         map[string]interface{}
}

// TaskExecutor manages and executes tasks with dependency resolution
type TaskExecutor struct {
	db    database.DataRepository
	tasks map[string]*Task
}

// NewTaskExecutor creates a new task executor
func NewTaskExecutor(db database.DataRepository, tasks map[string]*Task) *TaskExecutor {
	return &TaskExecutor{
		db:    db,
		tasks: tasks,
	}
}

func (te *TaskExecutor) Run(ctx context.Context, taskNames []string, args *TaskArgs) error {
	if len(taskNames) == 0 {
		return nil
	}

	order, err := te.topologicalSort(taskNames)
	if err != nil {
		return fmt.Errorf("failed to resolve task dependencies: %w", err)
	}

	results := make(map[string]*TaskResult)
	pending := make(map[string]bool)
	for _, name := range order {
		pending[name] = true
	}

	for len(pending) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		ready := te.findReadyTasks(pending, results)
		if len(ready) == 0 {
			return fmt.Errorf("circular dependency detected or no ready tasks")
		}

		var wg sync.WaitGroup
		for _, name := range ready {
			task, exists := te.tasks[name]
			if !exists {
				delete(pending, name)
				continue
			}

			if task.SkipIf != nil && task.SkipIf(ctx, te.db, args) {
				results[name] = &TaskResult{State: StateSkipped, Message: "skipped by condition"}
				delete(pending, name)
				continue
			}

			wg.Add(1)
			go func(n string, t *Task) {
				defer wg.Done()
				results[n] = te.executeTask(ctx, t, args)
			}(name, task)
		}

		wg.Wait()

		for _, name := range ready {
			result := results[name]
			if result.Error != nil {
				task := te.tasks[name]
				if task.OnError == ErrorModeStop {
					return fmt.Errorf("task %s failed: %w", name, result.Error)
				}
			}
			delete(pending, name)
		}
	}

	return nil
}

func (te *TaskExecutor) executeTask(ctx context.Context, task *Task, args *TaskArgs) *TaskResult {
	result, err := task.Executor(ctx, te.db, args)
	if err != nil {
		return &TaskResult{
			State: StateFailed,
			Error: err,
		}
	}
	return result
}

func (te *TaskExecutor) topologicalSort(taskNames []string) ([]string, error) {
	inDegree := make(map[string]int)
	adj := make(map[string][]string)
	taskSet := make(map[string]bool)

	for _, name := range taskNames {
		if _, exists := te.tasks[name]; !exists {
			return nil, fmt.Errorf("task %s not found", name)
		}
		taskSet[name] = true
		inDegree[name] = 0
	}

	for _, name := range taskNames {
		task := te.tasks[name]
		for _, dep := range task.DependsOn {
			if !taskSet[dep] {
				continue
			}
			adj[dep] = append(adj[dep], name)
			inDegree[name]++
		}
	}

	var queue []string
	for name, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, name)
		}
	}

	var order []string
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		order = append(order, current)

		for _, neighbor := range adj[current] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	if len(order) != len(taskNames) {
		return nil, fmt.Errorf("circular dependency detected")
	}

	return order, nil
}

func (te *TaskExecutor) findReadyTasks(pending map[string]bool, results map[string]*TaskResult) []string {
	var ready []string

	for name := range pending {
		task := te.tasks[name]

		allDepsDone := true
		for _, dep := range task.DependsOn {
			result, exists := results[dep]
			if !exists || (result.State != StateCompleted && result.State != StateSkipped) {
				allDepsDone = false
				break
			}
		}

		if allDepsDone {
			ready = append(ready, name)
		}
	}

	return ready
}

func (te *TaskExecutor) GetTaskNames() []string {
	names := make([]string, 0, len(te.tasks))
	for name := range te.tasks {
		names = append(names, name)
	}
	return names
}

func (te *TaskExecutor) HasTask(name string) bool {
	_, exists := te.tasks[name]
	return exists
}

func ParseMinline(minline string) (need1Min, need5Min bool, err error) {
	if minline == "" {
		return false, false, nil
	}

	parts := strings.Split(minline, ",")
	seen := make(map[string]bool)

	for _, p := range parts {
		p = strings.TrimSpace(p)
		if seen[p] {
			continue
		}
		seen[p] = true

		switch p {
		case "1":
			need1Min = true
		case "5":
			need5Min = true
		default:
			return false, false, fmt.Errorf("invalid minline value: %s (expected '1', '5', '1,5')", minline)
		}
	}

	return need1Min, need5Min, nil
}

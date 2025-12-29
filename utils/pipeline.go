package utils

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// PipelineResult 执行结果统计
type PipelineResult struct {
	TotalItems     int
	ProcessedItems int64
	OutputRows     int64
	Errors         []error
	Duration       time.Duration
}

// Pipeline 通用并发处理管道
type Pipeline[I, O any] struct {
	concurrency int
	bufferSize  int

	processedItems atomic.Int64
	outputRows     atomic.Int64

	errors []error
	errMu  sync.Mutex
}

type PipelineOption func(*pipelineConfig)

type pipelineConfig struct {
	concurrency int
	bufferSize  int
}

func WithConcurrency(n int) PipelineOption {
	return func(c *pipelineConfig) {
		if n > 0 {
			c.concurrency = n
		}
	}
}

func WithBufferSize(n int) PipelineOption {
	return func(c *pipelineConfig) {
		if n > 0 {
			c.bufferSize = n
		}
	}
}

func NewPipeline[I, O any](opts ...PipelineOption) *Pipeline[I, O] {
	cfg := &pipelineConfig{
		concurrency: runtime.NumCPU(),
	}

	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.bufferSize == 0 {
		cfg.bufferSize = cfg.concurrency * 4
	}

	return &Pipeline[I, O]{
		concurrency: cfg.concurrency,
		bufferSize:  cfg.bufferSize,
	}
}

type batchResult[O any] struct {
	Rows []O
	Err  error
}

func (p *Pipeline[I, O]) Run(
	ctx context.Context,
	inputs []I,
	process func(ctx context.Context, input I) ([]O, error),
	consume func(rows []O) error,
) (*PipelineResult, error) {
	startTime := time.Now()

	if len(inputs) == 0 {
		return &PipelineResult{Duration: time.Since(startTime)}, nil
	}

	p.processedItems.Store(0)
	p.outputRows.Store(0)
	p.errors = nil

	resultChan := make(chan batchResult[O], p.bufferSize)

	sem := make(chan struct{}, p.concurrency)
	var producerWg sync.WaitGroup
	var consumerWg sync.WaitGroup
	consumerWg.Add(1)
	go func() {
		defer consumerWg.Done()
		for batch := range resultChan {
			if batch.Err != nil {
				p.collectError(batch.Err)
				continue
			}

			if len(batch.Rows) > 0 {
				if err := consume(batch.Rows); err != nil {
					p.collectError(fmt.Errorf("consume error: %w", err))
				} else {
					p.outputRows.Add(int64(len(batch.Rows)))
				}
			}
		}
	}()

	for _, input := range inputs {
		producerWg.Add(1)

		go func() {
			defer producerWg.Done()
			defer func() {
				if r := recover(); r != nil {
					p.collectError(fmt.Errorf("panic processing input: %v", r))
				}
			}()

			sem <- struct{}{}
			defer func() { <-sem }()

			rows, err := process(ctx, input)

			resultChan <- batchResult[O]{Rows: rows, Err: err}

			if err == nil {
				p.processedItems.Add(1)
			}
		}()
	}

	producerWg.Wait()
	close(resultChan)
	consumerWg.Wait()

	result := &PipelineResult{
		TotalItems:     len(inputs),
		ProcessedItems: p.processedItems.Load(),
		OutputRows:     p.outputRows.Load(),
		Errors:         p.getErrors(),
		Duration:       time.Since(startTime),
	}

	return result, nil
}

func (p *Pipeline[I, O]) RunWithWriter(
	ctx context.Context,
	inputs []I,
	process func(ctx context.Context, input I) ([]O, error),
	writer *CSVWriter[O],
) (*PipelineResult, error) {
	return p.Run(ctx, inputs, process, func(rows []O) error {
		return writer.Write(rows)
	})
}

func (p *Pipeline[I, O]) collectError(err error) {
	p.errMu.Lock()
	p.errors = append(p.errors, err)
	p.errMu.Unlock()
}

func (p *Pipeline[I, O]) getErrors() []error {
	p.errMu.Lock()
	defer p.errMu.Unlock()

	if len(p.errors) == 0 {
		return nil
	}

	result := make([]error, len(p.errors))
	copy(result, p.errors)
	return result
}

func (r *PipelineResult) HasErrors() bool {
	return len(r.Errors) > 0
}

func (r *PipelineResult) FirstError() error {
	if len(r.Errors) > 0 {
		return r.Errors[0]
	}
	return nil
}

func (r *PipelineResult) ErrorSummary() string {
	if len(r.Errors) == 0 {
		return ""
	}
	return fmt.Sprintf("%d errors, first: %v", len(r.Errors), r.Errors[0])
}

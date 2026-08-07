package backend

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

type countingBackend struct {
	labelNamesCount  atomic.Int32
	labelValuesCount atomic.Int32
	queryCount       atomic.Int32
	queryRangeCount  atomic.Int32
	seriesCount      atomic.Int32
	delay            time.Duration
}

func (c *countingBackend) Config(ctx context.Context) (v1.ConfigResult, error) {
	return v1.ConfigResult{}, nil
}

func (c *countingBackend) LabelNames(ctx context.Context, matches []string, startTime, endTime time.Time, opts ...v1.Option) ([]string, v1.Warnings, error) {
	c.labelNamesCount.Add(1)
	time.Sleep(c.delay)
	return []string{"app", "job"}, nil, nil
}

func (c *countingBackend) LabelValues(ctx context.Context, label string, matches []string, startTime, endTime time.Time, opts ...v1.Option) (model.LabelValues, v1.Warnings, error) {
	c.labelValuesCount.Add(1)
	time.Sleep(c.delay)
	return model.LabelValues{"val1", "val2"}, nil, nil
}

func (c *countingBackend) Query(ctx context.Context, query string, ts time.Time, opts ...v1.Option) (model.Value, v1.Warnings, error) {
	c.queryCount.Add(1)
	time.Sleep(c.delay)
	return model.Vector{}, nil, nil
}

func (c *countingBackend) QueryRange(ctx context.Context, query string, r v1.Range, opts ...v1.Option) (model.Value, v1.Warnings, error) {
	c.queryRangeCount.Add(1)
	time.Sleep(c.delay)
	return model.Matrix{}, nil, nil
}

func (c *countingBackend) Series(ctx context.Context, matches []string, startTime, endTime time.Time, opts ...v1.Option) ([]model.LabelSet, v1.Warnings, error) {
	c.seriesCount.Add(1)
	time.Sleep(c.delay)
	return []model.LabelSet{{"__name__": "up"}}, nil, nil
}

func TestSingleflightClientDeduplicatesConcurrentRequests(t *testing.T) {
	base := &countingBackend{delay: 50 * time.Millisecond}
	sfClient := NewSingleflightClient(base)

	ctx := context.Background()
	var wg sync.WaitGroup

	const concurrency = 10

	// Test QueryRange deduplication
	wg.Add(concurrency)
	r := v1.Range{Start: time.Unix(1000, 0), End: time.Unix(2000, 0), Step: time.Minute}
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			_, _, err := sfClient.QueryRange(ctx, "up", r)
			if err != nil {
				t.Errorf("QueryRange error: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := base.queryRangeCount.Load(); got != 1 {
		t.Fatalf("queryRangeCount = %d, want 1 due to singleflight deduplication", got)
	}

	// Test LabelValues deduplication
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			_, _, err := sfClient.LabelValues(ctx, "job", []string{`{__name__="up"}`}, time.Unix(1000, 0), time.Unix(2000, 0))
			if err != nil {
				t.Errorf("LabelValues error: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := base.labelValuesCount.Load(); got != 1 {
		t.Fatalf("labelValuesCount = %d, want 1 due to singleflight deduplication", got)
	}

	// Test LabelNames deduplication
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			_, _, err := sfClient.LabelNames(ctx, []string{`{__name__="up"}`}, time.Unix(1000, 0), time.Unix(2000, 0))
			if err != nil {
				t.Errorf("LabelNames error: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := base.labelNamesCount.Load(); got != 1 {
		t.Fatalf("labelNamesCount = %d, want 1 due to singleflight deduplication", got)
	}
}

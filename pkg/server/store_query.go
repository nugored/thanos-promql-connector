package server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"main.go/pkg/backend"
	"main.go/pkg/promql"
)

type QueryServer struct {
	logger             log.Logger
	backends           []backend.QueryBackendEndpoint
	dropLabels         promql.LabelDropSet
	SeriesStep         time.Duration
	MaxPointsPerSeries int
	labelCache         *labelMatchCache
	labelNamesCache    *labelNamesCache
	labelValuesCache   *labelValuesCache
}

func NewQueryServer(logger log.Logger, queryBackendClient backend.QueryBackendAPI, dropLabels []string, externalLabels func() labels.Labels, seriesStep time.Duration, maxPointsPerSeries int, labelCacheTTL time.Duration) *QueryServer {
	return NewQueryServerFromBackends(logger, []backend.QueryBackendEndpoint{{
		Name:           "backend",
		Client:         queryBackendClient,
		ExternalLabels: externalLabels,
	}}, dropLabels, seriesStep, maxPointsPerSeries, labelCacheTTL)
}

func NewQueryServerFromBackends(logger log.Logger, backends []backend.QueryBackendEndpoint, dropLabels []string, seriesStep time.Duration, maxPointsPerSeries int, labelCacheTTL time.Duration) *QueryServer {
	return NewQueryServerWithCacheTTLs(logger, backends, dropLabels, seriesStep, maxPointsPerSeries, labelCacheTTL, labelCacheTTL, labelCacheTTL)
}

func NewQueryServerWithCacheConfig(logger log.Logger, backends []backend.QueryBackendEndpoint, dropLabels []string, seriesStep time.Duration, maxPointsPerSeries int, labelCacheTTL, labelNamesCacheTTL, labelValuesCacheTTL time.Duration, labelCacheMaxEntries, labelNamesCacheMaxEntries, labelValuesCacheMaxEntries int) *QueryServer {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	normalized := make([]backend.QueryBackendEndpoint, 0, len(backends))
	for _, b := range backends {
		if b.ExternalLabels == nil {
			b.ExternalLabels = labels.EmptyLabels
		}
		normalized = append(normalized, b)
	}
	if len(normalized) == 0 {
		normalized = append(normalized, backend.QueryBackendEndpoint{
			Name:           "backend",
			ExternalLabels: labels.EmptyLabels,
		})
	}

	if seriesStep <= 0 {
		seriesStep = time.Minute
	}

	return &QueryServer{
		logger:             logger,
		backends:           normalized,
		dropLabels:         promql.NewLabelDropSet(dropLabels),
		SeriesStep:         seriesStep,
		MaxPointsPerSeries: maxPointsPerSeries,
		labelCache:         newLabelMatchCache(labelCacheTTL, labelCacheMaxEntries),
		labelNamesCache:    newLabelNamesCache(labelNamesCacheTTL, labelNamesCacheMaxEntries),
		labelValuesCache:   newLabelValuesCache(labelValuesCacheTTL, labelValuesCacheMaxEntries),
	}
}

func NewQueryServerWithCacheTTLs(logger log.Logger, backends []backend.QueryBackendEndpoint, dropLabels []string, seriesStep time.Duration, maxPointsPerSeries int, labelCacheTTL, labelNamesCacheTTL, labelValuesCacheTTL time.Duration) *QueryServer {
	return NewQueryServerWithCacheConfig(logger, backends, dropLabels, seriesStep, maxPointsPerSeries, labelCacheTTL, labelNamesCacheTTL, labelValuesCacheTTL, 10000, 10000, 10000)
}

func (qs *QueryServer) PurgeExpiredCaches() {
	if qs == nil {
		return
	}
	qs.labelCache.PurgeExpired()
	qs.labelNamesCache.PurgeExpired()
	qs.labelValuesCache.PurgeExpired()
}

func (qs *QueryServer) Series(request *storepb.SeriesRequest, server storepb.Store_SeriesServer) error {
	start := promql.TimeFromMillis(request.MinTime)
	end := promql.TimeFromMillis(request.MaxTime)
	if end.Before(start) {
		return status.Error(codes.InvalidArgument, "max_time must be greater than or equal to min_time")
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	matched := false
	warningsSet := make(map[string]struct{})
	var warnings v1.Warnings
	seriesSet := make([]storepb.Series, 0)
	var lastErr error
	successCount := 0

	for _, b := range qs.backends {
		b := b
		externalLabels := b.Labels()
		match, matchers, err := promql.MatchesExternalLabels(request.Matchers, externalLabels)
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		if !match {
			continue
		}
		if len(matchers) == 0 {
			return status.Error(codes.InvalidArgument, "no matchers specified (excluding external labels)")
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			selector := promql.QuerySelectorFromPromMatchers(matchers)
			var bSeriesSet []storepb.Series
			var bWarnings v1.Warnings

			if request.SkipChunks {
				var bErr error
				bSeriesSet, bWarnings, bErr = qs.seriesMetadataFromBackend(server.Context(), b, externalLabels, request, selector, start, end)
				if bErr != nil {
					level.Error(qs.logger).Log("msg", "backend series metadata query failed", "backend", b.Name, "selector", selector, "err", bErr)
					mu.Lock()
					lastErr = status.Error(codes.Aborted, bErr.Error())
					wStr := fmt.Sprintf("backend %s series metadata query failed: %v", b.Name, bErr)
					if _, ok := warningsSet[wStr]; !ok {
						warningsSet[wStr] = struct{}{}
						warnings = append(warnings, wStr)
					}
					mu.Unlock()
					return
				}
			} else {
				interval := v1.Range{
					Start: start,
					End:   end,
					Step:  promql.SeriesStepForRange(request, qs.SeriesStep, start, end, qs.MaxPointsPerSeries),
				}
				values, w, bErr := b.Client.QueryRange(server.Context(), selector, interval)
				bWarnings = w
				if bErr != nil {
					level.Error(qs.logger).Log("msg", "backend series query_range failed", "backend", b.Name, "selector", selector, "err", bErr)
					mu.Lock()
					lastErr = status.Error(codes.Aborted, bErr.Error())
					wStr := fmt.Sprintf("backend %s series query_range failed: %v", b.Name, bErr)
					if _, ok := warningsSet[wStr]; !ok {
						warningsSet[wStr] = struct{}{}
						warnings = append(warnings, wStr)
					}
					mu.Unlock()
					return
				}

				matrix, ok := values.(model.Matrix)
				if !ok {
					err := fmt.Errorf("backend returned %T for series selector %q, want model.Matrix", values, selector)
					level.Error(qs.logger).Log("msg", "unexpected response type for backend series query_range", "backend", b.Name, "selector", selector, "err", err)
					mu.Lock()
					lastErr = status.Error(codes.Internal, err.Error())
					wStr := fmt.Sprintf("backend %s series query_range invalid response: %v", b.Name, err)
					if _, ok := warningsSet[wStr]; !ok {
						warningsSet[wStr] = struct{}{}
						warnings = append(warnings, wStr)
					}
					mu.Unlock()
					return
				}

				for _, result := range matrix {
					if result == nil {
						continue
					}
					var chunks []storepb.AggrChunk
					var err error
					if len(result.Histograms) > 0 {
						chunks, err = promql.ChunksFromModelHistogramSamples(result.Histograms)
					} else if len(result.Values) > 0 {
						chunks, err = promql.ChunksFromModelSamples(result.Values)
					} else {
						continue
					}
					if err != nil {
						mu.Lock()
						lastErr = status.Error(codes.Internal, err.Error())
						mu.Unlock()
						return
					}
					bSeriesSet = append(bSeriesSet, storepb.Series{
						Labels: qs.dropLabels.ZLabelsFromMetric(result.Metric, externalLabels, request.WithoutReplicaLabels),
						Chunks: chunks,
					})
				}
			}

			mu.Lock()
			matched = true
			successCount++
			seriesSet = append(seriesSet, bSeriesSet...)
			for _, w := range bWarnings {
				if _, ok := warningsSet[w]; !ok {
					warningsSet[w] = struct{}{}
					warnings = append(warnings, w)
				}
			}
			mu.Unlock()
		}()
	}

	wg.Wait()

	if !matched {
		return nil
	}

	if successCount == 0 && lastErr != nil {
		return lastErr
	}

	if err := promql.SendStoreWarnings(server, warnings); err != nil {
		return err
	}

	promql.SortStoreSeries(seriesSet)

	var sent int64
	for i := range seriesSet {
		if err := server.Send(storepb.NewSeriesResponse(&seriesSet[i])); err != nil {
			return err
		}
		sent++
		if request.Limit > 0 && sent >= request.Limit {
			return nil
		}
	}
	return nil
}

func (qs *QueryServer) seriesMetadataFromBackend(ctx context.Context, b backend.QueryBackendEndpoint, externalLabels labels.Labels, request *storepb.SeriesRequest, selector string, start, end time.Time) ([]storepb.Series, v1.Warnings, error) {
	labelSets, warnings, err := b.Client.Series(ctx, []string{selector}, start, end)
	if err != nil {
		if strings.Contains(err.Error(), "bad_data") || strings.Contains(err.Error(), "400") {
			level.Warn(qs.logger).Log("msg", "backend series metadata query returned bad_data, returning empty series set", "backend", b.Name, "selector", selector, "err", err)
			return nil, v1.Warnings{fmt.Sprintf("backend %s series metadata query returned bad_data: %v", b.Name, err)}, nil
		}
		return nil, nil, err
	}

	seriesSet := make([]storepb.Series, 0, len(labelSets))
	for _, labelSet := range labelSets {
		seriesSet = append(seriesSet, storepb.Series{
			Labels: qs.dropLabels.ZLabelsFromLabelSet(labelSet, externalLabels, request.WithoutReplicaLabels),
		})
	}
	return seriesSet, warnings, nil
}

func (qs *QueryServer) LabelNames(ctx context.Context, request *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	nameSet := make(map[string]struct{})
	var warnings v1.Warnings
	warningsSet := make(map[string]struct{})
	var lastErr error
	successCount := 0
	matchedBackends := 0

	for _, b := range qs.backends {
		b := b
		externalLabels := b.Labels()
		match, promMatchers, err := promql.MatchesExternalLabels(request.Matchers, externalLabels)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if !match {
			continue
		}
		matchedBackends++

		matches := promql.LabelAPISelectorsFromPromMatchers(promMatchers)
		startTime := promql.TimeFromMillis(request.Start)
		endTime := promql.TimeFromMillis(request.End)

		wg.Add(1)
		go func() {
			defer wg.Done()
			names, backendWarnings, cached := qs.labelNamesCache.Get(b.Name, matches, startTime, endTime)
			if !cached {
				var backendErr error
				names, backendWarnings, backendErr = b.Client.LabelNames(ctx, matches, startTime, endTime)
				if backendErr != nil {
					level.Warn(qs.logger).Log("msg", "backend LabelNames query returned error, returning empty names", "backend", b.Name, "matches", fmt.Sprint(matches), "err", backendErr)
					mu.Lock()
					wStr := fmt.Sprintf("backend %s LabelNames query failed: %v", b.Name, backendErr)
					if _, ok := warningsSet[wStr]; !ok {
						warningsSet[wStr] = struct{}{}
						warnings = append(warnings, wStr)
					}
					successCount++
					mu.Unlock()
					return
				}
				qs.labelNamesCache.Put(b.Name, matches, startTime, endTime, names, backendWarnings)
			}

			droppedNames := qs.dropLabels.LabelNames(names, externalLabels, request.WithoutReplicaLabels)

			mu.Lock()
			successCount++
			for _, name := range droppedNames {
				nameSet[name] = struct{}{}
			}
			for _, w := range backendWarnings {
				if _, ok := warningsSet[w]; !ok {
					warningsSet[w] = struct{}{}
					warnings = append(warnings, w)
				}
			}
			mu.Unlock()
		}()
	}

	wg.Wait()

	if matchedBackends > 0 && successCount == 0 && lastErr != nil {
		return nil, lastErr
	}

	names := make([]string, 0, len(nameSet))
	for name := range nameSet {
		names = append(names, name)
	}
	sort.Strings(names)
	return &storepb.LabelNamesResponse{
		Names:    names,
		Warnings: warnings,
		Hints:    nil,
	}, nil
}

func (qs *QueryServer) LabelValues(ctx context.Context, request *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	if request.Label == "" {
		return nil, status.Error(codes.InvalidArgument, "label name parameter cannot be empty")
	}
	if qs.dropLabels.Has(request.Label) || promql.ContainsString(request.WithoutReplicaLabels, request.Label) {
		return &storepb.LabelValuesResponse{}, nil
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	valueSet := make(map[string]struct{})
	var warnings v1.Warnings
	warningsSet := make(map[string]struct{})
	var lastErr error
	successCount := 0
	matchedBackends := 0

	for _, b := range qs.backends {
		b := b
		externalLabels := b.Labels()
		match, promMatchers, err := promql.MatchesExternalLabels(request.Matchers, externalLabels)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if !match {
			continue
		}
		matchedBackends++

		wg.Add(1)
		go func() {
			defer wg.Done()
			if value := externalLabels.Get(request.Label); value != "" {
				if len(promMatchers) == 0 {
					mu.Lock()
					valueSet[value] = struct{}{}
					successCount++
					mu.Unlock()
					return
				}

				matches := promql.LabelAPISelectorsFromPromMatchers(promMatchers)
				selector := promql.QuerySelectorFromPromMatchers(promMatchers)
				start := promql.TimeFromMillis(request.Start)
				end := promql.TimeFromMillis(request.End)

				matched, backendWarnings, err := qs.hasMatchingSeries(ctx, b, selector, matches, start, end)
				if err != nil {
					level.Warn(qs.logger).Log("msg", "backend hasMatchingSeries failed, returning empty values", "backend", b.Name, "selector", selector, "err", err)
					mu.Lock()
					wStr := fmt.Sprintf("backend %s hasMatchingSeries failed: %v", b.Name, err)
					if _, ok := warningsSet[wStr]; !ok {
						warningsSet[wStr] = struct{}{}
						warnings = append(warnings, wStr)
					}
					successCount++
					mu.Unlock()
					return
				}

				mu.Lock()
				successCount++
				if matched {
					valueSet[value] = struct{}{}
				}
				for _, w := range backendWarnings {
					if _, ok := warningsSet[w]; !ok {
						warningsSet[w] = struct{}{}
						warnings = append(warnings, w)
					}
				}
				mu.Unlock()
				return
			}

			matches := promql.LabelAPISelectorsFromPromMatchers(promMatchers)
			startTime := promql.TimeFromMillis(request.Start)
			endTime := promql.TimeFromMillis(request.End)

			req, backendWarnings, cached := qs.labelValuesCache.Get(b.Name, request.Label, matches, startTime, endTime)
			if !cached {
				var backendErr error
				req, backendWarnings, backendErr = b.Client.LabelValues(ctx, request.Label, matches, startTime, endTime)
				if backendErr != nil {
					level.Warn(qs.logger).Log("msg", "backend LabelValues query failed, returning empty values", "backend", b.Name, "label", request.Label, "matches", fmt.Sprint(matches), "err", backendErr)
					mu.Lock()
					wStr := fmt.Sprintf("backend %s LabelValues query failed: %v", b.Name, backendErr)
					if _, ok := warningsSet[wStr]; !ok {
						warningsSet[wStr] = struct{}{}
						warnings = append(warnings, wStr)
					}
					successCount++
					mu.Unlock()
					return
				}
				qs.labelValuesCache.Put(b.Name, request.Label, matches, startTime, endTime, req, backendWarnings)
			}

			mu.Lock()
			successCount++
			for _, value := range req {
				valueSet[string(value)] = struct{}{}
			}
			for _, w := range backendWarnings {
				if _, ok := warningsSet[w]; !ok {
					warningsSet[w] = struct{}{}
					warnings = append(warnings, w)
				}
			}
			mu.Unlock()
		}()
	}

	wg.Wait()

	if matchedBackends > 0 && successCount == 0 && lastErr != nil {
		return nil, lastErr
	}

	values := make([]string, 0, len(valueSet))
	for value := range valueSet {
		values = append(values, value)
	}
	sort.Strings(values)
	return &storepb.LabelValuesResponse{
		Values:   values,
		Warnings: warnings,
		Hints:    nil,
	}, nil
}

func (qs *QueryServer) Query(req *querypb.QueryRequest, srv querypb.Query_QueryServer) error {
	ts := time.Unix(req.TimeSeconds, 0)
	timeout := time.Duration(req.TimeoutSeconds) * time.Second
	rawQuery, err := promql.QueryStringFromRequestPlan(req.Query, req.QueryPlan)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	resultsSeries := make([]*prompb.TimeSeries, 0)
	var warnings []string
	warningsSet := make(map[string]struct{})
	var lastErr error
	successCount := 0
	matchedBackends := 0

	for _, b := range qs.backends {
		b := b
		externalLabels := b.Labels()
		query, match, err := promql.RewriteQueryForExternalLabels(rawQuery, externalLabels)
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		if !match {
			continue
		}
		matchedBackends++

		wg.Add(1)
		go func() {
			defer wg.Done()
			values, w, err := b.Client.Query(srv.Context(), query, ts, v1.WithTimeout(timeout))
			if err != nil {
				level.Error(qs.logger).Log("msg", "backend instant query failed", "backend", b.Name, "query", query, "err", err)
				mu.Lock()
				lastErr = status.Error(codes.Aborted, err.Error())
				wStr := fmt.Sprintf("backend %s instant query failed: %v", b.Name, err)
				if _, ok := warningsSet[wStr]; !ok {
					warningsSet[wStr] = struct{}{}
					warnings = append(warnings, wStr)
				}
				mu.Unlock()
				return
			}

			var bSeries []*prompb.TimeSeries
			switch results := values.(type) {
			case model.Vector:
				for _, result := range results {
					if result == nil {
						continue
					}
					if result.Histogram != nil {
						bSeries = append(bSeries, &prompb.TimeSeries{
							Histograms: []prompb.Histogram{promql.SampleHistogramToProto(result)},
							Labels:     qs.dropLabels.ZLabelsFromMetric(result.Metric, externalLabels, nil),
						})
					} else {
						bSeries = append(bSeries, &prompb.TimeSeries{
							Samples: []prompb.Sample{{Value: float64(result.Value), Timestamp: int64(result.Timestamp)}},
							Labels:  qs.dropLabels.ZLabelsFromMetric(result.Metric, externalLabels, nil),
						})
					}
				}
			case *model.Scalar:
				if results != nil {
					bSeries = append(bSeries, &prompb.TimeSeries{
						Samples: []prompb.Sample{{Value: float64(results.Value), Timestamp: int64(results.Timestamp)}},
					})
				}
			}

			mu.Lock()
			successCount++
			resultsSeries = append(resultsSeries, bSeries...)
			for _, warn := range w {
				if _, ok := warningsSet[warn]; !ok {
					warningsSet[warn] = struct{}{}
					warnings = append(warnings, warn)
				}
			}
			mu.Unlock()
		}()
	}

	wg.Wait()

	if matchedBackends > 0 && successCount == 0 && lastErr != nil {
		return lastErr
	}

	if len(warnings) > 0 {
		errs := make([]error, 0, len(warnings))
		for _, warning := range warnings {
			errs = append(errs, errors.New(warning))
		}
		if err = srv.SendMsg(querypb.NewQueryWarningsResponse(errs...)); err != nil {
			return err
		}
	}

	for _, series := range resultsSeries {
		if err := srv.Send(querypb.NewQueryResponse(series)); err != nil {
			return err
		}
	}

	return nil
}

func (qs *QueryServer) QueryRange(req *querypb.QueryRangeRequest, srv querypb.Query_QueryRangeServer) error {
	timeout := time.Duration(req.TimeoutSeconds) * time.Second
	interval := v1.Range{
		Start: time.Unix(req.StartTimeSeconds, 0),
		End:   time.Unix(req.EndTimeSeconds, 0),
		Step:  time.Duration(req.IntervalSeconds) * time.Second,
	}
	rawQuery, err := promql.QueryStringFromRequestPlan(req.Query, req.QueryPlan)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	resultsSeries := make([]*prompb.TimeSeries, 0)
	var warnings []string
	warningsSet := make(map[string]struct{})
	var lastErr error
	successCount := 0
	matchedBackends := 0

	for _, b := range qs.backends {
		b := b
		externalLabels := b.Labels()
		query, match, err := promql.RewriteQueryForExternalLabels(rawQuery, externalLabels)
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		if !match {
			continue
		}
		matchedBackends++

		wg.Add(1)
		go func() {
			defer wg.Done()
			values, w, err := b.Client.QueryRange(srv.Context(), query, interval, v1.WithTimeout(timeout))
			if err != nil {
				level.Error(qs.logger).Log("msg", "backend range query failed", "backend", b.Name, "query", query, "err", err)
				mu.Lock()
				lastErr = status.Error(codes.Aborted, err.Error())
				wStr := fmt.Sprintf("backend %s range query failed: %v", b.Name, err)
				if _, ok := warningsSet[wStr]; !ok {
					warningsSet[wStr] = struct{}{}
					warnings = append(warnings, wStr)
				}
				mu.Unlock()
				return
			}

			var bSeries []*prompb.TimeSeries
			switch results := values.(type) {
			case model.Matrix:
				for _, result := range results {
					if result == nil {
						continue
					}
					if len(result.Histograms) > 0 {
						histograms := make([]prompb.Histogram, 0, len(result.Histograms))
						for _, h := range result.Histograms {
							histograms = append(histograms, promql.SampleHistogramPairToProto(h))
						}
						bSeries = append(bSeries, &prompb.TimeSeries{
							Histograms: histograms,
							Labels:     qs.dropLabels.ZLabelsFromMetric(result.Metric, externalLabels, nil),
						})
					} else if len(result.Values) > 0 {
						bSeries = append(bSeries, &prompb.TimeSeries{
							Samples: promql.SamplesFromModel(result.Values),
							Labels:  qs.dropLabels.ZLabelsFromMetric(result.Metric, externalLabels, nil),
						})
					}
				}
			case model.Vector:
				for _, result := range results {
					if result == nil {
						continue
					}
					if result.Histogram != nil {
						bSeries = append(bSeries, &prompb.TimeSeries{
							Histograms: []prompb.Histogram{promql.SampleHistogramToProto(result)},
							Labels:     qs.dropLabels.ZLabelsFromMetric(result.Metric, externalLabels, nil),
						})
					} else {
						bSeries = append(bSeries, &prompb.TimeSeries{
							Samples: []prompb.Sample{{Value: float64(result.Value), Timestamp: int64(result.Timestamp)}},
							Labels:  qs.dropLabels.ZLabelsFromMetric(result.Metric, externalLabels, nil),
						})
					}
				}
			case *model.Scalar:
				if results != nil {
					bSeries = append(bSeries, &prompb.TimeSeries{
						Samples: []prompb.Sample{{Value: float64(results.Value), Timestamp: int64(results.Timestamp)}},
					})
				}
			}

			mu.Lock()
			successCount++
			resultsSeries = append(resultsSeries, bSeries...)
			for _, warn := range w {
				if _, ok := warningsSet[warn]; !ok {
					warningsSet[warn] = struct{}{}
					warnings = append(warnings, warn)
				}
			}
			mu.Unlock()
		}()
	}

	wg.Wait()

	if matchedBackends > 0 && successCount == 0 && lastErr != nil {
		return lastErr
	}

	if len(warnings) > 0 {
		errs := make([]error, 0, len(warnings))
		for _, warning := range warnings {
			errs = append(errs, errors.New(warning))
		}
		if err = srv.SendMsg(querypb.NewQueryRangeWarningsResponse(errs...)); err != nil {
			return err
		}
	}

	for _, series := range resultsSeries {
		if err := srv.Send(querypb.NewQueryRangeResponse(series)); err != nil {
			return err
		}
	}

	return nil
}

func (qs *QueryServer) hasMatchingSeries(ctx context.Context, b backend.QueryBackendEndpoint, selector string, matches []string, start, end time.Time) (bool, v1.Warnings, error) {
	if matched, ok := qs.labelCache.Get(b.Name, selector); ok {
		return matched, nil, nil
	}

	ts := end
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	// 1. Fast path: Instant query for recent samples (last 5m)
	val, warnings, err := b.Client.Query(ctx, selector, ts)
	if err == nil && valueHasSamples(val) {
		qs.labelCache.Put(b.Name, selector, true)
		return true, warnings, nil
	}

	// 2. Range selector query: for sparse/infrequent metrics (e.g. GCP Cloud Storage metrics),
	// query over a 6-hour lookback window (or the requested start-end range)
	lookback := 6 * time.Hour
	if !start.IsZero() && !end.IsZero() && end.After(start) {
		reqLookback := end.Sub(start)
		if reqLookback > lookback {
			lookback = reqLookback
		}
	}
	rangeSelector := fmt.Sprintf("%s[%dm]", selector, int(lookback.Minutes()))
	rangeVal, rangeWarnings, rangeErr := b.Client.Query(ctx, rangeSelector, ts)
	warnings = append(warnings, rangeWarnings...)
	if rangeErr == nil && valueHasSamples(rangeVal) {
		qs.labelCache.Put(b.Name, selector, true)
		return true, warnings, nil
	}

	// 3. Fallback to Series API
	seriesList, seriesWarnings, seriesErr := b.Client.Series(ctx, matches, start, end)
	warnings = append(warnings, seriesWarnings...)
	if seriesErr == nil && len(seriesList) > 0 {
		qs.labelCache.Put(b.Name, selector, true)
		return true, warnings, nil
	}

	qs.labelCache.Put(b.Name, selector, false)
	return false, warnings, nil
}

func valueHasSamples(val model.Value) bool {
	if val == nil {
		return false
	}
	switch v := val.(type) {
	case model.Vector:
		return len(v) > 0
	case model.Matrix:
		for _, stream := range v {
			if stream != nil && len(stream.Values) > 0 {
				return true
			}
		}
		return false
	case *model.Scalar:
		return v != nil
	default:
		return false
	}
}

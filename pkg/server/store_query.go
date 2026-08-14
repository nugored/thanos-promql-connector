package server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

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
	backends           []backend.QueryBackendEndpoint
	dropLabels         promql.LabelDropSet
	SeriesStep         time.Duration
	MaxPointsPerSeries int
	labelCache         *labelMatchCache
	labelNamesCache    *labelNamesCache
	labelValuesCache   *labelValuesCache
}

func NewQueryServer(queryBackendClient backend.QueryBackendAPI, dropLabels []string, externalLabels func() labels.Labels, seriesStep time.Duration, maxPointsPerSeries int, labelCacheTTL time.Duration) *QueryServer {
	return NewQueryServerFromBackends([]backend.QueryBackendEndpoint{{
		Name:           "backend",
		Client:         queryBackendClient,
		ExternalLabels: externalLabels,
	}}, dropLabels, seriesStep, maxPointsPerSeries, labelCacheTTL)
}

func NewQueryServerFromBackends(backends []backend.QueryBackendEndpoint, dropLabels []string, seriesStep time.Duration, maxPointsPerSeries int, labelCacheTTL time.Duration) *QueryServer {
	return NewQueryServerWithCacheTTLs(backends, dropLabels, seriesStep, maxPointsPerSeries, labelCacheTTL, labelCacheTTL, labelCacheTTL)
}

func NewQueryServerWithCacheTTLs(backends []backend.QueryBackendEndpoint, dropLabels []string, seriesStep time.Duration, maxPointsPerSeries int, labelCacheTTL, labelNamesCacheTTL, labelValuesCacheTTL time.Duration) *QueryServer {
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
		backends:           normalized,
		dropLabels:         promql.NewLabelDropSet(dropLabels),
		SeriesStep:         seriesStep,
		MaxPointsPerSeries: maxPointsPerSeries,
		labelCache:         newLabelMatchCache(labelCacheTTL),
		labelNamesCache:    newLabelNamesCache(labelNamesCacheTTL),
		labelValuesCache:   newLabelValuesCache(labelValuesCacheTTL),
	}
}

func (qs *QueryServer) Series(request *storepb.SeriesRequest, server storepb.Store_SeriesServer) error {
	start := promql.TimeFromMillis(request.MinTime)
	end := promql.TimeFromMillis(request.MaxTime)
	if end.Before(start) {
		return status.Error(codes.InvalidArgument, "max_time must be greater than or equal to min_time")
	}

	matched := false
	var sent int64
	seriesSet := make([]storepb.Series, 0)
	for _, b := range qs.backends {
		externalLabels := b.Labels()
		match, matchers, err := promql.MatchesExternalLabels(request.Matchers, externalLabels)
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		if !match {
			continue
		}
		matched = true
		if len(matchers) == 0 {
			return status.Error(codes.InvalidArgument, "no matchers specified (excluding external labels)")
		}
		selector := promql.QuerySelectorFromPromMatchers(matchers)

		if request.SkipChunks {
			backendSeriesSet, warnings, err := qs.seriesMetadataFromBackend(server.Context(), b, externalLabels, request, selector, start, end)
			if err != nil {
				return status.Error(codes.Aborted, err.Error())
			}
			if err := promql.SendStoreWarnings(server, warnings); err != nil {
				return err
			}
			seriesSet = append(seriesSet, backendSeriesSet...)
			continue
		}

		interval := v1.Range{
			Start: start,
			End:   end,
			Step:  promql.SeriesStepForRange(request, qs.SeriesStep, start, end, qs.MaxPointsPerSeries),
		}
		values, warnings, err := b.Client.QueryRange(server.Context(), selector, interval)
		if err != nil {
			return status.Error(codes.Aborted, err.Error())
		}
		if err := promql.SendStoreWarnings(server, warnings); err != nil {
			return err
		}

		matrix, ok := values.(model.Matrix)
		if !ok {
			return status.Errorf(codes.Internal, "backend returned %T for series selector %q, want model.Matrix", values, selector)
		}

		for _, result := range matrix {
			if result == nil || len(result.Values) == 0 {
				continue
			}
			chunks, err := promql.ChunksFromModelSamples(result.Values)
			if err != nil {
				return status.Error(codes.Internal, err.Error())
			}
			seriesSet = append(seriesSet, storepb.Series{
				Labels: qs.dropLabels.ZLabelsFromMetric(result.Metric, externalLabels, request.WithoutReplicaLabels),
				Chunks: chunks,
			})
		}
	}
	if !matched {
		return nil
	}
	promql.SortStoreSeries(seriesSet)

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
	nameSet := make(map[string]struct{})
	var warnings v1.Warnings

	for _, b := range qs.backends {
		externalLabels := b.Labels()
		match, promMatchers, err := promql.MatchesExternalLabels(request.Matchers, externalLabels)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if !match {
			continue
		}

		matches := promql.LabelAPISelectorsFromPromMatchers(promMatchers)
		startTime := promql.TimeFromMillis(request.Start)
		endTime := promql.TimeFromMillis(request.End)

		names, backendWarnings, cached := qs.labelNamesCache.Get(b.Name, matches, startTime, endTime)
		if !cached {
			var backendErr error
			names, backendWarnings, backendErr = b.Client.LabelNames(ctx, matches, startTime, endTime)
			if backendErr != nil {
				return nil, status.Error(codes.Internal, backendErr.Error())
			}
			qs.labelNamesCache.Put(b.Name, matches, startTime, endTime, names, backendWarnings)
		}
		warnings = append(warnings, backendWarnings...)
		for _, name := range qs.dropLabels.LabelNames(names, externalLabels, request.WithoutReplicaLabels) {
			nameSet[name] = struct{}{}
		}
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

	valueSet := make(map[string]struct{})
	var warnings v1.Warnings

	for _, b := range qs.backends {
		externalLabels := b.Labels()
		match, promMatchers, err := promql.MatchesExternalLabels(request.Matchers, externalLabels)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if !match {
			continue
		}

		if value := externalLabels.Get(request.Label); value != "" {
			if len(promMatchers) == 0 {
				valueSet[value] = struct{}{}
				continue
			}

			matches := promql.LabelAPISelectorsFromPromMatchers(promMatchers)
			selector := promql.QuerySelectorFromPromMatchers(promMatchers)
			start := promql.TimeFromMillis(request.Start)
			end := promql.TimeFromMillis(request.End)

			matched, backendWarnings, err := qs.hasMatchingSeries(ctx, b, selector, matches, start, end)
			if err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			warnings = append(warnings, backendWarnings...)
			if matched {
				valueSet[value] = struct{}{}
			}
			continue
		}

		matches := promql.LabelAPISelectorsFromPromMatchers(promMatchers)
		startTime := promql.TimeFromMillis(request.Start)
		endTime := promql.TimeFromMillis(request.End)

		req, backendWarnings, cached := qs.labelValuesCache.Get(b.Name, request.Label, matches, startTime, endTime)
		if !cached {
			var backendErr error
			req, backendWarnings, backendErr = b.Client.LabelValues(ctx, request.Label, matches, startTime, endTime)
			if backendErr != nil {
				return nil, status.Error(codes.Internal, backendErr.Error())
			}
			qs.labelValuesCache.Put(b.Name, request.Label, matches, startTime, endTime, req, backendWarnings)
		}
		warnings = append(warnings, backendWarnings...)
		for _, value := range req {
			valueSet[string(value)] = struct{}{}
		}
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

	for _, b := range qs.backends {
		externalLabels := b.Labels()
		query, match, err := promql.RewriteQueryForExternalLabels(rawQuery, externalLabels)
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		if !match {
			continue
		}

		values, warnings, err := b.Client.Query(srv.Context(), query, ts, v1.WithTimeout(timeout))
		if err != nil {
			return status.Error(codes.Aborted, err.Error())
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
		switch results := values.(type) {
		case model.Vector:
			for _, result := range results {
				if result == nil {
					continue
				}
				series := &prompb.TimeSeries{
					Samples: []prompb.Sample{{Value: float64(result.Value), Timestamp: int64(result.Timestamp)}},
					Labels:  qs.dropLabels.ZLabelsFromMetric(result.Metric, externalLabels, nil),
				}
				if err := srv.Send(querypb.NewQueryResponse(series)); err != nil {
					return err
				}
			}
		case *model.Scalar:
			if results == nil {
				continue
			}
			series := &prompb.TimeSeries{Samples: []prompb.Sample{{Value: float64(results.Value), Timestamp: int64(results.Timestamp)}}}
			if err := srv.Send(querypb.NewQueryResponse(series)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (qs *QueryServer) QueryRange(req *querypb.QueryRangeRequest, srv querypb.Query_QueryRangeServer) error {
	timeout := time.Duration(req.TimeoutSeconds) * time.Second
	interval := v1.Range{
		Start: time.Unix(req.StartTimeSeconds, 0),
		End:   time.Unix(req.EndTimeSeconds, 0),
		Step:  time.Duration(req.IntervalSeconds) * time.Second}
	rawQuery, err := promql.QueryStringFromRequestPlan(req.Query, req.QueryPlan)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	for _, b := range qs.backends {
		externalLabels := b.Labels()
		query, match, err := promql.RewriteQueryForExternalLabels(rawQuery, externalLabels)
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		if !match {
			continue
		}

		values, warnings, err := b.Client.QueryRange(srv.Context(), query, interval, v1.WithTimeout(timeout))
		if err != nil {
			return status.Error(codes.Aborted, err.Error())
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
		switch results := values.(type) {
		case model.Matrix:
			for _, result := range results {
				if result == nil || len(result.Values) == 0 {
					continue
				}
				series := &prompb.TimeSeries{
					Samples: promql.SamplesFromModel(result.Values),
					Labels:  qs.dropLabels.ZLabelsFromMetric(result.Metric, externalLabels, nil),
				}
				if err := srv.Send(querypb.NewQueryRangeResponse(series)); err != nil {
					return err
				}
			}
		case model.Vector:
			for _, result := range results {
				if result == nil {
					continue
				}
				series := &prompb.TimeSeries{
					Samples: []prompb.Sample{{Value: float64(result.Value), Timestamp: int64(result.Timestamp)}},
					Labels:  qs.dropLabels.ZLabelsFromMetric(result.Metric, externalLabels, nil),
				}
				if err := srv.Send(querypb.NewQueryRangeResponse(series)); err != nil {
					return err
				}
			}
		case *model.Scalar:
			if results == nil {
				continue
			}
			series := &prompb.TimeSeries{Samples: []prompb.Sample{{Value: float64(results.Value), Timestamp: int64(results.Timestamp)}}}
			if err := srv.Send(querypb.NewQueryRangeResponse(series)); err != nil {
				return err
			}
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
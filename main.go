package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/status"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/component"
	_ "github.com/thanos-io/thanos/pkg/extgrpc/snappy"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"google.golang.org/api/option"
	apihttp "google.golang.org/api/transport/http"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"gopkg.in/yaml.v2"
)

var (
	queryTargetURL      = flag.String("query.target-url", "", "PromQL HTTP API backend URL.")
	queryHeaders        headerFlags
	queryAuthScopes     stringListFlag
	queryDropLabels     stringListFlag
	queryAnnounceLabels stringListFlag

	queryAuthCredentialsFile    = flag.String("query.auth.credentials-file", "", "Google auth credentials file for backend requests.")
	queryExternalLabels         = flag.Bool("query.external-labels", false, "Read and apply global.external_labels from the backend /api/v1/status/config endpoint, matching Thanos sidecar behavior.")
	queryExternalLabelsURL      = flag.String("query.external-labels-url", "", "Prometheus-compatible base URL for reading external labels. Defaults to query.target-url when query.external-labels is enabled.")
	queryExternalLabelsInterval = flag.Duration("query.external-labels.interval", 30*time.Second,
		"How often to refresh external labels when query.external-labels is enabled.")
	queryExternalLabelsTimeout = flag.Duration("query.external-labels.timeout", 5*time.Second,
		"Timeout for each external labels fetch when query.external-labels is enabled.")
	queryAnnounceLabelRefresh = flag.Duration("query.announce-label-refresh", time.Minute,
		"How often to refresh StoreAPI Info label sets from backend label values when query.announce-label is set.")
	queryAnnounceLabelTimeout = flag.Duration("query.announce-label-timeout", 5*time.Second,
		"Timeout for each announced label values fetch when query.announce-label is set.")
	querySeriesStep = flag.Duration("query.series-step", time.Minute,
		"Fallback step for StoreAPI series queries when Thanos does not send a query step hint.")
	queryMaxPointsPerSeries = flag.Int("query.max-points-per-series", 11000,
		"Maximum backend query_range points per series for StoreAPI series requests. The connector increases the backend step for long ranges when needed. Set 0 to disable connector-side clamping; backend limits still apply.")
	connectorAddress = flag.String("connector-address", ":8081",
		"Address on which to expose the query grpc server.")
	grpcServerTLSCertFile = flag.String("grpc-server-tls-cert", "",
		"TLS certificate file for the gRPC server. Requires grpc-server-tls-key when set.")
	grpcServerTLSKeyFile = flag.String("grpc-server-tls-key", "",
		"TLS private key file for the gRPC server. Requires grpc-server-tls-cert when set.")
	grpcServerTLSClientCAFile = flag.String("grpc-server-tls-client-ca", "",
		"Client CA certificate file for mTLS on the gRPC server. When set, client certificates are required and verified.")
	metricsAddress = flag.String("metrics-address", ":9090",
		"Address on which to expose metrics")
)

func init() {
	flag.Var(&queryHeaders, "query.header", "Static header to add to backend requests, in Name=Value or Name: Value format. May be repeated.")
	flag.Var(&queryAuthScopes, "query.auth.scope", "Google auth OAuth scope for backend requests. May be repeated or comma-separated.")
	flag.Var(&queryDropLabels, "query.drop-label", "Label to remove from query and StoreAPI responses. May be repeated or comma-separated.")
	flag.Var(&queryAnnounceLabels, "query.announce-label", "Backend label whose values should be advertised as StoreAPI Info label sets. May be repeated or comma-separated.")
}

type queryBackendAPI interface {
	Config(ctx context.Context) (v1.ConfigResult, error)
	LabelNames(ctx context.Context, matches []string, startTime, endTime time.Time, opts ...v1.Option) ([]string, v1.Warnings, error)
	LabelValues(ctx context.Context, label string, matches []string, startTime, endTime time.Time, opts ...v1.Option) (model.LabelValues, v1.Warnings, error)
	Query(ctx context.Context, query string, ts time.Time, opts ...v1.Option) (model.Value, v1.Warnings, error)
	QueryRange(ctx context.Context, query string, r v1.Range, opts ...v1.Option) (model.Value, v1.Warnings, error)
	Series(ctx context.Context, matches []string, startTime, endTime time.Time, opts ...v1.Option) ([]model.LabelSet, v1.Warnings, error)
}

type queryServer struct {
	queryBackendClient queryBackendAPI
	dropLabels         labelDropSet
	externalLabels     func() labels.Labels
}

func newQueryServer(queryBackendClient queryBackendAPI, dropLabels []string, externalLabels func() labels.Labels) *queryServer {
	if externalLabels == nil {
		externalLabels = labels.EmptyLabels
	}
	return &queryServer{
		queryBackendClient: queryBackendClient,
		dropLabels:         newLabelDropSet(dropLabels),
		externalLabels:     externalLabels,
	}
}

func (qs *queryServer) Series(request *storepb.SeriesRequest, server storepb.Store_SeriesServer) error {
	externalLabels := qs.externalLabels()
	match, matchers, err := matchesExternalLabels(request.Matchers, externalLabels)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if !match {
		return nil
	}
	if len(matchers) == 0 {
		return status.Error(codes.InvalidArgument, "no matchers specified (excluding external labels)")
	}
	selector := querySelectorFromPromMatchers(matchers)

	start := timeFromMillis(request.MinTime)
	end := timeFromMillis(request.MaxTime)
	if end.Before(start) {
		return status.Error(codes.InvalidArgument, "max_time must be greater than or equal to min_time")
	}

	if request.SkipChunks {
		return qs.sendSeriesMetadata(request, server, selector, start, end)
	}

	interval := v1.Range{
		Start: start,
		End:   end,
		Step:  seriesStepForRange(request, *querySeriesStep, start, end, *queryMaxPointsPerSeries),
	}
	values, warnings, err := qs.queryBackendClient.QueryRange(server.Context(), selector, interval)
	if err != nil {
		return status.Error(codes.Aborted, err.Error())
	}
	if err := sendStoreWarnings(server, warnings); err != nil {
		return err
	}

	matrix, ok := values.(model.Matrix)
	if !ok {
		return status.Errorf(codes.Internal, "backend returned %T for series selector %q, want model.Matrix", values, selector)
	}

	var sent int64
	for _, result := range matrix {
		chunks, err := chunksFromModelSamples(result.Values)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		if err := server.Send(storepb.NewSeriesResponse(&storepb.Series{
			Labels: qs.dropLabels.zLabelsFromMetric(result.Metric, externalLabels, request.WithoutReplicaLabels),
			Chunks: chunks,
		})); err != nil {
			return err
		}

		sent++
		if request.Limit > 0 && sent >= request.Limit {
			return nil
		}
	}
	return nil
}

func (qs *queryServer) LabelNames(ctx context.Context, request *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	externalLabels := qs.externalLabels()
	match, promMatchers, err := matchesExternalLabels(request.Matchers, externalLabels)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !match {
		return &storepb.LabelNamesResponse{}, nil
	}
	matches := labelAPISelectorsFromPromMatchers(promMatchers)
	req, warnings, err := qs.queryBackendClient.LabelNames(ctx, matches, timeFromMillis(request.Start), timeFromMillis(request.End))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &storepb.LabelNamesResponse{
		Names:    qs.dropLabels.labelNames(req, externalLabels, request.WithoutReplicaLabels),
		Warnings: warnings,
		Hints:    nil,
	}, nil
}

func (qs *queryServer) LabelValues(ctx context.Context, request *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	if request.Label == "" {
		return nil, status.Error(codes.InvalidArgument, "label name parameter cannot be empty")
	}
	if qs.dropLabels.has(request.Label) || containsString(request.WithoutReplicaLabels, request.Label) {
		return &storepb.LabelValuesResponse{}, nil
	}

	externalLabels := qs.externalLabels()
	match, promMatchers, err := matchesExternalLabels(request.Matchers, externalLabels)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !match {
		return &storepb.LabelValuesResponse{}, nil
	}
	matches := labelAPISelectorsFromPromMatchers(promMatchers)

	if value := externalLabels.Get(request.Label); value != "" {
		if len(promMatchers) == 0 {
			return &storepb.LabelValuesResponse{Values: []string{value}}, nil
		}
		labelSets, warnings, err := qs.queryBackendClient.Series(ctx, matches, timeFromMillis(request.Start), timeFromMillis(request.End))
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if len(labelSets) == 0 {
			return &storepb.LabelValuesResponse{Warnings: warnings}, nil
		}
		return &storepb.LabelValuesResponse{Values: []string{value}, Warnings: warnings}, nil
	}

	req, warnings, err := qs.queryBackendClient.LabelValues(ctx, request.Label, matches, timeFromMillis(request.Start), timeFromMillis(request.End))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	values := make([]string, 0, len(req))
	for _, value := range req {
		values = append(values, string(value))
	}
	return &storepb.LabelValuesResponse{
		Values:   values,
		Warnings: warnings,
		Hints:    nil,
	}, nil
}

func (qs *queryServer) Query(req *querypb.QueryRequest, srv querypb.Query_QueryServer) error {
	ts := time.Unix(req.TimeSeconds, 0)
	timeout := time.Duration(req.TimeoutSeconds) * time.Second
	values, warnings, err := qs.queryBackendClient.Query(srv.Context(), req.Query, ts, v1.WithTimeout(timeout))
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
			series := &prompb.TimeSeries{
				Samples: []prompb.Sample{{Value: float64(result.Value), Timestamp: int64(result.Timestamp)}},
				Labels:  qs.dropLabels.zLabelsFromMetric(result.Metric, qs.externalLabels(), nil),
			}
			if err := srv.Send(querypb.NewQueryResponse(series)); err != nil {
				return err
			}
		}
	case *model.Scalar:
		series := &prompb.TimeSeries{Samples: []prompb.Sample{{Value: float64(results.Value), Timestamp: int64(results.Timestamp)}}}
		return srv.Send(querypb.NewQueryResponse(series))
	}
	return nil
}

func (qs *queryServer) QueryRange(req *querypb.QueryRangeRequest, srv querypb.Query_QueryRangeServer) error {
	timeout := time.Duration(req.TimeoutSeconds) * time.Second
	interval := v1.Range{
		Start: time.Unix(req.StartTimeSeconds, 0),
		End:   time.Unix(req.EndTimeSeconds, 0),
		Step:  time.Duration(req.IntervalSeconds) * time.Second}
	values, warnings, err := qs.queryBackendClient.QueryRange(srv.Context(), req.Query, interval, v1.WithTimeout(timeout))
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
			series := &prompb.TimeSeries{
				Samples: samplesFromModel(result.Values),
				Labels:  qs.dropLabels.zLabelsFromMetric(result.Metric, qs.externalLabels(), nil),
			}
			if err := srv.Send(querypb.NewQueryRangeResponse(series)); err != nil {
				return err
			}
		}
	case model.Vector:
		for _, result := range results {
			series := &prompb.TimeSeries{
				Samples: []prompb.Sample{{Value: float64(result.Value), Timestamp: int64(result.Timestamp)}},
				Labels:  qs.dropLabels.zLabelsFromMetric(result.Metric, qs.externalLabels(), nil),
			}
			if err := srv.Send(querypb.NewQueryRangeResponse(series)); err != nil {
				return err
			}
		}
	case *model.Scalar:
		series := &prompb.TimeSeries{Samples: []prompb.Sample{{Value: float64(results.Value), Timestamp: int64(results.Timestamp)}}}
		return srv.Send(querypb.NewQueryRangeResponse(series))
	}
	return nil
}

func (qs *queryServer) sendSeriesMetadata(request *storepb.SeriesRequest, server storepb.Store_SeriesServer, selector string, start, end time.Time) error {
	externalLabels := qs.externalLabels()
	labelSets, warnings, err := qs.queryBackendClient.Series(server.Context(), []string{selector}, start, end)
	if err != nil {
		return status.Error(codes.Aborted, err.Error())
	}
	if err := sendStoreWarnings(server, warnings); err != nil {
		return err
	}

	var sent int64
	for _, labelSet := range labelSets {
		if err := server.Send(storepb.NewSeriesResponse(&storepb.Series{
			Labels: qs.dropLabels.zLabelsFromLabelSet(labelSet, externalLabels, request.WithoutReplicaLabels),
		})); err != nil {
			return err
		}

		sent++
		if request.Limit > 0 && sent >= request.Limit {
			return nil
		}
	}
	return nil
}

// samplesFromModel converts model.SamplePair to prompb.Sample.
func samplesFromModel(samples []model.SamplePair) []prompb.Sample {
	result := make([]prompb.Sample, 0, len(samples))
	for _, s := range samples {
		result = append(result, prompb.Sample{
			Value:     float64(s.Value),
			Timestamp: int64(s.Timestamp),
		})
	}
	return result
}

type labelDropSet map[string]struct{}

func newLabelDropSet(labels []string) labelDropSet {
	if len(labels) == 0 {
		return nil
	}

	result := make(labelDropSet, len(labels))
	for _, label := range labels {
		if label == "" {
			continue
		}
		result[label] = struct{}{}
	}
	return result
}

func (s labelDropSet) has(name string) bool {
	_, ok := s[name]
	return ok
}

func (s labelDropSet) filterNames(names []string) []string {
	if len(s) == 0 {
		return names
	}

	filtered := make([]string, 0, len(names))
	for _, name := range names {
		if !s.has(name) {
			filtered = append(filtered, name)
		}
	}
	return filtered
}

func (s labelDropSet) labelNames(names []string, externalLabels labels.Labels, withoutLabels []string) []string {
	remove := s.with(withoutLabels)
	nameSet := make(map[string]struct{}, len(names)+externalLabels.Len())
	for _, name := range names {
		if !remove.has(name) {
			nameSet[name] = struct{}{}
		}
	}
	if len(names) > 0 {
		externalLabels.Range(func(label labels.Label) {
			if !remove.has(label.Name) {
				nameSet[label.Name] = struct{}{}
			}
		})
	}

	result := make([]string, 0, len(nameSet))
	for name := range nameSet {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

func (s labelDropSet) with(names []string) labelDropSet {
	if len(names) == 0 {
		return s
	}
	result := make(labelDropSet, len(s)+len(names))
	for name := range s {
		result[name] = struct{}{}
	}
	for _, name := range names {
		if name != "" {
			result[name] = struct{}{}
		}
	}
	return result
}

// zLabelsFromMetric converts model.Metric to labelpb.ZLabel.
func (s labelDropSet) zLabelsFromMetric(metric model.Metric, externalLabels labels.Labels, withoutLabels []string) []labelpb.ZLabel {
	labelSet := make(model.LabelSet, len(metric))
	for name, value := range metric {
		labelSet[name] = value
	}
	return s.zLabelsFromLabelSet(labelSet, externalLabels, withoutLabels)
}

func (s labelDropSet) zLabelsFromLabelSet(labelSet model.LabelSet, externalLabels labels.Labels, withoutLabels []string) []labelpb.ZLabel {
	remove := s.with(withoutLabels)
	values := make(map[string]string, len(labelSet))
	for name, value := range labelSet {
		if remove.has(string(name)) {
			continue
		}
		values[string(name)] = string(value)
	}
	merged := labelpb.ExtendSortedLabels(labels.FromMap(values), externalLabels)
	if len(remove) == 0 {
		return labelpb.ZLabelsFromPromLabels(merged)
	}

	filtered := make(map[string]string, merged.Len())
	merged.Range(func(label labels.Label) {
		if !remove.has(label.Name) {
			filtered[label.Name] = label.Value
		}
	})
	return labelpb.ZLabelsFromPromLabels(labels.FromMap(filtered))
}

func labelAPISelectorsFromMatchers(matchers []storepb.LabelMatcher) ([]string, error) {
	if len(matchers) == 0 {
		return nil, nil
	}
	selector, err := querySelectorFromMatchers(matchers)
	if err != nil {
		return nil, err
	}
	return []string{selector}, nil
}

func labelAPISelectorsFromPromMatchers(matchers []*labels.Matcher) []string {
	if len(matchers) == 0 {
		return nil
	}
	return []string{querySelectorFromPromMatchers(matchers)}
}

func querySelectorFromMatchers(matchers []storepb.LabelMatcher) (string, error) {
	if len(matchers) == 0 {
		return `{__name__=~".+"}`, nil
	}
	promMatchers, err := storepb.MatchersToPromMatchers(matchers...)
	if err != nil {
		return "", err
	}
	return storepb.PromMatchersToString(promMatchers...), nil
}

func querySelectorFromPromMatchers(matchers []*labels.Matcher) string {
	if len(matchers) == 0 {
		return `{__name__=~".+"}`
	}
	return storepb.PromMatchersToString(matchers...)
}

// matchesExternalLabels follows Thanos sidecar StoreAPI semantics: external
// label matchers select this store and are removed before querying the backend.
func matchesExternalLabels(matchers []storepb.LabelMatcher, externalLabels labels.Labels) (bool, []*labels.Matcher, error) {
	promMatchers, err := storepb.MatchersToPromMatchers(matchers...)
	if err != nil {
		return false, nil, err
	}
	if externalLabels.IsEmpty() {
		return true, promMatchers, nil
	}

	filtered := make([]*labels.Matcher, 0, len(promMatchers))
	for _, matcher := range promMatchers {
		externalValue := externalLabels.Get(matcher.Name)
		if externalValue == "" {
			filtered = append(filtered, matcher)
			continue
		}
		if !matcher.Matches(externalValue) {
			return false, nil, nil
		}
	}
	return true, filtered, nil
}

func seriesStep(request *storepb.SeriesRequest, fallback time.Duration) time.Duration {
	if request.QueryHints != nil && request.QueryHints.StepMillis > 0 {
		return time.Duration(request.QueryHints.StepMillis) * time.Millisecond
	}
	if request.Step > 0 {
		return time.Duration(request.Step) * time.Millisecond
	}
	if fallback > 0 {
		return fallback
	}
	return time.Minute
}

func seriesStepForRange(request *storepb.SeriesRequest, fallback time.Duration, start, end time.Time, maxPoints int) time.Duration {
	step := seriesStep(request, fallback)
	if maxPoints <= 0 || !end.After(start) {
		return step
	}

	minStep := minStepForMaxPoints(end.Sub(start), maxPoints)
	if minStep > step {
		return minStep
	}
	return step
}

func minStepForMaxPoints(rng time.Duration, maxPoints int) time.Duration {
	if maxPoints <= 0 || rng <= 0 {
		return 0
	}
	if maxPoints == 1 {
		return rng
	}

	divisor := time.Duration(maxPoints - 1)
	step := rng / divisor
	if rng%divisor != 0 {
		step++
	}
	return step
}

func timeFromMillis(ms int64) time.Time {
	return time.Unix(ms/1000, (ms%1000)*int64(time.Millisecond)).UTC()
}

func chunksFromModelSamples(samples []model.SamplePair) ([]storepb.AggrChunk, error) {
	if len(samples) == 0 {
		return nil, nil
	}

	const samplesPerChunk = 120
	chunks := make([]storepb.AggrChunk, 0, (len(samples)+samplesPerChunk-1)/samplesPerChunk)
	for i := 0; i < len(samples); i += samplesPerChunk {
		end := i + samplesPerChunk
		if end > len(samples) {
			end = len(samples)
		}
		chunk, err := chunkFromModelSamples(samples[i:end])
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

func chunkFromModelSamples(samples []model.SamplePair) (storepb.AggrChunk, error) {
	xorChunk := chunkenc.NewXORChunk()
	appender, err := xorChunk.Appender()
	if err != nil {
		return storepb.AggrChunk{}, fmt.Errorf("creating XOR chunk appender: %w", err)
	}

	minTime := int64(samples[0].Timestamp)
	maxTime := minTime
	for _, sample := range samples {
		timestamp := int64(sample.Timestamp)
		appender.Append(timestamp, float64(sample.Value))
		if timestamp < minTime {
			minTime = timestamp
		}
		if timestamp > maxTime {
			maxTime = timestamp
		}
	}
	xorChunk.Compact()

	return storepb.AggrChunk{
		MinTime: minTime,
		MaxTime: maxTime,
		Raw: &storepb.Chunk{
			Type: storepb.Chunk_XOR,
			Data: xorChunk.Bytes(),
		},
	}, nil
}

func sendStoreWarnings(server storepb.Store_SeriesServer, warnings v1.Warnings) error {
	for _, warning := range warnings {
		if err := server.Send(storepb.NewWarnSeriesResponse(errors.New(warning))); err != nil {
			return err
		}
	}
	return nil
}

type externalLabelsStore struct {
	mtx    sync.RWMutex
	labels labels.Labels
}

func newExternalLabelsStore() *externalLabelsStore {
	return &externalLabelsStore{labels: labels.EmptyLabels()}
}

func (s *externalLabelsStore) Labels() labels.Labels {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.labels.Copy()
}

func (s *externalLabelsStore) UpdateFromBackend(ctx context.Context, client queryBackendAPI) error {
	config, err := client.Config(ctx)
	if err != nil {
		return err
	}
	externalLabels, err := externalLabelsFromConfigYAML(config.YAML)
	if err != nil {
		return err
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.labels = externalLabels
	return nil
}

type announcedLabelSetsStore struct {
	mtx       sync.RWMutex
	labelSets []labels.Labels
}

func newAnnouncedLabelSetsStore() *announcedLabelSetsStore {
	return &announcedLabelSetsStore{}
}

func (s *announcedLabelSetsStore) LabelSets() []labels.Labels {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	result := make([]labels.Labels, 0, len(s.labelSets))
	for _, labelSet := range s.labelSets {
		result = append(result, labelSet.Copy())
	}
	return result
}

func (s *announcedLabelSetsStore) UpdateFromBackend(ctx context.Context, client queryBackendAPI, labelNames []string) error {
	labelSets, err := announcedLabelSetsFromBackend(ctx, client, labelNames)
	if err != nil {
		return err
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.labelSets = labelSets
	return nil
}

func announcedLabelSetsFromBackend(ctx context.Context, client queryBackendAPI, labelNames []string) ([]labels.Labels, error) {
	labelSets := make([]labels.Labels, 0)
	seen := make(map[string]struct{})

	for _, labelName := range labelNames {
		if !model.LabelName(labelName).IsValid() {
			return nil, fmt.Errorf("invalid announced label name %q", labelName)
		}

		values, _, err := client.LabelValues(ctx, labelName, nil, time.Time{}, time.Time{})
		if err != nil {
			return nil, fmt.Errorf("read values for announced label %q: %w", labelName, err)
		}
		for _, labelSet := range announcedLabelSetsFromValues(labelName, values) {
			key := labelSet.String()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			labelSets = append(labelSets, labelSet)
		}
	}

	sort.Slice(labelSets, func(i, j int) bool {
		return labelSets[i].String() < labelSets[j].String()
	})
	return labelSets, nil
}

func announcedLabelSetsFromValues(labelName string, values model.LabelValues) []labels.Labels {
	uniqueValues := make(map[string]struct{}, len(values))
	for _, value := range values {
		value := string(value)
		if value == "" {
			continue
		}
		uniqueValues[value] = struct{}{}
	}

	sortedValues := make([]string, 0, len(uniqueValues))
	for value := range uniqueValues {
		sortedValues = append(sortedValues, value)
	}
	sort.Strings(sortedValues)

	labelSets := make([]labels.Labels, 0, len(sortedValues))
	for _, value := range sortedValues {
		labelSets = append(labelSets, labels.FromStrings(labelName, value))
	}
	return labelSets
}

func externalLabelsFromConfigYAML(configYAML string) (labels.Labels, error) {
	var cfg struct {
		GlobalConfig struct {
			ExternalLabels map[string]string `yaml:"external_labels"`
		} `yaml:"global"`
	}
	if err := yaml.Unmarshal([]byte(configYAML), &cfg); err != nil {
		return labels.EmptyLabels(), fmt.Errorf("parse Prometheus config: %w", err)
	}
	return labels.FromMap(cfg.GlobalConfig.ExternalLabels), nil
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

type infoServer struct {
	queryBackend       string
	externalLabels     func() labels.Labels
	announcedLabelSets func() []labels.Labels
}

func (info *infoServer) Info(ctx context.Context, in *infopb.InfoRequest) (*infopb.InfoResponse, error) {
	labelSets := labelpb.ZLabelSetsFromPromLabels(labels.FromStrings("query-backend", info.queryBackend))
	tsdbInfos := []infopb.TSDBInfo{{MinTime: math.MinInt64, MaxTime: math.MaxInt64}}
	usingFallbackLabelSet := true

	if info.announcedLabelSets != nil {
		announcedLabelSets := info.announcedLabelSets()
		if len(announcedLabelSets) > 0 {
			labelSets = labelpb.ZLabelSetsFromPromLabels(announcedLabelSets...)
			tsdbInfos = tsdbInfosFromLabelSets(labelSets)
			usingFallbackLabelSet = false
		}
	}

	if usingFallbackLabelSet && info.externalLabels != nil {
		externalLabels := info.externalLabels()
		if externalLabels.Len() > 0 {
			labelSets = labelpb.ZLabelSetsFromPromLabels(externalLabels)
			tsdbInfos = tsdbInfosFromLabelSets(labelSets)
		}
	}

	return &infopb.InfoResponse{
		ComponentType: component.Query.String(),
		LabelSets:     labelSets,
		Store: &infopb.StoreInfo{
			MinTime:                      math.MinInt64,
			MaxTime:                      math.MaxInt64,
			SupportsWithoutReplicaLabels: true,
			TsdbInfos:                    tsdbInfos,
		},
		Query: &infopb.QueryAPIInfo{},
	}, nil
}

func tsdbInfosFromLabelSets(labelSets []labelpb.ZLabelSet) []infopb.TSDBInfo {
	result := make([]infopb.TSDBInfo, 0, len(labelSets))
	for _, labelSet := range labelSets {
		result = append(result, infopb.TSDBInfo{
			Labels:  labelSet,
			MinTime: math.MinInt64,
			MaxTime: math.MaxInt64,
		})
	}
	return result
}

type headerRoundTripper struct {
	base    http.RoundTripper
	headers http.Header
}

func newHeaderRoundTripper(base http.RoundTripper, headers map[string]string) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	if len(headers) == 0 {
		return base
	}

	rt := &headerRoundTripper{
		base:    base,
		headers: make(http.Header, len(headers)),
	}
	for name, value := range headers {
		rt.headers.Set(name, value)
	}
	return rt
}

func (rt *headerRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	outgoing := req.Clone(req.Context())
	for name, values := range rt.headers {
		outgoing.Header.Del(name)
		for _, value := range values {
			outgoing.Header.Add(name, value)
		}
	}

	return rt.base.RoundTrip(outgoing)
}

// newQueryBackendRoundTripper creates the HTTP transport for backend requests.
func newQueryBackendRoundTripper(queryConfig queryBackendConfig) (http.RoundTripper, error) {
	transport := http.RoundTripper(http.DefaultTransport)
	if queryAuthEnabled(queryConfig.Auth) {
		opts := make([]option.ClientOption, 0, 2)
		if len(queryConfig.Auth.Scopes) > 0 {
			opts = append(opts, option.WithScopes(queryConfig.Auth.Scopes...))
		}
		if queryConfig.Auth.CredentialsFile != "" {
			opts = append(opts, option.WithCredentialsFile(queryConfig.Auth.CredentialsFile))
		}

		var err error
		transport, err = apihttp.NewTransport(context.Background(), transport, opts...)
		if err != nil {
			return nil, fmt.Errorf("error creating proxy HTTP transport: %s", err)
		}
	}

	return newHeaderRoundTripper(transport, queryConfig.Headers), nil
}

func queryAuthEnabled(auth queryBackendAuthConfig) bool {
	return auth.CredentialsFile != "" || len(auth.Scopes) > 0
}

// createQueryBackendClient creates a connection with the backend that the queries will be forwarded to.
func createQueryBackendClient(queryConfig queryBackendConfig) (queryBackendAPI, error) {
	transport, err := newQueryBackendRoundTripper(queryConfig)
	if err != nil {
		return nil, err
	}
	client, err := api.NewClient(api.Config{
		Address:      queryConfig.QueryTargetURL,
		RoundTripper: transport,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating client: %s", err)
	}
	return v1.NewAPI(client), nil
}

type grpcServerTLSConfig struct {
	CertFile     string
	KeyFile      string
	ClientCAFile string
}

func newGRPCServerOptions(tlsConfig grpcServerTLSConfig) ([]grpc.ServerOption, bool, error) {
	tlsEnabled := tlsConfig.CertFile != "" || tlsConfig.KeyFile != "" || tlsConfig.ClientCAFile != ""
	if !tlsEnabled {
		return nil, false, nil
	}
	if tlsConfig.CertFile == "" || tlsConfig.KeyFile == "" {
		return nil, false, fmt.Errorf("grpc-server-tls-cert and grpc-server-tls-key must both be set to enable gRPC TLS")
	}

	cert, err := tls.LoadX509KeyPair(tlsConfig.CertFile, tlsConfig.KeyFile)
	if err != nil {
		return nil, false, fmt.Errorf("load gRPC server certificate: %w", err)
	}

	serverTLSConfig := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"h2"},
		Certificates: []tls.Certificate{cert},
	}
	if tlsConfig.ClientCAFile != "" {
		clientCAPEM, err := os.ReadFile(tlsConfig.ClientCAFile)
		if err != nil {
			return nil, false, fmt.Errorf("read gRPC client CA file: %w", err)
		}
		clientCAs := x509.NewCertPool()
		if !clientCAs.AppendCertsFromPEM(clientCAPEM) {
			return nil, false, fmt.Errorf("parse gRPC client CA file %q: no certificates found", tlsConfig.ClientCAFile)
		}
		serverTLSConfig.ClientCAs = clientCAs
		serverTLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return []grpc.ServerOption{grpc.Creds(credentials.NewTLS(serverTLSConfig))}, true, nil
}

func main() {
	flag.Parse()
	logger := log.NewJSONLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	logger = log.With(logger, "caller", log.DefaultCaller)

	var err error
	// Configuration Loading.
	queryConfig, err := newQueryBackendConfig(*queryTargetURL, queryHeaders.values(), *queryAuthCredentialsFile, queryAuthScopes.values())
	if err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}
	// Query client setup.
	queryBackendClient, err := createQueryBackendClient(*queryConfig)
	if err != nil {
		level.Error(logger).Log("msg", "Error creating client", "err", err)
		os.Exit(1)
	}
	externalLabels := newExternalLabelsStore()
	announcedLabelSets := newAnnouncedLabelSetsStore()
	announcedLabelNames := queryAnnounceLabels.values()

	externalLabelsClient := queryBackendClient
	if *queryExternalLabelsURL != "" {
		externalLabelsConfig := *queryConfig
		externalLabelsConfig.QueryTargetURL = *queryExternalLabelsURL
		externalLabelsClient, err = createQueryBackendClient(externalLabelsConfig)
		if err != nil {
			level.Error(logger).Log("msg", "Error creating external labels client", "err", err)
			os.Exit(1)
		}
	}
	if *queryExternalLabels {
		ctx, cancel := context.WithTimeout(context.Background(), *queryExternalLabelsTimeout)
		err = externalLabels.UpdateFromBackend(ctx, externalLabelsClient)
		cancel()
		if err != nil {
			level.Error(logger).Log("msg", "Error loading initial external labels", "err", err)
			os.Exit(1)
		}
		if externalLabels.Labels().Len() == 0 {
			level.Error(logger).Log("msg", "no external labels configured on backend")
			os.Exit(1)
		}
		level.Info(logger).Log("msg", "successfully loaded backend external labels", "external_labels", externalLabels.Labels().String())
	}
	if len(announcedLabelNames) > 0 {
		if *queryAnnounceLabelRefresh <= 0 {
			level.Error(logger).Log("msg", "query.announce-label-refresh must be greater than zero")
			os.Exit(1)
		}
		if *queryAnnounceLabelTimeout <= 0 {
			level.Error(logger).Log("msg", "query.announce-label-timeout must be greater than zero")
			os.Exit(1)
		}

		ctx, cancel := context.WithTimeout(context.Background(), *queryAnnounceLabelTimeout)
		err = announcedLabelSets.UpdateFromBackend(ctx, queryBackendClient, announcedLabelNames)
		cancel()
		if err != nil {
			level.Error(logger).Log("msg", "Error loading initial announced label sets", "err", err)
			os.Exit(1)
		}
		if len(announcedLabelSets.LabelSets()) == 0 {
			level.Error(logger).Log("msg", "no announced label values found on backend", "labels", fmt.Sprint(announcedLabelNames))
			os.Exit(1)
		}
		level.Info(logger).Log("msg", "successfully loaded backend announced label sets", "labels", fmt.Sprint(announcedLabelNames), "label_sets", fmt.Sprint(announcedLabelSets.LabelSets()))
	}

	var g run.Group
	{
		term := make(chan os.Signal, 1)
		cancel := make(chan struct{})
		signal.Notify(term, os.Interrupt, syscall.SIGTERM)

		g.Add(
			func() error {
				select {
				case <-term:
					level.Info(logger).Log("msg", "received SIGTERM, exiting gracefully...")
				case <-cancel:
				}
				return nil
			},
			func(err error) {
				close(cancel)
			},
		)
	}
	if *queryExternalLabels {
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			ticker := time.NewTicker(*queryExternalLabelsInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					iterCtx, iterCancel := context.WithTimeout(context.Background(), *queryExternalLabelsTimeout)
					err := externalLabels.UpdateFromBackend(iterCtx, externalLabelsClient)
					iterCancel()
					if err != nil {
						level.Warn(logger).Log("msg", "updating external labels failed", "err", err)
						continue
					}
					level.Info(logger).Log("msg", "updated backend external labels", "external_labels", externalLabels.Labels().String())
				case <-ctx.Done():
					return nil
				}
			}
		}, func(err error) {
			cancel()
		})
	}
	if len(announcedLabelNames) > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			ticker := time.NewTicker(*queryAnnounceLabelRefresh)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					iterCtx, iterCancel := context.WithTimeout(context.Background(), *queryAnnounceLabelTimeout)
					err := announcedLabelSets.UpdateFromBackend(iterCtx, queryBackendClient, announcedLabelNames)
					iterCancel()
					if err != nil {
						level.Warn(logger).Log("msg", "updating announced label sets failed", "err", err)
						continue
					}
					level.Info(logger).Log("msg", "updated backend announced label sets", "labels", fmt.Sprint(announcedLabelNames), "label_sets", fmt.Sprint(announcedLabelSets.LabelSets()))
				case <-ctx.Done():
					return nil
				}
			}
		}, func(err error) {
			cancel()
		})
	}
	{
		//grpc server.
		listener, err := net.Listen("tcp", *connectorAddress)
		if err != nil {
			panic(err)
		}
		serverOptions, tlsEnabled, err := newGRPCServerOptions(grpcServerTLSConfig{
			CertFile:     *grpcServerTLSCertFile,
			KeyFile:      *grpcServerTLSKeyFile,
			ClientCAFile: *grpcServerTLSClientCAFile,
		})
		if err != nil {
			level.Error(logger).Log("msg", "Error creating grpc server TLS config", "err", err)
			os.Exit(1)
		}
		server := grpc.NewServer(serverOptions...)
		queryServer := newQueryServer(queryBackendClient, queryDropLabels.values(), externalLabels.Labels)
		storepb.RegisterStoreServer(server, queryServer)
		querypb.RegisterQueryServer(server, queryServer)
		infopb.RegisterInfoServer(server, &infoServer{
			queryBackend:       queryConfig.QueryTargetURL,
			externalLabels:     externalLabels.Labels,
			announcedLabelSets: announcedLabelSets.LabelSets,
		})
		g.Add(func() error {
			level.Info(logger).Log("msg", "Starting grpc server for query endpoint", "listen", *connectorAddress, "tls", tlsEnabled, "mtls", *grpcServerTLSClientCAFile != "")
			return server.Serve(listener)
		}, func(err error) {
			server.GracefulStop()
		})
	}
	{
		// http server.
		ctx, cancel := context.WithCancel(context.Background())
		server := &http.Server{Addr: *metricsAddress}
		http.Handle("/metrics", promhttp.Handler())

		http.HandleFunc("/-/healthy", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "OK")
		})
		http.HandleFunc("/-/ready", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "OK")
		})

		g.Add(func() error {
			level.Info(logger).Log("msg", "Starting web server for metrics", "listen", *metricsAddress)
			return server.ListenAndServe()
		}, func(err error) {
			server.Shutdown(ctx)
			cancel()
		})
	}

	if err := g.Run(); err != nil {
		level.Error(logger).Log("msg", "running reloader failed", "err", err)
		os.Exit(1)
	}
}

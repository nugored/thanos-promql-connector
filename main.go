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
	"strings"
	"sync"
	"syscall"
	"time"

	authcredentials "cloud.google.com/go/auth/credentials"
	authhttptransport "cloud.google.com/go/auth/httptransport"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/status"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/component"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"gopkg.in/yaml.v2"
)

var (
	queryTargetURL      = flag.String("query.target-url", "", "PromQL HTTP API backend URL.")
	queryHeaders        headerFlags
	queryParams         queryParamFlags
	queryAuthScopes     stringListFlag
	queryDropLabels     stringListFlag
	queryExternalLabel  labelFlags
	queryGCPProjects    stringListFlag
	queryAnnounceLabels stringListFlag

	queryAuthGoogle             = flag.Bool("query.auth.google", false, "Enable Google Application Default Credentials for backend requests. Uses credentials from GOOGLE_APPLICATION_CREDENTIALS, gcloud ADC, or the metadata server such as GKE Workload Identity.")
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
	queryAnnounceLabelLookback = flag.Duration("query.announce-label-lookback", 0,
		"Only read announced label values from this recent time range. Set 0 to read without start/end bounds.")
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
	grpcInfoAPIMode = flag.String("grpc-info-api-mode", string(infoAPIModeStore),
		"API support to advertise in the Info response. Valid values: store, query, both.")
	grpcInfoAdvertiseQueryAPI = flag.Bool("grpc-info-advertise-query-api", false,
		"Deprecated: advertise both StoreAPI and QueryAPI support in the Info response when grpc-info-api-mode is left as store.")
	metricsAddress = flag.String("metrics-address", ":9090",
		"Address on which to expose metrics")
)

const googleMonitoringReadScope = "https://www.googleapis.com/auth/monitoring.read"

func init() {
	flag.Var(&queryHeaders, "query.header", "Static header to add to backend requests, in Name=Value or Name: Value format. May be repeated.")
	flag.Var(&queryParams, "query.param", "Static query parameter to add to every backend Prometheus API request, in Name=Value format. May be repeated.")
	flag.Var(&queryAuthScopes, "query.auth.scope", "Google auth OAuth scope for backend requests. May be repeated or comma-separated.")
	flag.Var(&queryDropLabels, "query.drop-label", "Label to remove from query and StoreAPI responses. May be repeated or comma-separated.")
	flag.Var(&queryExternalLabel, "query.external-label", "Static external label to announce and attach to every response, in Name=Value format. May be repeated.")
	flag.Var(&queryGCPProjects, "query.gcp-project", "Google Cloud project ID to query through Managed Service for Prometheus. Derives query.target-url and prometheus=gcp-<project> external label. May be repeated or comma-separated.")
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
	backends   []queryBackendEndpoint
	dropLabels labelDropSet
}

type queryBackendEndpoint struct {
	name           string
	queryBackend   string
	client         queryBackendAPI
	externalLabels func() labels.Labels
}

type infoAPIMode string

const (
	infoAPIModeStore infoAPIMode = "store"
	infoAPIModeQuery infoAPIMode = "query"
	infoAPIModeBoth  infoAPIMode = "both"
)

func parseInfoAPIMode(value string, advertiseQueryAPI bool) (infoAPIMode, error) {
	mode := infoAPIMode(strings.ToLower(strings.TrimSpace(value)))
	if mode == "" {
		mode = infoAPIModeStore
	}
	switch mode {
	case infoAPIModeStore:
		if advertiseQueryAPI {
			return infoAPIModeBoth, nil
		}
		return infoAPIModeStore, nil
	case infoAPIModeQuery, infoAPIModeBoth:
		return mode, nil
	default:
		return "", fmt.Errorf("grpc-info-api-mode must be one of store, query, or both")
	}
}

func (mode infoAPIMode) effective() infoAPIMode {
	if mode == "" {
		return infoAPIModeStore
	}
	return mode
}

func (mode infoAPIMode) advertisesStoreAPI() bool {
	mode = mode.effective()
	return mode == infoAPIModeStore || mode == infoAPIModeBoth
}

func (mode infoAPIMode) advertisesQueryAPI() bool {
	mode = mode.effective()
	return mode == infoAPIModeQuery || mode == infoAPIModeBoth
}

func newQueryServer(queryBackendClient queryBackendAPI, dropLabels []string, externalLabels func() labels.Labels) *queryServer {
	return newQueryServerFromBackends([]queryBackendEndpoint{{
		name:           "backend",
		client:         queryBackendClient,
		externalLabels: externalLabels,
	}}, dropLabels)
}

func newQueryServerFromBackends(backends []queryBackendEndpoint, dropLabels []string) *queryServer {
	normalized := make([]queryBackendEndpoint, 0, len(backends))
	for _, backend := range backends {
		if backend.externalLabels == nil {
			backend.externalLabels = labels.EmptyLabels
		}
		normalized = append(normalized, backend)
	}
	if len(normalized) == 0 {
		normalized = append(normalized, queryBackendEndpoint{
			name:           "backend",
			externalLabels: labels.EmptyLabels,
		})
	}

	return &queryServer{
		backends:   normalized,
		dropLabels: newLabelDropSet(dropLabels),
	}
}

func staticExternalLabelsFunc(value labels.Labels) func() labels.Labels {
	value = value.Copy()
	return func() labels.Labels {
		return value.Copy()
	}
}

func (backend queryBackendEndpoint) labels() labels.Labels {
	if backend.externalLabels == nil {
		return labels.EmptyLabels()
	}
	return backend.externalLabels()
}

func (qs *queryServer) Series(request *storepb.SeriesRequest, server storepb.Store_SeriesServer) error {
	start := timeFromMillis(request.MinTime)
	end := timeFromMillis(request.MaxTime)
	if end.Before(start) {
		return status.Error(codes.InvalidArgument, "max_time must be greater than or equal to min_time")
	}

	matched := false
	var sent int64
	seriesSet := make([]storepb.Series, 0)
	for _, backend := range qs.backends {
		externalLabels := backend.labels()
		match, matchers, err := matchesExternalLabels(request.Matchers, externalLabels)
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
		selector := querySelectorFromPromMatchers(matchers)

		if request.SkipChunks {
			backendSeriesSet, warnings, err := qs.seriesMetadataFromBackend(server.Context(), backend, externalLabels, request, selector, start, end)
			if err != nil {
				return status.Error(codes.Aborted, err.Error())
			}
			if err := sendStoreWarnings(server, warnings); err != nil {
				return err
			}
			seriesSet = append(seriesSet, backendSeriesSet...)
			continue
		}

		interval := v1.Range{
			Start: start,
			End:   end,
			Step:  seriesStepForRange(request, *querySeriesStep, start, end, *queryMaxPointsPerSeries),
		}
		values, warnings, err := backend.client.QueryRange(server.Context(), selector, interval)
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

		for _, result := range matrix {
			if result == nil || len(result.Values) == 0 {
				continue
			}
			chunks, err := chunksFromModelSamples(result.Values)
			if err != nil {
				return status.Error(codes.Internal, err.Error())
			}
			seriesSet = append(seriesSet, storepb.Series{
				Labels: qs.dropLabels.zLabelsFromMetric(result.Metric, externalLabels, request.WithoutReplicaLabels),
				Chunks: chunks,
			})
		}
	}
	if !matched {
		return nil
	}
	sortStoreSeries(seriesSet)

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

func (qs *queryServer) seriesMetadataFromBackend(ctx context.Context, backend queryBackendEndpoint, externalLabels labels.Labels, request *storepb.SeriesRequest, selector string, start, end time.Time) ([]storepb.Series, v1.Warnings, error) {
	labelSets, warnings, err := backend.client.Series(ctx, []string{selector}, start, end)
	if err != nil {
		return nil, nil, err
	}

	seriesSet := make([]storepb.Series, 0, len(labelSets))
	for _, labelSet := range labelSets {
		seriesSet = append(seriesSet, storepb.Series{
			Labels: qs.dropLabels.zLabelsFromLabelSet(labelSet, externalLabels, request.WithoutReplicaLabels),
		})
	}
	return seriesSet, warnings, nil
}

func (qs *queryServer) LabelNames(ctx context.Context, request *storepb.LabelNamesRequest) (*storepb.LabelNamesResponse, error) {
	nameSet := make(map[string]struct{})
	var warnings v1.Warnings

	for _, backend := range qs.backends {
		externalLabels := backend.labels()
		match, promMatchers, err := matchesExternalLabels(request.Matchers, externalLabels)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if !match {
			continue
		}

		matches := labelAPISelectorsFromPromMatchers(promMatchers)
		names, backendWarnings, err := backend.client.LabelNames(ctx, matches, timeFromMillis(request.Start), timeFromMillis(request.End))
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		warnings = append(warnings, backendWarnings...)
		for _, name := range qs.dropLabels.labelNames(names, externalLabels, request.WithoutReplicaLabels) {
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

func (qs *queryServer) LabelValues(ctx context.Context, request *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	if request.Label == "" {
		return nil, status.Error(codes.InvalidArgument, "label name parameter cannot be empty")
	}
	if qs.dropLabels.has(request.Label) || containsString(request.WithoutReplicaLabels, request.Label) {
		return &storepb.LabelValuesResponse{}, nil
	}

	valueSet := make(map[string]struct{})
	var warnings v1.Warnings

	for _, backend := range qs.backends {
		externalLabels := backend.labels()
		match, promMatchers, err := matchesExternalLabels(request.Matchers, externalLabels)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if !match {
			continue
		}

		// Connector-owned labels are served directly. Every other label value
		// request goes to the backend with connector-owned matchers stripped.
		if value := externalLabels.Get(request.Label); value != "" {
			valueSet[value] = struct{}{}
			continue
		}

		matches := labelAPISelectorsFromPromMatchers(promMatchers)
		req, backendWarnings, err := backend.client.LabelValues(ctx, request.Label, matches, timeFromMillis(request.Start), timeFromMillis(request.End))
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
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

func (qs *queryServer) Query(req *querypb.QueryRequest, srv querypb.Query_QueryServer) error {
	ts := time.Unix(req.TimeSeconds, 0)
	timeout := time.Duration(req.TimeoutSeconds) * time.Second
	rawQuery, err := queryStringFromRequestPlan(req.Query, req.QueryPlan)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	for _, backend := range qs.backends {
		externalLabels := backend.labels()
		query, match, err := rewriteQueryForExternalLabels(rawQuery, externalLabels)
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		if !match {
			continue
		}

		values, warnings, err := backend.client.Query(srv.Context(), query, ts, v1.WithTimeout(timeout))
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
					Labels:  qs.dropLabels.zLabelsFromMetric(result.Metric, externalLabels, nil),
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

func (qs *queryServer) QueryRange(req *querypb.QueryRangeRequest, srv querypb.Query_QueryRangeServer) error {
	timeout := time.Duration(req.TimeoutSeconds) * time.Second
	interval := v1.Range{
		Start: time.Unix(req.StartTimeSeconds, 0),
		End:   time.Unix(req.EndTimeSeconds, 0),
		Step:  time.Duration(req.IntervalSeconds) * time.Second}
	rawQuery, err := queryStringFromRequestPlan(req.Query, req.QueryPlan)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	for _, backend := range qs.backends {
		externalLabels := backend.labels()
		query, match, err := rewriteQueryForExternalLabels(rawQuery, externalLabels)
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		if !match {
			continue
		}

		values, warnings, err := backend.client.QueryRange(srv.Context(), query, interval, v1.WithTimeout(timeout))
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
					Samples: samplesFromModel(result.Values),
					Labels:  qs.dropLabels.zLabelsFromMetric(result.Metric, externalLabels, nil),
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
					Labels:  qs.dropLabels.zLabelsFromMetric(result.Metric, externalLabels, nil),
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

func queryStringFromRequestPlan(query string, plan *querypb.QueryPlan) (string, error) {
	if plan == nil {
		return query, nil
	}

	jsonPlan := plan.GetJson()
	if len(jsonPlan) == 0 {
		return "", fmt.Errorf("query plan has no JSON payload")
	}

	node, err := logicalplan.Unmarshal(jsonPlan)
	if err != nil {
		return "", fmt.Errorf("decode query plan: %w", err)
	}
	if node == nil {
		return "", fmt.Errorf("query plan contains unsupported logical node")
	}

	node = mergeQueryPlanSelectorFilters(node)
	plannedQuery := strings.TrimSpace(node.String())
	if plannedQuery == "" {
		return "", fmt.Errorf("query plan rendered an empty query")
	}
	if _, err := parser.ParseExpr(plannedQuery); err != nil {
		return "", fmt.Errorf("query plan rendered invalid PromQL %q: %w", plannedQuery, err)
	}
	return plannedQuery, nil
}

func mergeQueryPlanSelectorFilters(node logicalplan.Node) logicalplan.Node {
	clone := node.Clone()
	logicalplan.Traverse(&clone, func(current *logicalplan.Node) {
		selector, ok := (*current).(*logicalplan.VectorSelector)
		if !ok || len(selector.Filters) == 0 {
			return
		}

		for _, filter := range selector.Filters {
			if filter == nil || containsMatcher(selector.LabelMatchers, filter) {
				continue
			}
			selector.LabelMatchers = append(selector.LabelMatchers, filter)
		}
		selector.Filters = nil
	})
	return clone
}

func containsMatcher(matchers []*labels.Matcher, matcher *labels.Matcher) bool {
	for _, current := range matchers {
		if current != nil && current.Type == matcher.Type && current.Name == matcher.Name && current.Value == matcher.Value {
			return true
		}
	}
	return false
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

// rewriteQueryForExternalLabels applies StoreAPI-style external label matching
// to raw PromQL QueryAPI requests.
func rewriteQueryForExternalLabels(query string, externalLabels labels.Labels) (string, bool, error) {
	if externalLabels.IsEmpty() || !queryMightContainExternalLabel(query, externalLabels) {
		return query, true, nil
	}

	expr, err := parser.ParseExpr(query)
	if err != nil {
		return "", false, err
	}

	matches := true
	changed := false
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		if !matches {
			return nil
		}
		vectorSelector, ok := node.(*parser.VectorSelector)
		if !ok {
			return nil
		}

		filtered := make([]*labels.Matcher, 0, len(vectorSelector.LabelMatchers))
		selectorChanged := false
		for _, matcher := range vectorSelector.LabelMatchers {
			externalValue := externalLabels.Get(matcher.Name)
			if externalValue == "" {
				filtered = append(filtered, matcher)
				continue
			}
			selectorChanged = true
			if !matcher.Matches(externalValue) {
				matches = false
				return nil
			}
		}
		if !selectorChanged {
			return nil
		}

		if vectorSelector.Name == "" && len(filtered) == 0 {
			filtered = append(filtered, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+"))
		}
		vectorSelector.LabelMatchers = filtered
		changed = true
		return nil
	})
	if !matches {
		return "", false, nil
	}
	if !changed {
		return query, true, nil
	}
	return expr.String(), true, nil
}

func queryMightContainExternalLabel(query string, externalLabels labels.Labels) bool {
	query = strings.NewReplacer(" ", "", "\t", "", "\n", "", "\r", "").Replace(query)
	mightContain := false
	externalLabels.Range(func(label labels.Label) {
		if mightContain {
			return
		}
		quotedName := `"` + label.Name + `"`
		mightContain = strings.Contains(query, label.Name+"=") ||
			strings.Contains(query, label.Name+"!") ||
			strings.Contains(query, quotedName+"=") ||
			strings.Contains(query, quotedName+"!")
	})
	return mightContain
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

func sortStoreSeries(seriesSet []storepb.Series) {
	for i := range seriesSet {
		sort.Slice(seriesSet[i].Chunks, func(a, b int) bool {
			return seriesSet[i].Chunks[a].Compare(seriesSet[i].Chunks[b]) > 0
		})
	}
	sort.Slice(seriesSet, func(i, j int) bool {
		return labels.Compare(
			labelpb.ZLabelsToPromLabels(seriesSet[i].Labels),
			labelpb.ZLabelsToPromLabels(seriesSet[j].Labels),
		) < 0
	})
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

type announcedLabelSource struct {
	name   string
	client queryBackendAPI
}

type announcedLabelSourceConfig struct {
	name   string
	config queryBackendConfig
}

type announcedLabelSourceFailure struct {
	source string
	err    error
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
	_, err := s.UpdateFromSources(ctx, []announcedLabelSource{{name: "backend", client: client}}, labelNames, 0, 0)
	return err
}

func (s *announcedLabelSetsStore) UpdateFromSources(ctx context.Context, sources []announcedLabelSource, labelNames []string, sourceTimeout, lookback time.Duration) ([]announcedLabelSourceFailure, error) {
	if len(sources) == 0 {
		return nil, fmt.Errorf("no announced label sources configured")
	}
	startTime, endTime, err := announcedLabelTimeRange(lookback)
	if err != nil {
		return nil, err
	}

	labelSets := make([]labels.Labels, 0)
	seen := make(map[string]struct{})
	failures := make([]announcedLabelSourceFailure, 0)
	successfulSources := 0

	for _, source := range sources {
		sourceCtx := ctx
		cancel := func() {}
		if sourceTimeout > 0 {
			sourceCtx, cancel = context.WithTimeout(ctx, sourceTimeout)
		}
		sourceLabelSets, err := announcedLabelSetsFromBackend(sourceCtx, source.client, labelNames, startTime, endTime)
		cancel()
		if err != nil {
			failures = append(failures, announcedLabelSourceFailure{source: source.name, err: err})
			continue
		}

		successfulSources++
		for _, labelSet := range sourceLabelSets {
			key := labelSet.String()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			labelSets = append(labelSets, labelSet)
		}
	}

	if successfulSources == 0 {
		return failures, announcedLabelSourceFailuresError(failures)
	}

	sort.Slice(labelSets, func(i, j int) bool {
		return labelSets[i].String() < labelSets[j].String()
	})

	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.labelSets = labelSets
	return failures, nil
}

func announcedLabelTimeRange(lookback time.Duration) (time.Time, time.Time, error) {
	if lookback < 0 {
		return time.Time{}, time.Time{}, fmt.Errorf("query.announce-label-lookback must be greater than or equal to zero")
	}
	if lookback == 0 {
		return time.Time{}, time.Time{}, nil
	}

	endTime := time.Now().UTC()
	return endTime.Add(-lookback), endTime, nil
}

func announcedLabelSetsFromBackend(ctx context.Context, client queryBackendAPI, labelNames []string, startTime, endTime time.Time) ([]labels.Labels, error) {
	labelSets := make([]labels.Labels, 0)
	seen := make(map[string]struct{})

	for _, labelName := range labelNames {
		if !model.LabelName(labelName).IsValid() {
			return nil, fmt.Errorf("invalid announced label name %q", labelName)
		}

		values, _, err := client.LabelValues(ctx, labelName, nil, startTime, endTime)
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

func createAnnouncedLabelSources(queryConfig queryBackendConfig) ([]announcedLabelSource, error) {
	sourceConfigs := announcedLabelSourceConfigs(queryConfig)
	sources := make([]announcedLabelSource, 0, len(sourceConfigs))
	for _, sourceConfig := range sourceConfigs {
		client, err := createQueryBackendClient(sourceConfig.config)
		if err != nil {
			return nil, fmt.Errorf("create announced label source %q: %w", sourceConfig.name, err)
		}
		sources = append(sources, announcedLabelSource{
			name:   sourceConfig.name,
			client: client,
		})
	}
	return sources, nil
}

func announcedLabelSourceConfigs(queryConfig queryBackendConfig) []announcedLabelSourceConfig {
	headerName, headerValue, ok := headerValue(queryConfig.Headers, "X-Scope-OrgID")
	if !ok {
		return []announcedLabelSourceConfig{{name: "backend", config: queryConfig}}
	}

	tenants := splitTenantHeader(headerValue)
	if len(tenants) == 0 {
		return []announcedLabelSourceConfig{{name: "backend", config: queryConfig}}
	}

	sourceConfigs := make([]announcedLabelSourceConfig, 0, len(tenants))
	for _, tenant := range tenants {
		sourceConfig := queryConfig
		sourceConfig.Headers = cloneHeaders(queryConfig.Headers)
		sourceConfig.Headers[headerName] = tenant
		sourceConfigs = append(sourceConfigs, announcedLabelSourceConfig{
			name:   tenant,
			config: sourceConfig,
		})
	}
	return sourceConfigs
}

func headerValue(headers map[string]string, name string) (string, string, bool) {
	for headerName, headerValue := range headers {
		if strings.EqualFold(headerName, name) {
			return headerName, headerValue, true
		}
	}
	return "", "", false
}

func splitTenantHeader(value string) []string {
	parts := strings.Split(value, "|")
	tenants := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		tenants = append(tenants, part)
	}
	return tenants
}

func cloneHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return nil
	}

	result := make(map[string]string, len(headers))
	for name, value := range headers {
		result[name] = value
	}
	return result
}

func announcedLabelSourceFailuresError(failures []announcedLabelSourceFailure) error {
	if len(failures) == 0 {
		return fmt.Errorf("all announced label sources failed")
	}

	messages := make([]string, 0, len(failures))
	for _, failure := range failures {
		messages = append(messages, fmt.Sprintf("%s: %s", failure.source, failure.err))
	}
	return fmt.Errorf("all announced label sources failed: %s", strings.Join(messages, "; "))
}

func logAnnouncedLabelSourceFailures(logger log.Logger, failures []announcedLabelSourceFailure) {
	for _, failure := range failures {
		level.Warn(logger).Log("msg", "loading announced label sets from source failed", "source", failure.source, "err", failure.err)
	}
}

func announcedLabelSourceNames(sources []announcedLabelSource) []string {
	names := make([]string, 0, len(sources))
	for _, source := range sources {
		names = append(names, source.name)
	}
	return names
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
	apiMode            infoAPIMode
}

func (info *infoServer) Info(ctx context.Context, in *infopb.InfoRequest) (*infopb.InfoResponse, error) {
	labelSets := labelpb.ZLabelSetsFromPromLabels(labels.FromStrings("query-backend", info.queryBackend))
	tsdbInfos := []infopb.TSDBInfo{{MinTime: math.MinInt64, MaxTime: math.MaxInt64}}
	usingFallbackLabelSet := true
	externalLabels := labels.EmptyLabels()
	if info.externalLabels != nil {
		externalLabels = info.externalLabels()
	}

	if info.announcedLabelSets != nil {
		announcedLabelSets := info.announcedLabelSets()
		if len(announcedLabelSets) > 0 {
			announcedLabelSets = extendLabelSets(announcedLabelSets, externalLabels)
			labelSets = labelpb.ZLabelSetsFromPromLabels(announcedLabelSets...)
			tsdbInfos = tsdbInfosFromLabelSets(labelSets)
			usingFallbackLabelSet = false
		}
	}

	if usingFallbackLabelSet {
		if externalLabels.Len() > 0 {
			labelSets = labelpb.ZLabelSetsFromPromLabels(externalLabels)
			tsdbInfos = tsdbInfosFromLabelSets(labelSets)
		}
	}

	componentType := component.Store.String()
	var queryInfo *infopb.QueryAPIInfo
	if info.apiMode.advertisesQueryAPI() {
		componentType = component.Query.String()
		queryInfo = &infopb.QueryAPIInfo{}
	}

	var storeInfo *infopb.StoreInfo
	if info.apiMode.advertisesStoreAPI() {
		storeInfo = &infopb.StoreInfo{
			MinTime:                      math.MinInt64,
			MaxTime:                      math.MaxInt64,
			SupportsWithoutReplicaLabels: true,
			TsdbInfos:                    tsdbInfos,
		}
	}

	return &infopb.InfoResponse{
		ComponentType: componentType,
		LabelSets:     labelSets,
		Store:         storeInfo,
		Query:         queryInfo,
	}, nil
}

func extendLabelSets(labelSets []labels.Labels, externalLabels labels.Labels) []labels.Labels {
	if externalLabels.IsEmpty() {
		result := make([]labels.Labels, 0, len(labelSets))
		for _, labelSet := range labelSets {
			result = append(result, labelSet.Copy())
		}
		return result
	}

	result := make([]labels.Labels, 0, len(labelSets))
	for _, labelSet := range labelSets {
		result = append(result, labelpb.ExtendSortedLabels(labelSet, externalLabels))
	}
	return result
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
		client, err := authhttptransport.NewClient(&authhttptransport.Options{
			BaseRoundTripper: transport,
			DetectOpts: &authcredentials.DetectOptions{
				CredentialsFile: queryConfig.Auth.CredentialsFile,
				Scopes:          googleAuthScopes(queryConfig.Auth),
			},
		})
		if err != nil {
			return nil, fmt.Errorf("error creating Google auth HTTP transport: %w", err)
		}
		transport = client.Transport
	}

	return newHeaderRoundTripper(transport, queryConfig.Headers), nil
}

func queryAuthEnabled(auth queryBackendAuthConfig) bool {
	return auth.Google || auth.CredentialsFile != "" || len(auth.Scopes) > 0
}

func googleAuthScopes(auth queryBackendAuthConfig) []string {
	if len(auth.Scopes) > 0 {
		return auth.Scopes
	}
	if auth.Google {
		return []string{googleMonitoringReadScope}
	}
	return nil
}

func newGCPQueryBackends(baseConfig queryBackendConfig, projects []string, staticExternalLabels labels.Labels) ([]queryBackendEndpoint, error) {
	backends := make([]queryBackendEndpoint, 0, len(projects))
	for _, project := range projects {
		targetURL, err := googlePrometheusTargetURL(project)
		if err != nil {
			return nil, err
		}

		backendConfig := baseConfig
		backendConfig.QueryTargetURL = targetURL
		backendConfig.Auth.Google = true

		client, err := createQueryBackendClient(backendConfig)
		if err != nil {
			return nil, fmt.Errorf("create Google project backend %q: %w", project, err)
		}

		projectLabels := labelpb.ExtendSortedLabels(staticExternalLabels, labels.FromStrings("prometheus", "gcp-"+project))
		backends = append(backends, queryBackendEndpoint{
			name:           project,
			queryBackend:   targetURL,
			client:         client,
			externalLabels: staticExternalLabelsFunc(projectLabels),
		})
	}
	return backends, nil
}

func queryBackendLabelSets(backends []queryBackendEndpoint) []labels.Labels {
	labelSets := make([]labels.Labels, 0, len(backends))
	for _, backend := range backends {
		externalLabels := backend.labels()
		if externalLabels.IsEmpty() {
			continue
		}
		labelSets = append(labelSets, externalLabels)
	}
	return labelSets
}

// createQueryBackendClient creates a connection with the backend that the queries will be forwarded to.
func createQueryBackendClient(queryConfig queryBackendConfig) (queryBackendAPI, error) {
	transport, err := newQueryBackendRoundTripper(queryConfig)
	if err != nil {
		return nil, err
	}
	targetURL, err := backendTargetURL(queryConfig)
	if err != nil {
		return nil, err
	}
	client, err := api.NewClient(api.Config{
		Address:      targetURL,
		RoundTripper: transport,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating client: %s", err)
	}
	return v1.NewAPI(client), nil
}

func newWebHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/-/healthy", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "OK")
	})
	mux.HandleFunc("/-/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "OK")
	})
	return mux
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
	infoMode, err := parseInfoAPIMode(*grpcInfoAPIMode, *grpcInfoAdvertiseQueryAPI)
	if err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}

	gcpProjects, err := normalizeGCPProjects(queryGCPProjects.values())
	if err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}
	staticExternalLabels := labels.FromMap(queryExternalLabel.values())
	announcedLabelNames := queryAnnounceLabels.values()

	// Configuration Loading.
	queryTargetURLValue := *queryTargetURL
	googleAuth := *queryAuthGoogle
	if len(gcpProjects) > 0 {
		if strings.TrimSpace(*queryTargetURL) != "" {
			level.Error(logger).Log("msg", "query.target-url cannot be used with query.gcp-project because the Google target URL is derived per project")
			os.Exit(1)
		}
		if *queryExternalLabels {
			level.Error(logger).Log("msg", "query.external-labels cannot be used with query.gcp-project because external labels are derived per project")
			os.Exit(1)
		}
		if strings.TrimSpace(*queryExternalLabelsURL) != "" {
			level.Error(logger).Log("msg", "query.external-labels-url cannot be used with query.gcp-project")
			os.Exit(1)
		}
		if len(announcedLabelNames) > 0 {
			level.Error(logger).Log("msg", "query.announce-label cannot be used with query.gcp-project because announced label sets are derived per project")
			os.Exit(1)
		}
		if staticExternalLabels.Get("prometheus") != "" {
			level.Error(logger).Log("msg", "query.external-label=prometheus=... cannot be used with query.gcp-project because prometheus=gcp-<project> is derived per project")
			os.Exit(1)
		}
		queryTargetURLValue, err = googlePrometheusTargetURL(gcpProjects[0])
		if err != nil {
			level.Error(logger).Log("err", err)
			os.Exit(1)
		}
		googleAuth = true
	}

	queryConfig, err := newQueryBackendConfig(queryTargetURLValue, queryHeaders.values(), queryParams.values(), googleAuth, *queryAuthCredentialsFile, queryAuthScopes.values())
	if err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}
	// Query client setup.
	externalLabels := newExternalLabelsStore()
	externalLabelsForRequests := func() labels.Labels {
		return labelpb.ExtendSortedLabels(externalLabels.Labels(), staticExternalLabels)
	}
	if staticExternalLabels.Len() > 0 {
		level.Info(logger).Log("msg", "configured static external labels", "external_labels", staticExternalLabels.String())
	}

	var queryBackends []queryBackendEndpoint
	var queryBackendClient queryBackendAPI
	var externalLabelsClient queryBackendAPI
	if len(gcpProjects) > 0 {
		queryBackends, err = newGCPQueryBackends(*queryConfig, gcpProjects, staticExternalLabels)
		if err != nil {
			level.Error(logger).Log("msg", "Error creating Google project clients", "err", err)
			os.Exit(1)
		}
		level.Info(logger).Log("msg", "configured Google Managed Service for Prometheus projects", "projects", strings.Join(gcpProjects, ","), "auth_google", queryConfig.Auth.Google)
	} else {
		queryBackendClient, err = createQueryBackendClient(*queryConfig)
		if err != nil {
			level.Error(logger).Log("msg", "Error creating client", "err", err)
			os.Exit(1)
		}
		queryBackends = []queryBackendEndpoint{{
			name:           "backend",
			queryBackend:   queryConfig.QueryTargetURL,
			client:         queryBackendClient,
			externalLabels: externalLabelsForRequests,
		}}

		externalLabelsClient = queryBackendClient
		if *queryExternalLabelsURL != "" {
			externalLabelsConfig := *queryConfig
			externalLabelsConfig.QueryTargetURL = *queryExternalLabelsURL
			externalLabelsClient, err = createQueryBackendClient(externalLabelsConfig)
			if err != nil {
				level.Error(logger).Log("msg", "Error creating external labels client", "err", err)
				os.Exit(1)
			}
		}
	}

	announcedLabelSets := newAnnouncedLabelSetsStore()
	var announcedLabelSources []announcedLabelSource

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
		if *queryAnnounceLabelLookback < 0 {
			level.Error(logger).Log("msg", "query.announce-label-lookback must be greater than or equal to zero")
			os.Exit(1)
		}

		announcedLabelSources, err = createAnnouncedLabelSources(*queryConfig)
		if err != nil {
			level.Error(logger).Log("msg", "Error creating announced label sources", "err", err)
			os.Exit(1)
		}

		failures, err := announcedLabelSets.UpdateFromSources(context.Background(), announcedLabelSources, announcedLabelNames, *queryAnnounceLabelTimeout, *queryAnnounceLabelLookback)
		logAnnouncedLabelSourceFailures(logger, failures)
		if err != nil {
			level.Error(logger).Log("msg", "Error loading initial announced label sets", "err", err)
			os.Exit(1)
		}
		if len(announcedLabelSets.LabelSets()) == 0 {
			level.Error(logger).Log("msg", "no announced label values found on backend", "labels", fmt.Sprint(announcedLabelNames))
			os.Exit(1)
		}
		level.Info(logger).Log("msg", "successfully loaded backend announced label sets", "labels", fmt.Sprint(announcedLabelNames), "sources", fmt.Sprint(announcedLabelSourceNames(announcedLabelSources)), "lookback", queryAnnounceLabelLookback.String(), "label_sets", fmt.Sprint(announcedLabelSets.LabelSets()))
	}

	queryBackendDescription := queryConfig.QueryTargetURL
	infoExternalLabels := externalLabelsForRequests
	infoAnnouncedLabelSets := announcedLabelSets.LabelSets
	if len(gcpProjects) > 0 {
		queryBackendDescription = "gcp-projects:" + strings.Join(gcpProjects, ",")
		infoExternalLabels = labels.EmptyLabels
		infoAnnouncedLabelSets = func() []labels.Labels {
			return queryBackendLabelSets(queryBackends)
		}
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
					failures, err := announcedLabelSets.UpdateFromSources(ctx, announcedLabelSources, announcedLabelNames, *queryAnnounceLabelTimeout, *queryAnnounceLabelLookback)
					logAnnouncedLabelSourceFailures(logger, failures)
					if err != nil {
						level.Warn(logger).Log("msg", "updating announced label sets failed", "err", err)
						continue
					}
					level.Info(logger).Log("msg", "updated backend announced label sets", "labels", fmt.Sprint(announcedLabelNames), "sources", fmt.Sprint(announcedLabelSourceNames(announcedLabelSources)), "lookback", queryAnnounceLabelLookback.String(), "label_sets", fmt.Sprint(announcedLabelSets.LabelSets()))
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
		queryServer := newQueryServerFromBackends(queryBackends, queryDropLabels.values())
		storepb.RegisterStoreServer(server, queryServer)
		querypb.RegisterQueryServer(server, queryServer)
		infopb.RegisterInfoServer(server, &infoServer{
			queryBackend:       queryBackendDescription,
			externalLabels:     infoExternalLabels,
			announcedLabelSets: infoAnnouncedLabelSets,
			apiMode:            infoMode,
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
		server := &http.Server{Addr: *metricsAddress, Handler: newWebHandler()}

		g.Add(func() error {
			level.Info(logger).Log("msg", "Starting web server", "listen", *metricsAddress)
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

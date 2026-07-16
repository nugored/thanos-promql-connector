package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
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
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
	"google.golang.org/api/option"
	apihttp "google.golang.org/api/transport/http"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var (
	queryTargetURL  = flag.String("query.target-url", "", "PromQL HTTP API backend URL.")
	queryHeaders    headerFlags
	queryAuthScopes stringListFlag

	queryAuthCredentialsFile = flag.String("query.auth.credentials-file", "", "Google auth credentials file for backend requests.")
	querySeriesStep          = flag.Duration("query.series-step", time.Minute, "Fallback step for StoreAPI series queries when Thanos does not send a query step hint.")
	connectorAddress         = flag.String("connector-address", ":8081",
		"Address on which to expose the query grpc server.")
	metricsAddress = flag.String("metrics-address", ":9090",
		"Address on which to expose metrics")
)

func init() {
	flag.Var(&queryHeaders, "query.header", "Static header to add to backend requests, in Name=Value or Name: Value format. May be repeated.")
	flag.Var(&queryAuthScopes, "query.auth.scope", "Google auth OAuth scope for backend requests. May be repeated or comma-separated.")
}

type queryBackendAPI interface {
	LabelNames(ctx context.Context, matches []string, startTime, endTime time.Time, opts ...v1.Option) ([]string, v1.Warnings, error)
	LabelValues(ctx context.Context, label string, matches []string, startTime, endTime time.Time, opts ...v1.Option) (model.LabelValues, v1.Warnings, error)
	Query(ctx context.Context, query string, ts time.Time, opts ...v1.Option) (model.Value, v1.Warnings, error)
	QueryRange(ctx context.Context, query string, r v1.Range, opts ...v1.Option) (model.Value, v1.Warnings, error)
	Series(ctx context.Context, matches []string, startTime, endTime time.Time, opts ...v1.Option) ([]model.LabelSet, v1.Warnings, error)
}

type queryServer struct {
	queryBackendClient queryBackendAPI
}

func (qs *queryServer) Series(request *storepb.SeriesRequest, server storepb.Store_SeriesServer) error {
	selector, err := querySelectorFromMatchers(request.Matchers)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

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
		Step:  seriesStep(request, *querySeriesStep),
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
			Labels: zLabelsFromMetric(result.Metric),
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
	matches, err := labelAPISelectorsFromMatchers(request.Matchers)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	req, warnings, err := qs.queryBackendClient.LabelNames(ctx, matches, timeFromMillis(request.Start), timeFromMillis(request.End))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &storepb.LabelNamesResponse{
		Names:    req,
		Warnings: warnings,
		Hints:    nil,
	}, nil
}

func (qs *queryServer) LabelValues(ctx context.Context, request *storepb.LabelValuesRequest) (*storepb.LabelValuesResponse, error) {
	matches, err := labelAPISelectorsFromMatchers(request.Matchers)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
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
				Labels:  zLabelsFromMetric(result.Metric),
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
				Labels:  zLabelsFromMetric(result.Metric),
			}
			if err := srv.Send(querypb.NewQueryRangeResponse(series)); err != nil {
				return err
			}
		}
	case model.Vector:
		for _, result := range results {
			series := &prompb.TimeSeries{
				Samples: []prompb.Sample{{Value: float64(result.Value), Timestamp: int64(result.Timestamp)}},
				Labels:  zLabelsFromMetric(result.Metric),
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
			Labels: zLabelsFromLabelSet(labelSet),
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

// zLabelsFromMetric converts model.Metric to labelpb.Zlabel.
func zLabelsFromMetric(metric model.Metric) []labelpb.ZLabel {
	labelSet := make(model.LabelSet, len(metric))
	for name, value := range metric {
		labelSet[name] = value
	}
	return zLabelsFromLabelSet(labelSet)
}

func zLabelsFromLabelSet(labelSet model.LabelSet) []labelpb.ZLabel {
	values := make(map[string]string, len(labelSet))
	for name, value := range labelSet {
		values[string(name)] = string(value)
	}
	return labelpb.ZLabelsFromPromLabels(labels.FromMap(values))
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

type infoServer struct {
	queryBackend string
}

func (info *infoServer) Info(ctx context.Context, in *infopb.InfoRequest) (*infopb.InfoResponse, error) {
	return &infopb.InfoResponse{
		ComponentType: component.Query.String(),
		LabelSets:     labelpb.ZLabelSetsFromPromLabels(labels.FromStrings("query-backend", info.queryBackend)),
		Store: &infopb.StoreInfo{
			MinTime:                      math.MinInt64,
			MaxTime:                      math.MaxInt64,
			SupportsWithoutReplicaLabels: true,
			TsdbInfos: []infopb.TSDBInfo{
				{
					MinTime: math.MinInt64,
					MaxTime: math.MaxInt64,
				},
			},
		},
		Query: &infopb.QueryAPIInfo{},
	}, nil
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
	{
		//grpc server.
		listener, err := net.Listen("tcp", *connectorAddress)
		if err != nil {
			panic(err)
		}
		server := grpc.NewServer()
		storepb.RegisterStoreServer(server, &queryServer{queryBackendClient: queryBackendClient})
		querypb.RegisterQueryServer(server, &queryServer{queryBackendClient: queryBackendClient})
		infopb.RegisterInfoServer(server, &infoServer{queryBackend: queryConfig.QueryTargetURL})
		g.Add(func() error {
			level.Info(logger).Log("msg", "Starting grpc server for query endpoint", "listen", *connectorAddress)
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

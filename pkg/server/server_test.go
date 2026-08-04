package server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/store/storepb"

	"main.go/pkg/backend"
	"main.go/pkg/promql"
)

type fakeQueryBackendAPI struct {
	labelValues      map[string]model.LabelValues
	labelValuesCalls *[]fakeLabelValuesCall
	seriesLabelSets  []model.LabelSet
	queryValue       model.Value
	queryCalls       *[]fakeQueryCall
	queryRangeValue  model.Value
	queryRangeCalls  *[]fakeQueryRangeCall
	err              error
}

type fakeLabelValuesCall struct {
	label     string
	matches   []string
	startTime time.Time
	endTime   time.Time
}

type fakeQueryCall struct {
	query string
	ts    time.Time
}

type fakeQueryRangeCall struct {
	query string
	r     v1.Range
}

func (f fakeQueryBackendAPI) Config(ctx context.Context) (v1.ConfigResult, error) {
	return v1.ConfigResult{}, nil
}

func (f fakeQueryBackendAPI) LabelNames(ctx context.Context, matches []string, startTime, endTime time.Time, opts ...v1.Option) ([]string, v1.Warnings, error) {
	return nil, nil, nil
}

func (f fakeQueryBackendAPI) LabelValues(ctx context.Context, label string, matches []string, startTime, endTime time.Time, opts ...v1.Option) (model.LabelValues, v1.Warnings, error) {
	if f.labelValuesCalls != nil {
		*f.labelValuesCalls = append(*f.labelValuesCalls, fakeLabelValuesCall{
			label:     label,
			matches:   append([]string(nil), matches...),
			startTime: startTime,
			endTime:   endTime,
		})
	}
	if f.err != nil {
		return nil, nil, f.err
	}
	return f.labelValues[label], nil, nil
}

func (f fakeQueryBackendAPI) Query(ctx context.Context, query string, ts time.Time, opts ...v1.Option) (model.Value, v1.Warnings, error) {
	if f.queryCalls != nil {
		*f.queryCalls = append(*f.queryCalls, fakeQueryCall{
			query: query,
			ts:    ts,
		})
	}
	if f.err != nil {
		return nil, nil, f.err
	}
	return f.queryValue, nil, nil
}

func (f fakeQueryBackendAPI) QueryRange(ctx context.Context, query string, r v1.Range, opts ...v1.Option) (model.Value, v1.Warnings, error) {
	if f.queryRangeCalls != nil {
		*f.queryRangeCalls = append(*f.queryRangeCalls, fakeQueryRangeCall{
			query: query,
			r:     r,
		})
	}
	if f.err != nil {
		return nil, nil, f.err
	}
	return f.queryRangeValue, nil, nil
}

func (f fakeQueryBackendAPI) Series(ctx context.Context, matches []string, startTime, endTime time.Time, opts ...v1.Option) ([]model.LabelSet, v1.Warnings, error) {
	if f.err != nil {
		return nil, nil, f.err
	}
	return f.seriesLabelSets, nil, nil
}

type fakeStoreSeriesServer struct {
	storepb.Store_SeriesServer
	ctx       context.Context
	responses []*storepb.SeriesResponse
}

func (f *fakeStoreSeriesServer) Context() context.Context {
	if f.ctx != nil {
		return f.ctx
	}
	return context.Background()
}

func (f *fakeStoreSeriesServer) Send(resp *storepb.SeriesResponse) error {
	f.responses = append(f.responses, resp)
	return nil
}

type fakeQueryServerStream struct {
	querypb.Query_QueryServer
	ctx       context.Context
	responses []*querypb.QueryResponse
	warnings  []*querypb.QueryResponse
}

func (f *fakeQueryServerStream) Context() context.Context {
	if f.ctx != nil {
		return f.ctx
	}
	return context.Background()
}

func (f *fakeQueryServerStream) Send(resp *querypb.QueryResponse) error {
	f.responses = append(f.responses, resp)
	return nil
}

func (f *fakeQueryServerStream) SendMsg(m any) error {
	if warnings, ok := m.(*querypb.QueryResponse); ok {
		f.warnings = append(f.warnings, warnings)
	}
	return nil
}

type fakeQueryRangeServerStream struct {
	querypb.Query_QueryRangeServer
	ctx       context.Context
	responses []*querypb.QueryRangeResponse
	warnings  []*querypb.QueryRangeResponse
}

func (f *fakeQueryRangeServerStream) Context() context.Context {
	if f.ctx != nil {
		return f.ctx
	}
	return context.Background()
}

func (f *fakeQueryRangeServerStream) Send(resp *querypb.QueryRangeResponse) error {
	f.responses = append(f.responses, resp)
	return nil
}

func (f *fakeQueryRangeServerStream) SendMsg(m any) error {
	if warnings, ok := m.(*querypb.QueryRangeResponse); ok {
		f.warnings = append(f.warnings, warnings)
	}
	return nil
}

func writeTestCertificate(t *testing.T) (string, string) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey() returned error: %v", err)
	}
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("CreateCertificate() returned error: %v", err)
	}

	dir := t.TempDir()
	certFile := filepath.Join(dir, "tls.crt")
	keyFile := filepath.Join(dir, "tls.key")

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER})
	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		t.Fatalf("WriteFile(%q) returned error: %v", certFile, err)
	}

	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatalf("WriteFile(%q) returned error: %v", keyFile, err)
	}

	return certFile, keyFile
}

func TestNewGRPCServerOptionsSkipsTLSWhenEmpty(t *testing.T) {
	options, enabled, err := NewGRPCServerOptions(GRPCServerTLSConfig{})
	if err != nil {
		t.Fatalf("NewGRPCServerOptions() returned error: %v", err)
	}
	if enabled {
		t.Fatal("NewGRPCServerOptions() enabled TLS, want disabled")
	}
	if options != nil {
		t.Fatalf("NewGRPCServerOptions() = %v, want nil", options)
	}
}

func TestNewGRPCServerOptionsRequiresCertAndKey(t *testing.T) {
	if _, _, err := NewGRPCServerOptions(GRPCServerTLSConfig{CertFile: "/tls/tls.crt"}); err == nil {
		t.Fatal("NewGRPCServerOptions() succeeded with cert only, want error")
	}
	if _, _, err := NewGRPCServerOptions(GRPCServerTLSConfig{KeyFile: "/tls/tls.key"}); err == nil {
		t.Fatal("NewGRPCServerOptions() succeeded with key only, want error")
	}
	if _, _, err := NewGRPCServerOptions(GRPCServerTLSConfig{ClientCAFile: "/tls/ca.crt"}); err == nil {
		t.Fatal("NewGRPCServerOptions() succeeded with client CA only, want error")
	}
}

func TestNewGRPCServerOptionsLoadsTLSCertificate(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t)

	options, enabled, err := NewGRPCServerOptions(GRPCServerTLSConfig{
		CertFile: certFile,
		KeyFile:  keyFile,
	})
	if err != nil {
		t.Fatalf("NewGRPCServerOptions() returned error: %v", err)
	}
	if !enabled {
		t.Fatal("NewGRPCServerOptions() disabled TLS, want enabled")
	}
	if len(options) != 1 {
		t.Fatalf("len(options) = %d, want 1", len(options))
	}
}

func TestNewGRPCServerOptionsLoadsClientCA(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t)

	options, enabled, err := NewGRPCServerOptions(GRPCServerTLSConfig{
		CertFile:     certFile,
		KeyFile:      keyFile,
		ClientCAFile: certFile,
	})
	if err != nil {
		t.Fatalf("NewGRPCServerOptions() returned error: %v", err)
	}
	if !enabled {
		t.Fatal("NewGRPCServerOptions() disabled TLS, want enabled")
	}
	if len(options) != 1 {
		t.Fatalf("len(options) = %d, want 1", len(options))
	}
}

func TestSeriesSkipsEmptyBackendStreams(t *testing.T) {
	server := NewQueryServer(fakeQueryBackendAPI{
		queryRangeValue: model.Matrix{
			&model.SampleStream{Metric: model.Metric{"__name__": "up", "job": "api"}, Values: []model.SamplePair{}},
		},
	}, nil, nil, time.Minute, 11000)

	stream := &fakeStoreSeriesServer{}
	err := server.Series(&storepb.SeriesRequest{
		MinTime: 1000,
		MaxTime: 2000,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"},
		},
	}, stream)
	if err != nil {
		t.Fatalf("Series() returned error: %v", err)
	}
	if len(stream.responses) != 0 {
		t.Fatalf("len(stream.responses) = %d, want 0", len(stream.responses))
	}
}

func TestSeriesSortsByFinalLabels(t *testing.T) {
	server := NewQueryServer(fakeQueryBackendAPI{
		queryRangeValue: model.Matrix{
			&model.SampleStream{
				Metric: model.Metric{"__name__": "up", "job": "worker"},
				Values: []model.SamplePair{{Timestamp: 1000, Value: 1}},
			},
			&model.SampleStream{
				Metric: model.Metric{"__name__": "up", "job": "api"},
				Values: []model.SamplePair{{Timestamp: 1000, Value: 1}},
			},
		},
	}, nil, nil, time.Minute, 11000)

	stream := &fakeStoreSeriesServer{}
	err := server.Series(&storepb.SeriesRequest{
		MinTime: 1000,
		MaxTime: 2000,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"},
		},
	}, stream)
	if err != nil {
		t.Fatalf("Series() returned error: %v", err)
	}
	if len(stream.responses) != 2 {
		t.Fatalf("len(stream.responses) = %d, want 2", len(stream.responses))
	}

	gotLabels0 := promql.ZLabelsToPromLabels(stream.responses[0].GetSeries().Labels).Get("job")
	gotLabels1 := promql.ZLabelsToPromLabels(stream.responses[1].GetSeries().Labels).Get("job")

	if gotLabels0 != "api" || gotLabels1 != "worker" {
		t.Fatalf("sorted series jobs = [%q, %q], want [api, worker]", gotLabels0, gotLabels1)
	}
}

func TestSeriesMetadataSortsByFinalLabels(t *testing.T) {
	server := NewQueryServer(fakeQueryBackendAPI{
		seriesLabelSets: []model.LabelSet{
			{"__name__": "up", "job": "worker"},
			{"__name__": "up", "job": "api"},
		},
	}, nil, nil, time.Minute, 11000)

	stream := &fakeStoreSeriesServer{}
	err := server.Series(&storepb.SeriesRequest{
		MinTime:    1000,
		MaxTime:    2000,
		SkipChunks: true,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"},
		},
	}, stream)
	if err != nil {
		t.Fatalf("Series() returned error: %v", err)
	}
	if len(stream.responses) != 2 {
		t.Fatalf("len(stream.responses) = %d, want 2", len(stream.responses))
	}

	gotLabels0 := promql.ZLabelsToPromLabels(stream.responses[0].GetSeries().Labels).Get("job")
	gotLabels1 := promql.ZLabelsToPromLabels(stream.responses[1].GetSeries().Labels).Get("job")

	if gotLabels0 != "api" || gotLabels1 != "worker" {
		t.Fatalf("sorted series jobs = [%q, %q], want [api, worker]", gotLabels0, gotLabels1)
	}
}

func TestQueryUsesQueryPlan(t *testing.T) {
	calls := make([]fakeQueryCall, 0, 1)
	server := NewQueryServer(
		fakeQueryBackendAPI{
			queryValue: model.Vector{
				&model.Sample{Metric: model.Metric{"__name__": "up"}, Value: 1, Timestamp: 1000},
			},
			queryCalls: &calls,
		},
		nil,
		nil,
		time.Minute,
		11000,
	)

	queryPlan, err := querypb.NewJSONEncodedPlan(&logicalplan.VectorSelector{
		VectorSelector: &parser.VectorSelector{
			LabelMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "api"),
			},
		},
	})
	if err != nil {
		t.Fatalf("NewJSONEncodedPlan() returned error: %v", err)
	}

	stream := &fakeQueryServerStream{}
	err = server.Query(&querypb.QueryRequest{
		TimeSeconds: 1000,
		QueryPlan:   queryPlan,
	}, stream)
	if err != nil {
		t.Fatalf("Query() returned error: %v", err)
	}
	if len(calls) != 1 {
		t.Fatalf("query calls = %d, want 1", len(calls))
	}
}

func TestQueryRangeUsesQueryPlan(t *testing.T) {
	calls := make([]fakeQueryRangeCall, 0, 1)
	server := NewQueryServer(
		fakeQueryBackendAPI{
			queryRangeValue: model.Matrix{
				&model.SampleStream{
					Metric: model.Metric{"__name__": "up"},
					Values: []model.SamplePair{{Timestamp: 1000, Value: 1}},
				},
			},
			queryRangeCalls: &calls,
		},
		nil,
		nil,
		time.Minute,
		11000,
	)

	queryPlan, err := querypb.NewJSONEncodedPlan(&logicalplan.VectorSelector{
		VectorSelector: &parser.VectorSelector{
			LabelMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "api"),
			},
		},
	})
	if err != nil {
		t.Fatalf("NewJSONEncodedPlan() returned error: %v", err)
	}

	stream := &fakeQueryRangeServerStream{}
	err = server.QueryRange(&querypb.QueryRangeRequest{
		StartTimeSeconds: 1000,
		EndTimeSeconds:   2000,
		IntervalSeconds:  60,
		QueryPlan:        queryPlan,
	}, stream)
	if err != nil {
		t.Fatalf("QueryRange() returned error: %v", err)
	}
	if len(calls) != 1 {
		t.Fatalf("query range calls = %d, want 1", len(calls))
	}
}

func TestInfoServerUsesAnnouncedLabelSets(t *testing.T) {
	info := &InfoServer{
		QueryBackend: "http://backend.example",
		ExternalLabels: func() labels.Labels {
			return labels.FromStrings("region", "global")
		},
		AnnouncedLabelSets: func() []labels.Labels {
			return []labels.Labels{
				labels.FromStrings("prometheus", "prom-a"),
				labels.FromStrings("prometheus", "prom-b"),
			}
		},
	}

	resp, err := info.Info(context.Background(), nil)
	if err != nil {
		t.Fatalf("Info() returned error: %v", err)
	}
	if resp.ComponentType != "store" {
		t.Fatalf("Info().ComponentType = %q, want store", resp.ComponentType)
	}
	if resp.Query != nil {
		t.Fatal("Info().Query is set, want nil by default")
	}

	got := make([]map[string]string, 0, len(resp.LabelSets))
	for _, labelSet := range resp.LabelSets {
		got = append(got, promql.ZLabelsToPromLabels(labelSet.Labels).Map())
	}
	want := []map[string]string{
		{"prometheus": "prom-a", "region": "global"},
		{"prometheus": "prom-b", "region": "global"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Info().LabelSets = %v, want %v", got, want)
	}

	gotTSDBs := make([]map[string]string, 0, len(resp.Store.TsdbInfos))
	for _, tsdbInfo := range resp.Store.TsdbInfos {
		gotTSDBs = append(gotTSDBs, promql.ZLabelsToPromLabels(tsdbInfo.Labels.Labels).Map())
	}
	if !reflect.DeepEqual(gotTSDBs, want) {
		t.Fatalf("Info().Store.TsdbInfos labels = %v, want %v", gotTSDBs, want)
	}
}

func TestInfoServerCanAdvertiseQueryAPI(t *testing.T) {
	info := &InfoServer{
		QueryBackend: "http://backend.example",
		APIMode:      InfoAPIModeBoth,
	}

	resp, err := info.Info(context.Background(), nil)
	if err != nil {
		t.Fatalf("Info() returned error: %v", err)
	}
	if resp.ComponentType != "query" {
		t.Fatalf("Info().ComponentType = %q, want query", resp.ComponentType)
	}
	if resp.Query == nil {
		t.Fatal("Info().Query is nil, want QueryAPI info")
	}
	if resp.Store == nil {
		t.Fatal("Info().Store is nil, want StoreAPI info")
	}
}

func TestInfoServerCanAdvertiseQueryAPIOnly(t *testing.T) {
	info := &InfoServer{
		QueryBackend: "http://backend.example",
		APIMode:      InfoAPIModeQuery,
		AnnouncedLabelSets: func() []labels.Labels {
			return []labels.Labels{labels.FromStrings("prometheus", "prom-a")}
		},
	}

	resp, err := info.Info(context.Background(), nil)
	if err != nil {
		t.Fatalf("Info() returned error: %v", err)
	}
	if resp.ComponentType != "query" {
		t.Fatalf("Info().ComponentType = %q, want query", resp.ComponentType)
	}
	if resp.Query == nil {
		t.Fatal("Info().Query is nil, want QueryAPI info")
	}
	if resp.Store != nil {
		t.Fatal("Info().Store is set, want nil in QueryAPI-only mode")
	}
	if got := promql.ZLabelsToPromLabels(resp.LabelSets[0].Labels).Map(); !reflect.DeepEqual(got, map[string]string{"prometheus": "prom-a"}) {
		t.Fatalf("Info().LabelSets[0] = %v, want prometheus label set", got)
	}
}

func TestParseInfoAPIMode(t *testing.T) {
	for _, testCase := range []struct {
		name              string
		value             string
		advertiseQueryAPI bool
		want              InfoAPIMode
		wantErr           bool
	}{
		{name: "default store", value: "", want: InfoAPIModeStore},
		{name: "store", value: "store", want: InfoAPIModeStore},
		{name: "query", value: "query", want: InfoAPIModeQuery},
		{name: "both", value: "both", want: InfoAPIModeBoth},
		{name: "legacy advertise query", value: "store", advertiseQueryAPI: true, want: InfoAPIModeBoth},
		{name: "invalid", value: "bad", wantErr: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := ParseInfoAPIMode(testCase.value, testCase.advertiseQueryAPI)
			if testCase.wantErr {
				if err == nil {
					t.Fatal("ParseInfoAPIMode() succeeded, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseInfoAPIMode() returned error: %v", err)
			}
			if got != testCase.want {
				t.Fatalf("ParseInfoAPIMode() = %q, want %q", got, testCase.want)
			}
		})
	}
}

func TestLabelValuesReturnsExternalLabelWithBackendMatchers(t *testing.T) {
	server := NewQueryServer(
		fakeQueryBackendAPI{err: errors.New("backend should not be queried")},
		nil,
		func() labels.Labels { return labels.FromStrings("prometheus", "gcp-itk8s-208609") },
		time.Minute,
		11000,
	)

	resp, err := server.LabelValues(context.Background(), &storepb.LabelValuesRequest{
		Label: "prometheus",
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "logging_googleapis_com:byte_count"},
			{Type: storepb.LabelMatcher_EQ, Name: "monitored_resource", Value: "gce_backend_service"},
		},
		Start: 1784717940000,
		End:   1784721600000,
	})
	if err != nil {
		t.Fatalf("LabelValues() returned error: %v", err)
	}

	want := []string{"gcp-itk8s-208609"}
	if !reflect.DeepEqual(resp.Values, want) {
		t.Fatalf("LabelValues().Values = %v, want %v", resp.Values, want)
	}
}

func TestLabelValuesReturnsExternalLabelsForMultipleBackends(t *testing.T) {
	server := NewQueryServerFromBackends([]backend.QueryBackendEndpoint{
		{
			Name:           "itk8s-208609",
			Client:         fakeQueryBackendAPI{err: errors.New("backend should not be queried")},
			ExternalLabels: backend.StaticExternalLabelsFunc(labels.FromStrings("prometheus", "gcp-itk8s-208609")),
		},
		{
			Name:           "space-prod",
			Client:         fakeQueryBackendAPI{err: errors.New("backend should not be queried")},
			ExternalLabels: backend.StaticExternalLabelsFunc(labels.FromStrings("prometheus", "gcp-space-prod")),
		},
	}, nil, time.Minute, 11000)

	resp, err := server.LabelValues(context.Background(), &storepb.LabelValuesRequest{
		Label: "prometheus",
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "logging_googleapis_com:byte_count"},
			{Type: storepb.LabelMatcher_EQ, Name: "monitored_resource", Value: "gce_backend_service"},
		},
		Start: 1784717940000,
		End:   1784721600000,
	})
	if err != nil {
		t.Fatalf("LabelValues() returned error: %v", err)
	}

	want := []string{"gcp-itk8s-208609", "gcp-space-prod"}
	if !reflect.DeepEqual(resp.Values, want) {
		t.Fatalf("LabelValues().Values = %v, want %v", resp.Values, want)
	}
}

func TestLabelValuesRoutesExternalLabelMatcherToOneBackend(t *testing.T) {
	server := NewQueryServerFromBackends([]backend.QueryBackendEndpoint{
		{
			Name:           "itk8s-208609",
			Client:         fakeQueryBackendAPI{err: errors.New("backend should not be queried")},
			ExternalLabels: backend.StaticExternalLabelsFunc(labels.FromStrings("prometheus", "gcp-itk8s-208609")),
		},
		{
			Name:           "space-prod",
			Client:         fakeQueryBackendAPI{err: errors.New("backend should not be queried")},
			ExternalLabels: backend.StaticExternalLabelsFunc(labels.FromStrings("prometheus", "gcp-space-prod")),
		},
	}, nil, time.Minute, 11000)

	resp, err := server.LabelValues(context.Background(), &storepb.LabelValuesRequest{
		Label: "prometheus",
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "prometheus", Value: "gcp-space-prod"},
			{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "logging_googleapis_com:byte_count"},
			{Type: storepb.LabelMatcher_EQ, Name: "monitored_resource", Value: "gce_backend_service"},
		},
	})
	if err != nil {
		t.Fatalf("LabelValues() returned error: %v", err)
	}

	want := []string{"gcp-space-prod"}
	if !reflect.DeepEqual(resp.Values, want) {
		t.Fatalf("LabelValues().Values = %v, want %v", resp.Values, want)
	}
}

func TestLabelValuesReadsNonExternalLabelFromSelectedBackend(t *testing.T) {
	itk8sCalls := make([]fakeLabelValuesCall, 0, 1)
	spaceCalls := make([]fakeLabelValuesCall, 0, 1)
	server := NewQueryServerFromBackends([]backend.QueryBackendEndpoint{
		{
			Name: "itk8s-208609",
			Client: fakeQueryBackendAPI{
				labelValues: map[string]model.LabelValues{
					"monitored_resource": {"gce_backend_service"},
				},
				labelValuesCalls: &itk8sCalls,
			},
			ExternalLabels: backend.StaticExternalLabelsFunc(labels.FromStrings("prometheus", "gcp-itk8s-208609")),
		},
		{
			Name: "space-prod",
			Client: fakeQueryBackendAPI{
				labelValues: map[string]model.LabelValues{
					"monitored_resource": {"k8s_container"},
				},
				labelValuesCalls: &spaceCalls,
			},
			ExternalLabels: backend.StaticExternalLabelsFunc(labels.FromStrings("prometheus", "gcp-space-prod")),
		},
	}, nil, time.Minute, 11000)

	resp, err := server.LabelValues(context.Background(), &storepb.LabelValuesRequest{
		Label: "monitored_resource",
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "prometheus", Value: "gcp-itk8s-208609"},
			{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "logging_googleapis_com:byte_count"},
			{Type: storepb.LabelMatcher_EQ, Name: "location", Value: "global"},
		},
	})
	if err != nil {
		t.Fatalf("LabelValues() returned error: %v", err)
	}

	want := []string{"gce_backend_service"}
	if !reflect.DeepEqual(resp.Values, want) {
		t.Fatalf("LabelValues().Values = %v, want %v", resp.Values, want)
	}
	if len(itk8sCalls) != 1 {
		t.Fatalf("itk8s LabelValues calls = %d, want 1", len(itk8sCalls))
	}
	if len(spaceCalls) != 0 {
		t.Fatalf("space LabelValues calls = %d, want 0", len(spaceCalls))
	}
	call := itk8sCalls[0]
	if call.label != "monitored_resource" {
		t.Fatalf("LabelValues() label = %q, want monitored_resource", call.label)
	}
	if len(call.matches) != 1 {
		t.Fatalf("LabelValues() matches = %v, want one selector", call.matches)
	}
	if strings.Contains(call.matches[0], "prometheus") {
		t.Fatalf("LabelValues() backend selector = %q, want prometheus matcher stripped", call.matches[0])
	}
	if !strings.Contains(call.matches[0], `__name__="logging_googleapis_com:byte_count"`) || !strings.Contains(call.matches[0], `location="global"`) {
		t.Fatalf("LabelValues() backend selector = %q, want non-external matchers kept", call.matches[0])
	}
}
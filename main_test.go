package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"
)

func TestNewQueryBackendRoundTripperSkipsGoogleAuthWhenAuthEmpty(t *testing.T) {
	rt, err := newQueryBackendRoundTripper(queryBackendConfig{})
	if err != nil {
		t.Fatalf("newQueryBackendRoundTripper() returned error: %v", err)
	}
	if rt != http.DefaultTransport {
		t.Fatalf("newQueryBackendRoundTripper() = %T, want http.DefaultTransport", rt)
	}
}

func TestNewQueryBackendRoundTripperKeepsHeadersWithoutAuth(t *testing.T) {
	rt, err := newQueryBackendRoundTripper(queryBackendConfig{
		Headers: map[string]string{"X-Scope-OrgID": "tenant1|tenant2"},
	})
	if err != nil {
		t.Fatalf("newQueryBackendRoundTripper() returned error: %v", err)
	}

	headerRT, ok := rt.(*headerRoundTripper)
	if !ok {
		t.Fatalf("newQueryBackendRoundTripper() = %T, want *headerRoundTripper", rt)
	}
	if headerRT.base != http.DefaultTransport {
		t.Fatalf("header round tripper base = %T, want http.DefaultTransport", headerRT.base)
	}
}

func TestNewGRPCServerOptionsSkipsTLSWhenEmpty(t *testing.T) {
	options, enabled, err := newGRPCServerOptions(grpcServerTLSConfig{})
	if err != nil {
		t.Fatalf("newGRPCServerOptions() returned error: %v", err)
	}
	if enabled {
		t.Fatal("newGRPCServerOptions() enabled TLS, want disabled")
	}
	if len(options) != 0 {
		t.Fatalf("len(options) = %d, want 0", len(options))
	}
}

func TestSnappyGRPCCompressorRegistered(t *testing.T) {
	if encoding.GetCompressor("snappy") == nil {
		t.Fatal("snappy gRPC compressor is not registered")
	}
}

func TestSnappyGRPCCompressorReadRoundTrip(t *testing.T) {
	compressor := encoding.GetCompressor("snappy")
	if compressor == nil {
		t.Fatal("snappy gRPC compressor is not registered")
	}

	payload := bytes.Repeat([]byte("promql-connector grpc request payload "), 256)
	var compressed bytes.Buffer
	writer, err := compressor.Compress(&compressed)
	if err != nil {
		t.Fatalf("Compress() returned error: %v", err)
	}
	if _, err := writer.Write(payload); err != nil {
		t.Fatalf("Write() returned error: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}

	reader, err := compressor.Decompress(bytes.NewReader(compressed.Bytes()))
	if err != nil {
		t.Fatalf("Decompress() returned error: %v", err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() returned error: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("decompressed payload mismatch: got %d bytes, want %d", len(got), len(payload))
	}

	n, err := reader.Read(make([]byte, 1))
	if n != 0 || err != io.EOF {
		t.Fatalf("Read() after EOF = %d, %v; want 0, EOF", n, err)
	}
}

func TestNewGRPCServerOptionsRequiresCertAndKey(t *testing.T) {
	if _, _, err := newGRPCServerOptions(grpcServerTLSConfig{CertFile: "/tls/tls.crt"}); err == nil {
		t.Fatal("newGRPCServerOptions() succeeded with cert only, want error")
	}
	if _, _, err := newGRPCServerOptions(grpcServerTLSConfig{KeyFile: "/tls/tls.key"}); err == nil {
		t.Fatal("newGRPCServerOptions() succeeded with key only, want error")
	}
	if _, _, err := newGRPCServerOptions(grpcServerTLSConfig{ClientCAFile: "/tls/ca.crt"}); err == nil {
		t.Fatal("newGRPCServerOptions() succeeded with client CA only, want error")
	}
}

func TestNewGRPCServerOptionsLoadsTLSCertificate(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t)

	options, enabled, err := newGRPCServerOptions(grpcServerTLSConfig{
		CertFile: certFile,
		KeyFile:  keyFile,
	})
	if err != nil {
		t.Fatalf("newGRPCServerOptions() returned error: %v", err)
	}
	if !enabled {
		t.Fatal("newGRPCServerOptions() disabled TLS, want enabled")
	}
	if len(options) != 1 {
		t.Fatalf("len(options) = %d, want 1", len(options))
	}
}

func TestNewGRPCServerOptionsLoadsClientCA(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t)

	options, enabled, err := newGRPCServerOptions(grpcServerTLSConfig{
		CertFile:     certFile,
		KeyFile:      keyFile,
		ClientCAFile: certFile,
	})
	if err != nil {
		t.Fatalf("newGRPCServerOptions() returned error: %v", err)
	}
	if !enabled {
		t.Fatal("newGRPCServerOptions() disabled TLS, want enabled")
	}
	if len(options) != 1 {
		t.Fatalf("len(options) = %d, want 1", len(options))
	}
}

func TestQueryAuthEnabled(t *testing.T) {
	tests := []struct {
		name string
		auth queryBackendAuthConfig
		want bool
	}{
		{name: "empty"},
		{name: "google adc", auth: queryBackendAuthConfig{Google: true}, want: true},
		{name: "credentials file", auth: queryBackendAuthConfig{CredentialsFile: "/key.json"}, want: true},
		{name: "scopes", auth: queryBackendAuthConfig{Scopes: []string{"https://www.googleapis.com/auth/monitoring.read"}}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := queryAuthEnabled(tt.auth); got != tt.want {
				t.Fatalf("queryAuthEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGoogleAuthScopes(t *testing.T) {
	tests := []struct {
		name string
		auth queryBackendAuthConfig
		want []string
	}{
		{name: "empty"},
		{name: "google adc default", auth: queryBackendAuthConfig{Google: true}, want: []string{googleMonitoringReadScope}},
		{name: "explicit scopes", auth: queryBackendAuthConfig{Google: true, Scopes: []string{"scope-a"}}, want: []string{"scope-a"}},
		{name: "credentials file without google flag keeps empty scopes", auth: queryBackendAuthConfig{CredentialsFile: "/key.json"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := googleAuthScopes(tt.auth); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("googleAuthScopes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHeaderFlagsSet(t *testing.T) {
	var headers headerFlags

	if err := headers.Set("X-Scope-OrgID=tenant1|tenant2"); err != nil {
		t.Fatalf("headers.Set() returned error: %v", err)
	}
	if err := headers.Set("Authorization: Bearer token:with-colon=and-equals"); err != nil {
		t.Fatalf("headers.Set() returned error: %v", err)
	}

	want := map[string]string{
		"Authorization": "Bearer token:with-colon=and-equals",
		"X-Scope-OrgID": "tenant1|tenant2",
	}
	if got := headers.values(); !reflect.DeepEqual(got, want) {
		t.Fatalf("headers.values() = %v, want %v", got, want)
	}
}

func TestHeaderFlagsSetRejectsInvalidValue(t *testing.T) {
	var headers headerFlags

	if err := headers.Set("missing-separator"); err == nil {
		t.Fatal("headers.Set() succeeded, want error")
	}
	if err := headers.Set(" =value"); err == nil {
		t.Fatal("headers.Set() succeeded with empty header name, want error")
	}
}

func TestQueryParamFlagsSet(t *testing.T) {
	var params queryParamFlags

	if err := params.Set(`storeMatch[]={connector!="thanos-promql-connector"}`); err != nil {
		t.Fatalf("params.Set() returned error: %v", err)
	}
	if err := params.Set("dedup=false"); err != nil {
		t.Fatalf("params.Set() returned error: %v", err)
	}

	want := map[string][]string{
		"dedup":        {"false"},
		"storeMatch[]": {`{connector!="thanos-promql-connector"}`},
	}
	if got := params.values(); !reflect.DeepEqual(got, want) {
		t.Fatalf("params.values() = %v, want %v", got, want)
	}
}

func TestQueryParamFlagsSetRejectsInvalidValue(t *testing.T) {
	var params queryParamFlags

	if err := params.Set("missing-separator"); err == nil {
		t.Fatal("params.Set() succeeded, want error")
	}
	if err := params.Set(" =value"); err == nil {
		t.Fatal("params.Set() succeeded with empty parameter name, want error")
	}
}

func TestLabelFlagsSet(t *testing.T) {
	var values labelFlags

	if err := values.Set("prometheus=gcp-project"); err != nil {
		t.Fatalf("values.Set() returned error: %v", err)
	}
	if err := values.Set("region=global"); err != nil {
		t.Fatalf("values.Set() returned error: %v", err)
	}

	want := map[string]string{"prometheus": "gcp-project", "region": "global"}
	if got := values.values(); !reflect.DeepEqual(got, want) {
		t.Fatalf("values.values() = %v, want %v", got, want)
	}
}

func TestLabelFlagsSetRejectsInvalidValue(t *testing.T) {
	var values labelFlags

	for _, value := range []string{"missing-separator", "=value", "invalid-name=value", "prometheus="} {
		t.Run(value, func(t *testing.T) {
			if err := values.Set(value); err == nil {
				t.Fatal("values.Set() succeeded, want error")
			}
		})
	}
}

func TestStringListFlagSet(t *testing.T) {
	var values stringListFlag

	if err := values.Set("scope-a, scope-b"); err != nil {
		t.Fatalf("values.Set() returned error: %v", err)
	}
	if err := values.Set("scope-c"); err != nil {
		t.Fatalf("values.Set() returned error: %v", err)
	}

	want := []string{"scope-a", "scope-b", "scope-c"}
	if got := values.values(); !reflect.DeepEqual(got, want) {
		t.Fatalf("values.values() = %v, want %v", got, want)
	}
}

func TestNormalizeGCPProjectsDeduplicates(t *testing.T) {
	got, err := normalizeGCPProjects([]string{"itk8s-208609", " space-prod ", "itk8s-208609"})
	if err != nil {
		t.Fatalf("normalizeGCPProjects() returned error: %v", err)
	}

	want := []string{"itk8s-208609", "space-prod"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalizeGCPProjects() = %v, want %v", got, want)
	}
}

func TestGooglePrometheusTargetURL(t *testing.T) {
	got, err := googlePrometheusTargetURL("itk8s-208609")
	if err != nil {
		t.Fatalf("googlePrometheusTargetURL() returned error: %v", err)
	}

	want := "https://monitoring.googleapis.com/v1/projects/itk8s-208609/location/global/prometheus"
	if got != want {
		t.Fatalf("googlePrometheusTargetURL() = %q, want %q", got, want)
	}
}

func TestNewQueryBackendConfigRequiresTargetURL(t *testing.T) {
	if _, err := newQueryBackendConfig("", nil, nil, false, "", nil); err == nil {
		t.Fatal("newQueryBackendConfig() succeeded without target URL, want error")
	}
}

func TestNewQueryBackendConfigBuildsConfigFromStartupParameters(t *testing.T) {
	cfg, err := newQueryBackendConfig(
		" http://127.0.0.1:18080/prometheus ",
		map[string]string{"X-Scope-OrgID": "tenant1|tenant2"},
		map[string][]string{"storeMatch[]": {`{connector!="thanos-promql-connector"}`}},
		true,
		" /key.json ",
		[]string{" https://www.googleapis.com/auth/monitoring.read ", ""},
	)
	if err != nil {
		t.Fatalf("newQueryBackendConfig() returned error: %v", err)
	}

	if cfg.QueryTargetURL != "http://127.0.0.1:18080/prometheus" {
		t.Fatalf("QueryTargetURL = %q", cfg.QueryTargetURL)
	}
	if got := cfg.Headers; !reflect.DeepEqual(got, map[string]string{"X-Scope-OrgID": "tenant1|tenant2"}) {
		t.Fatalf("Headers = %v", got)
	}
	if got := cfg.QueryParams; !reflect.DeepEqual(got, map[string][]string{"storeMatch[]": {`{connector!="thanos-promql-connector"}`}}) {
		t.Fatalf("QueryParams = %v", got)
	}
	if !cfg.Auth.Google {
		t.Fatal("Auth.Google = false, want true")
	}
	if cfg.Auth.CredentialsFile != "/key.json" {
		t.Fatalf("CredentialsFile = %q", cfg.Auth.CredentialsFile)
	}
	if got := cfg.Auth.Scopes; !reflect.DeepEqual(got, []string{"https://www.googleapis.com/auth/monitoring.read"}) {
		t.Fatalf("Scopes = %v", got)
	}
}

func TestBackendTargetURLAppendsQueryParams(t *testing.T) {
	got, err := backendTargetURL(queryBackendConfig{
		QueryTargetURL: "http://thanos-query:10902/prometheus?dedup=true",
		QueryParams: map[string][]string{
			"storeMatch[]": {`{connector!="thanos-promql-connector"}`, `{cluster="prod"}`},
		},
	})
	if err != nil {
		t.Fatalf("backendTargetURL() returned error: %v", err)
	}

	want := "http://thanos-query:10902/prometheus?dedup=true&storeMatch%5B%5D=%7Bconnector%21%3D%22thanos-promql-connector%22%7D&storeMatch%5B%5D=%7Bcluster%3D%22prod%22%7D"
	if got != want {
		t.Fatalf("backendTargetURL() = %q, want %q", got, want)
	}
}

func TestQuerySelectorFromMatchers(t *testing.T) {
	selector, err := querySelectorFromMatchers([]storepb.LabelMatcher{
		{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"},
		{Type: storepb.LabelMatcher_RE, Name: "job", Value: "api|worker"},
	})
	if err != nil {
		t.Fatalf("querySelectorFromMatchers() returned error: %v", err)
	}

	want := `{__name__="up", job=~"api|worker"}`
	if selector != want {
		t.Fatalf("querySelectorFromMatchers() = %q, want %q", selector, want)
	}
}

func TestQuerySelectorFromMatchersDefaultsToAllSeries(t *testing.T) {
	selector, err := querySelectorFromMatchers(nil)
	if err != nil {
		t.Fatalf("querySelectorFromMatchers() returned error: %v", err)
	}

	want := `{__name__=~".+"}`
	if selector != want {
		t.Fatalf("querySelectorFromMatchers() = %q, want %q", selector, want)
	}
}

func TestLabelAPISelectorsFromMatchers(t *testing.T) {
	selectors, err := labelAPISelectorsFromMatchers([]storepb.LabelMatcher{
		{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"},
	})
	if err != nil {
		t.Fatalf("labelAPISelectorsFromMatchers() returned error: %v", err)
	}

	want := []string{`{job="api"}`}
	if !reflect.DeepEqual(selectors, want) {
		t.Fatalf("labelAPISelectorsFromMatchers() = %v, want %v", selectors, want)
	}
}

func TestTimeFromMillis(t *testing.T) {
	got := timeFromMillis(1710000000123)
	want := time.Unix(1710000000, 123*int64(time.Millisecond)).UTC()
	if !got.Equal(want) {
		t.Fatalf("timeFromMillis() = %s, want %s", got, want)
	}
}

func TestSeriesStep(t *testing.T) {
	tests := []struct {
		name     string
		request  *storepb.SeriesRequest
		fallback time.Duration
		want     time.Duration
	}{
		{
			name:     "query hints",
			request:  &storepb.SeriesRequest{QueryHints: &storepb.QueryHints{StepMillis: 15000}, Step: int64(time.Minute / time.Millisecond)},
			fallback: time.Minute,
			want:     15 * time.Second,
		},
		{
			name:     "deprecated step",
			request:  &storepb.SeriesRequest{Step: 30000},
			fallback: time.Minute,
			want:     30 * time.Second,
		},
		{
			name:     "fallback",
			request:  &storepb.SeriesRequest{},
			fallback: 2 * time.Minute,
			want:     2 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := seriesStep(tt.request, tt.fallback); got != tt.want {
				t.Fatalf("seriesStep() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestSeriesStepForRangeClampsToMaxPoints(t *testing.T) {
	start := time.Unix(0, 0)
	end := start.Add(28 * 24 * time.Hour)

	got := seriesStepForRange(
		&storepb.SeriesRequest{QueryHints: &storepb.QueryHints{StepMillis: int64((15 * time.Second) / time.Millisecond)}},
		time.Minute,
		start,
		end,
		11000,
	)

	want := minStepForMaxPoints(28*24*time.Hour, 11000)
	if got != want {
		t.Fatalf("seriesStepForRange() = %s, want %s", got, want)
	}
	if got < 3*time.Minute {
		t.Fatalf("seriesStepForRange() = %s, want clamped multi-minute step", got)
	}
}

func TestSeriesStepForRangeKeepsStepWhenUnderMaxPoints(t *testing.T) {
	start := time.Unix(0, 0)
	end := start.Add(time.Hour)

	got := seriesStepForRange(&storepb.SeriesRequest{}, time.Minute, start, end, 11000)
	if got != time.Minute {
		t.Fatalf("seriesStepForRange() = %s, want 1m", got)
	}
}

func TestSeriesStepForRangeCanDisableMaxPointsClamp(t *testing.T) {
	start := time.Unix(0, 0)
	end := start.Add(28 * 24 * time.Hour)

	got := seriesStepForRange(&storepb.SeriesRequest{}, time.Minute, start, end, 0)
	if got != time.Minute {
		t.Fatalf("seriesStepForRange() = %s, want unclamped 1m", got)
	}
}

func TestChunksFromModelSamples(t *testing.T) {
	chunks, err := chunksFromModelSamples([]model.SamplePair{
		{Timestamp: 1000, Value: 1},
		{Timestamp: 2000, Value: 2},
	})
	if err != nil {
		t.Fatalf("chunksFromModelSamples() returned error: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("len(chunks) = %d, want 1", len(chunks))
	}

	chunk := chunks[0]
	if chunk.MinTime != 1000 || chunk.MaxTime != 2000 {
		t.Fatalf("chunk time range = [%d,%d], want [1000,2000]", chunk.MinTime, chunk.MaxTime)
	}
	if chunk.Raw == nil {
		t.Fatal("chunk.Raw is nil")
	}
	if chunk.Raw.Type != storepb.Chunk_XOR {
		t.Fatalf("chunk.Raw.Type = %s, want XOR", chunk.Raw.Type)
	}
	if got := chunk.Raw.XORNumSamples(); got != 2 {
		t.Fatalf("chunk.Raw.XORNumSamples() = %d, want 2", got)
	}
}

func TestSeriesSkipsEmptyBackendStreams(t *testing.T) {
	server := newQueryServer(fakeQueryBackendAPI{
		queryRangeValue: model.Matrix{
			&model.SampleStream{
				Metric: model.Metric{"__name__": "up", "job": "empty"},
			},
			&model.SampleStream{
				Metric: model.Metric{"__name__": "up", "job": "api"},
				Values: []model.SamplePair{
					{Timestamp: 1000, Value: 1},
				},
			},
		},
	}, nil, nil)
	stream := &recordingStoreSeriesServer{ctx: context.Background()}

	err := server.Series(&storepb.SeriesRequest{
		MinTime: 0,
		MaxTime: 2000,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"},
		},
	}, stream)
	if err != nil {
		t.Fatalf("Series() returned error: %v", err)
	}

	if len(stream.responses) != 1 {
		t.Fatalf("len(responses) = %d, want 1", len(stream.responses))
	}
	series := stream.responses[0].GetSeries()
	if series == nil {
		t.Fatal("response series is nil")
	}
	gotLabels := labelpb.ZLabelsToPromLabels(series.Labels).Map()
	wantLabels := map[string]string{"__name__": "up", "job": "api"}
	if !reflect.DeepEqual(gotLabels, wantLabels) {
		t.Fatalf("series labels = %v, want %v", gotLabels, wantLabels)
	}
	if len(series.Chunks) != 1 {
		t.Fatalf("len(series.Chunks) = %d, want 1", len(series.Chunks))
	}
}

func TestSeriesSortsByFinalLabels(t *testing.T) {
	server := newQueryServer(fakeQueryBackendAPI{
		queryRangeValue: model.Matrix{
			&model.SampleStream{
				Metric: model.Metric{"__name__": "up", "job": "z"},
				Values: []model.SamplePair{
					{Timestamp: 1000, Value: 1},
				},
			},
			&model.SampleStream{
				Metric: model.Metric{"__name__": "up", "job": "a"},
				Values: []model.SamplePair{
					{Timestamp: 1000, Value: 1},
				},
			},
		},
	}, nil, nil)
	stream := &recordingStoreSeriesServer{ctx: context.Background()}

	err := server.Series(&storepb.SeriesRequest{
		MinTime: 0,
		MaxTime: 2000,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"},
		},
	}, stream)
	if err != nil {
		t.Fatalf("Series() returned error: %v", err)
	}
	if len(stream.responses) != 2 {
		t.Fatalf("len(responses) = %d, want 2", len(stream.responses))
	}

	got := []string{
		labelpb.ZLabelsToPromLabels(stream.responses[0].GetSeries().Labels).Get("job"),
		labelpb.ZLabelsToPromLabels(stream.responses[1].GetSeries().Labels).Get("job"),
	}
	want := []string{"a", "z"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("response order = %v, want %v", got, want)
	}
}

func TestSeriesMetadataSortsByFinalLabels(t *testing.T) {
	server := newQueryServer(fakeQueryBackendAPI{
		seriesLabelSets: []model.LabelSet{
			{"__name__": "up", "job": "z"},
			{"__name__": "up", "job": "a"},
		},
	}, nil, nil)
	stream := &recordingStoreSeriesServer{ctx: context.Background()}

	err := server.Series(&storepb.SeriesRequest{
		SkipChunks: true,
		MinTime:    0,
		MaxTime:    2000,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"},
		},
	}, stream)
	if err != nil {
		t.Fatalf("Series() returned error: %v", err)
	}
	if len(stream.responses) != 2 {
		t.Fatalf("len(responses) = %d, want 2", len(stream.responses))
	}

	got := []string{
		labelpb.ZLabelsToPromLabels(stream.responses[0].GetSeries().Labels).Get("job"),
		labelpb.ZLabelsToPromLabels(stream.responses[1].GetSeries().Labels).Get("job"),
	}
	want := []string{"a", "z"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("response order = %v, want %v", got, want)
	}
}

func TestLabelDropSetFiltersLabelsButKeepsPrometheusLabel(t *testing.T) {
	dropLabels := newLabelDropSet([]string{"__tenant_id__"})

	got := dropLabels.zLabelsFromMetric(model.Metric{
		"__name__":      "up",
		"__tenant_id__": "grazie",
		"prometheus":    "grazie-prometheus",
	}, labels.EmptyLabels(), nil)

	labelMap := labelpb.ZLabelsToPromLabels(got).Map()
	want := map[string]string{
		"__name__":   "up",
		"prometheus": "grazie-prometheus",
	}
	if !reflect.DeepEqual(labelMap, want) {
		t.Fatalf("zLabelsFromMetric() = %v, want %v", labelMap, want)
	}
}

func TestLabelDropSetFiltersLabelNames(t *testing.T) {
	dropLabels := newLabelDropSet([]string{"__tenant_id__"})

	got := dropLabels.filterNames([]string{"__name__", "__tenant_id__", "prometheus"})
	want := []string{"__name__", "prometheus"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("filterNames() = %v, want %v", got, want)
	}
}

func TestExternalLabelsFromConfigYAML(t *testing.T) {
	got, err := externalLabelsFromConfigYAML(`
global:
  scrape_interval: 1m
  external_labels:
    prometheus: tenant-prometheus
    region: eu
`)
	if err != nil {
		t.Fatalf("externalLabelsFromConfigYAML() returned error: %v", err)
	}

	want := map[string]string{"prometheus": "tenant-prometheus", "region": "eu"}
	if gotMap := got.Map(); !reflect.DeepEqual(gotMap, want) {
		t.Fatalf("externalLabelsFromConfigYAML() = %v, want %v", gotMap, want)
	}
}

func TestAnnouncedLabelSetsFromValues(t *testing.T) {
	got := announcedLabelSetsFromValues("prometheus", model.LabelValues{
		"prom-b",
		"",
		"prom-a",
		"prom-a",
	})

	if len(got) != 2 {
		t.Fatalf("len(announcedLabelSetsFromValues()) = %d, want 2", len(got))
	}
	if got[0].Get("prometheus") != "prom-a" || got[1].Get("prometheus") != "prom-b" {
		t.Fatalf("announcedLabelSetsFromValues() = %v, want prom-a then prom-b", got)
	}
}

func TestAnnouncedLabelSourceConfigsSplitTenantHeader(t *testing.T) {
	cfg := queryBackendConfig{
		QueryTargetURL: "http://mimir-querier:8080/prometheus",
		Headers: map[string]string{
			"Authorization": "Bearer token",
			"x-scope-orgid": "tenant-a| tenant-b ||",
		},
		Auth: queryBackendAuthConfig{
			CredentialsFile: "/key.json",
			Scopes:          []string{"scope-a"},
		},
	}

	got := announcedLabelSourceConfigs(cfg)

	if len(got) != 2 {
		t.Fatalf("len(announcedLabelSourceConfigs()) = %d, want 2", len(got))
	}
	wantNames := []string{"tenant-a", "tenant-b"}
	for i, wantName := range wantNames {
		if got[i].name != wantName {
			t.Fatalf("source[%d].name = %q, want %q", i, got[i].name, wantName)
		}
		if got[i].config.QueryTargetURL != cfg.QueryTargetURL {
			t.Fatalf("source[%d].QueryTargetURL = %q, want %q", i, got[i].config.QueryTargetURL, cfg.QueryTargetURL)
		}
		if got[i].config.Headers["x-scope-orgid"] != wantName {
			t.Fatalf("source[%d] x-scope-orgid = %q, want %q", i, got[i].config.Headers["x-scope-orgid"], wantName)
		}
		if got[i].config.Headers["Authorization"] != "Bearer token" {
			t.Fatalf("source[%d] Authorization = %q, want Bearer token", i, got[i].config.Headers["Authorization"])
		}
		if !reflect.DeepEqual(got[i].config.Auth, cfg.Auth) {
			t.Fatalf("source[%d].Auth = %v, want %v", i, got[i].config.Auth, cfg.Auth)
		}
	}
	if cfg.Headers["x-scope-orgid"] != "tenant-a| tenant-b ||" {
		t.Fatalf("original header mutated to %q", cfg.Headers["x-scope-orgid"])
	}
}

func TestAnnouncedLabelSetsUpdateFromSourcesSkipsFailedSources(t *testing.T) {
	store := newAnnouncedLabelSetsStore()
	sources := []announcedLabelSource{
		{
			name: "tenant-a",
			client: fakeQueryBackendAPI{labelValues: map[string]model.LabelValues{
				"prometheus": {"prom-a"},
			}},
		},
		{
			name:   "tenant-b",
			client: fakeQueryBackendAPI{err: errors.New("empty ring")},
		},
	}

	failures, err := store.UpdateFromSources(context.Background(), sources, []string{"prometheus"}, 0, 0)

	if err != nil {
		t.Fatalf("UpdateFromSources() returned error: %v", err)
	}
	if len(failures) != 1 || failures[0].source != "tenant-b" {
		t.Fatalf("failures = %v, want tenant-b failure", failures)
	}
	got := store.LabelSets()
	if len(got) != 1 || got[0].Get("prometheus") != "prom-a" {
		t.Fatalf("LabelSets() = %v, want prometheus=prom-a", got)
	}
}

func TestAnnouncedLabelSetsUpdateFromSourcesKeepsExistingLabelsWhenAllSourcesFail(t *testing.T) {
	store := newAnnouncedLabelSetsStore()
	_, err := store.UpdateFromSources(context.Background(), []announcedLabelSource{
		{
			name: "tenant-a",
			client: fakeQueryBackendAPI{labelValues: map[string]model.LabelValues{
				"prometheus": {"prom-a"},
			}},
		},
	}, []string{"prometheus"}, 0, 0)
	if err != nil {
		t.Fatalf("initial UpdateFromSources() returned error: %v", err)
	}

	failures, err := store.UpdateFromSources(context.Background(), []announcedLabelSource{
		{
			name:   "tenant-a",
			client: fakeQueryBackendAPI{err: errors.New("empty ring")},
		},
	}, []string{"prometheus"}, 0, 0)

	if err == nil {
		t.Fatal("UpdateFromSources() succeeded, want error")
	}
	if len(failures) != 1 || failures[0].source != "tenant-a" {
		t.Fatalf("failures = %v, want tenant-a failure", failures)
	}
	got := store.LabelSets()
	if len(got) != 1 || got[0].Get("prometheus") != "prom-a" {
		t.Fatalf("LabelSets() = %v, want previous prometheus=prom-a", got)
	}
}

func TestAnnouncedLabelSetsUpdateFromSourcesUsesLookback(t *testing.T) {
	store := newAnnouncedLabelSetsStore()
	calls := make([]fakeLabelValuesCall, 0, 1)
	lookback := 6 * time.Hour
	before := time.Now().UTC()

	failures, err := store.UpdateFromSources(context.Background(), []announcedLabelSource{
		{
			name: "tenant-a",
			client: fakeQueryBackendAPI{
				labelValues: map[string]model.LabelValues{
					"prometheus": {"prom-a"},
				},
				labelValuesCalls: &calls,
			},
		},
	}, []string{"prometheus"}, 0, lookback)
	after := time.Now().UTC()

	if err != nil {
		t.Fatalf("UpdateFromSources() returned error: %v", err)
	}
	if len(failures) != 0 {
		t.Fatalf("failures = %v, want none", failures)
	}
	if len(calls) != 1 {
		t.Fatalf("len(label values calls) = %d, want 1", len(calls))
	}
	call := calls[0]
	if call.startTime.IsZero() || call.endTime.IsZero() {
		t.Fatalf("LabelValues() start/end = %s/%s, want bounded range", call.startTime, call.endTime)
	}
	if call.endTime.Before(before) || call.endTime.After(after) {
		t.Fatalf("LabelValues() end = %s, want between %s and %s", call.endTime, before, after)
	}
	if !call.startTime.Equal(call.endTime.Add(-lookback)) {
		t.Fatalf("LabelValues() range = %s, want %s", call.endTime.Sub(call.startTime), lookback)
	}
}

func TestAnnouncedLabelSetsUpdateFromSourcesRejectsNegativeLookback(t *testing.T) {
	store := newAnnouncedLabelSetsStore()

	_, err := store.UpdateFromSources(context.Background(), []announcedLabelSource{
		{
			name:   "tenant-a",
			client: fakeQueryBackendAPI{},
		},
	}, []string{"prometheus"}, 0, -time.Second)

	if err == nil {
		t.Fatal("UpdateFromSources() succeeded with negative lookback, want error")
	}
}

func TestInfoServerUsesAnnouncedLabelSets(t *testing.T) {
	info := &infoServer{
		queryBackend: "http://backend.example",
		externalLabels: func() labels.Labels {
			return labels.FromStrings("region", "global")
		},
		announcedLabelSets: func() []labels.Labels {
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
		got = append(got, labelpb.ZLabelsToPromLabels(labelSet.Labels).Map())
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
		gotTSDBs = append(gotTSDBs, labelpb.ZLabelsToPromLabels(tsdbInfo.Labels.Labels).Map())
	}
	if !reflect.DeepEqual(gotTSDBs, want) {
		t.Fatalf("Info().Store.TsdbInfos labels = %v, want %v", gotTSDBs, want)
	}
}

func TestInfoServerCanAdvertiseQueryAPI(t *testing.T) {
	info := &infoServer{
		queryBackend: "http://backend.example",
		apiMode:      infoAPIModeBoth,
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
	info := &infoServer{
		queryBackend: "http://backend.example",
		apiMode:      infoAPIModeQuery,
		announcedLabelSets: func() []labels.Labels {
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
	if got := labelpb.ZLabelsToPromLabels(resp.LabelSets[0].Labels).Map(); !reflect.DeepEqual(got, map[string]string{"prometheus": "prom-a"}) {
		t.Fatalf("Info().LabelSets[0] = %v, want prometheus label set", got)
	}
}

func TestParseInfoAPIMode(t *testing.T) {
	for _, testCase := range []struct {
		name              string
		value             string
		advertiseQueryAPI bool
		want              infoAPIMode
		wantErr           bool
	}{
		{name: "default store", value: "", want: infoAPIModeStore},
		{name: "store", value: "store", want: infoAPIModeStore},
		{name: "query", value: "query", want: infoAPIModeQuery},
		{name: "both", value: "both", want: infoAPIModeBoth},
		{name: "legacy advertise query", value: "store", advertiseQueryAPI: true, want: infoAPIModeBoth},
		{name: "invalid", value: "bad", wantErr: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := parseInfoAPIMode(testCase.value, testCase.advertiseQueryAPI)
			if testCase.wantErr {
				if err == nil {
					t.Fatal("parseInfoAPIMode() succeeded, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseInfoAPIMode() returned error: %v", err)
			}
			if got != testCase.want {
				t.Fatalf("parseInfoAPIMode() = %q, want %q", got, testCase.want)
			}
		})
	}
}

func TestMatchesExternalLabelsFiltersMatchingExternalMatchers(t *testing.T) {
	match, got, err := matchesExternalLabels(
		[]storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "prometheus", Value: "tenant-prometheus"},
			{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"},
		},
		labels.FromStrings("prometheus", "tenant-prometheus"),
	)
	if err != nil {
		t.Fatalf("matchesExternalLabels() returned error: %v", err)
	}
	if !match {
		t.Fatal("matchesExternalLabels() match = false, want true")
	}
	if len(got) != 1 || got[0].Name != "__name__" || got[0].Value != "up" {
		t.Fatalf("matchesExternalLabels() matchers = %v, want only __name__ matcher", got)
	}
}

func TestMatchesExternalLabelsRejectsMismatchedExternalMatchers(t *testing.T) {
	match, got, err := matchesExternalLabels(
		[]storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "prometheus", Value: "other"}},
		labels.FromStrings("prometheus", "tenant-prometheus"),
	)
	if err != nil {
		t.Fatalf("matchesExternalLabels() returned error: %v", err)
	}
	if match {
		t.Fatal("matchesExternalLabels() match = true, want false")
	}
	if got != nil {
		t.Fatalf("matchesExternalLabels() matchers = %v, want nil", got)
	}
}

func TestLabelValuesReturnsExternalLabelWithBackendMatchers(t *testing.T) {
	server := newQueryServer(
		fakeQueryBackendAPI{err: errors.New("backend should not be queried")},
		nil,
		func() labels.Labels { return labels.FromStrings("prometheus", "gcp-itk8s-208609") },
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
	server := newQueryServerFromBackends([]queryBackendEndpoint{
		{
			name:           "itk8s-208609",
			client:         fakeQueryBackendAPI{err: errors.New("backend should not be queried")},
			externalLabels: staticExternalLabelsFunc(labels.FromStrings("prometheus", "gcp-itk8s-208609")),
		},
		{
			name:           "space-prod",
			client:         fakeQueryBackendAPI{err: errors.New("backend should not be queried")},
			externalLabels: staticExternalLabelsFunc(labels.FromStrings("prometheus", "gcp-space-prod")),
		},
	}, nil)

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
	server := newQueryServerFromBackends([]queryBackendEndpoint{
		{
			name:           "itk8s-208609",
			client:         fakeQueryBackendAPI{err: errors.New("backend should not be queried")},
			externalLabels: staticExternalLabelsFunc(labels.FromStrings("prometheus", "gcp-itk8s-208609")),
		},
		{
			name:           "space-prod",
			client:         fakeQueryBackendAPI{err: errors.New("backend should not be queried")},
			externalLabels: staticExternalLabelsFunc(labels.FromStrings("prometheus", "gcp-space-prod")),
		},
	}, nil)

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
	server := newQueryServerFromBackends([]queryBackendEndpoint{
		{
			name: "itk8s-208609",
			client: fakeQueryBackendAPI{
				labelValues: map[string]model.LabelValues{
					"monitored_resource": {"gce_backend_service"},
				},
				labelValuesCalls: &itk8sCalls,
			},
			externalLabels: staticExternalLabelsFunc(labels.FromStrings("prometheus", "gcp-itk8s-208609")),
		},
		{
			name: "space-prod",
			client: fakeQueryBackendAPI{
				labelValues: map[string]model.LabelValues{
					"monitored_resource": {"k8s_container"},
				},
				labelValuesCalls: &spaceCalls,
			},
			externalLabels: staticExternalLabelsFunc(labels.FromStrings("prometheus", "gcp-space-prod")),
		},
	}, nil)

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
		t.Fatalf("LabelValues() backend selector = %q, want backend matchers preserved", call.matches[0])
	}
}

func TestRewriteQueryForExternalLabelsStripsMatchingMatcher(t *testing.T) {
	got, match, err := rewriteQueryForExternalLabels(
		`irate(frr_bgp_peer_message_received_total{afi="ipv4",prometheus="gcp-project"}[1m])`,
		labels.FromStrings("prometheus", "gcp-project"),
	)
	if err != nil {
		t.Fatalf("rewriteQueryForExternalLabels() returned error: %v", err)
	}
	if !match {
		t.Fatal("rewriteQueryForExternalLabels() match = false, want true")
	}
	if strings.Contains(got, "prometheus") {
		t.Fatalf("rewriteQueryForExternalLabels() = %q, want prometheus matcher stripped", got)
	}
	if !strings.Contains(got, `afi="ipv4"`) {
		t.Fatalf("rewriteQueryForExternalLabels() = %q, want non-external matcher kept", got)
	}
}

func TestRewriteQueryForExternalLabelsRejectsMismatchedMatcher(t *testing.T) {
	_, match, err := rewriteQueryForExternalLabels(
		`up{prometheus="other"}`,
		labels.FromStrings("prometheus", "gcp-project"),
	)
	if err != nil {
		t.Fatalf("rewriteQueryForExternalLabels() returned error: %v", err)
	}
	if match {
		t.Fatal("rewriteQueryForExternalLabels() match = true, want false")
	}
}

func TestRewriteQueryForExternalLabelsKeepsSelectorValid(t *testing.T) {
	got, match, err := rewriteQueryForExternalLabels(
		`{prometheus="gcp-project"}`,
		labels.FromStrings("prometheus", "gcp-project"),
	)
	if err != nil {
		t.Fatalf("rewriteQueryForExternalLabels() returned error: %v", err)
	}
	if !match {
		t.Fatal("rewriteQueryForExternalLabels() match = false, want true")
	}
	if got != `{__name__=~".+"}` {
		t.Fatalf("rewriteQueryForExternalLabels() = %q, want all-series selector", got)
	}
}

func TestExternalLabelsOverrideResultLabels(t *testing.T) {
	got := labelDropSet(nil).zLabelsFromMetric(
		model.Metric{"__name__": "up", "prometheus": "from-result"},
		labels.FromStrings("prometheus", "from-external"),
		nil,
	)

	labelMap := labelpb.ZLabelsToPromLabels(got).Map()
	want := map[string]string{"__name__": "up", "prometheus": "from-external"}
	if !reflect.DeepEqual(labelMap, want) {
		t.Fatalf("zLabelsFromMetric() = %v, want %v", labelMap, want)
	}
}

type fakeQueryBackendAPI struct {
	labelValues      map[string]model.LabelValues
	labelValuesCalls *[]fakeLabelValuesCall
	seriesLabelSets  []model.LabelSet
	queryValue       model.Value
	queryRangeValue  model.Value
	err              error
}

type fakeLabelValuesCall struct {
	label     string
	matches   []string
	startTime time.Time
	endTime   time.Time
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
	if f.err != nil {
		return nil, nil, f.err
	}
	return f.queryValue, nil, nil
}

func (f fakeQueryBackendAPI) QueryRange(ctx context.Context, query string, r v1.Range, opts ...v1.Option) (model.Value, v1.Warnings, error) {
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

type recordingStoreSeriesServer struct {
	ctx       context.Context
	responses []*storepb.SeriesResponse
}

func (s *recordingStoreSeriesServer) Send(response *storepb.SeriesResponse) error {
	s.responses = append(s.responses, response)
	return nil
}

func (s *recordingStoreSeriesServer) SetHeader(metadata.MD) error {
	return nil
}

func (s *recordingStoreSeriesServer) SendHeader(metadata.MD) error {
	return nil
}

func (s *recordingStoreSeriesServer) SetTrailer(metadata.MD) {}

func (s *recordingStoreSeriesServer) Context() context.Context {
	if s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

func (s *recordingStoreSeriesServer) SendMsg(any) error {
	return nil
}

func (s *recordingStoreSeriesServer) RecvMsg(any) error {
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

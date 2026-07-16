package main

import (
	"context"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
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

func TestQueryAuthEnabled(t *testing.T) {
	tests := []struct {
		name string
		auth queryBackendAuthConfig
		want bool
	}{
		{name: "empty"},
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

func TestNewQueryBackendConfigRequiresTargetURL(t *testing.T) {
	if _, err := newQueryBackendConfig("", nil, "", nil); err == nil {
		t.Fatal("newQueryBackendConfig() succeeded without target URL, want error")
	}
}

func TestNewQueryBackendConfigBuildsConfigFromStartupParameters(t *testing.T) {
	cfg, err := newQueryBackendConfig(
		" http://127.0.0.1:18080/prometheus ",
		map[string]string{"X-Scope-OrgID": "tenant1|tenant2"},
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
	if cfg.Auth.CredentialsFile != "/key.json" {
		t.Fatalf("CredentialsFile = %q", cfg.Auth.CredentialsFile)
	}
	if got := cfg.Auth.Scopes; !reflect.DeepEqual(got, []string{"https://www.googleapis.com/auth/monitoring.read"}) {
		t.Fatalf("Scopes = %v", got)
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

func TestInfoServerUsesAnnouncedLabelSets(t *testing.T) {
	info := &infoServer{
		queryBackend: "http://backend.example",
		externalLabels: func() labels.Labels {
			return labels.FromStrings("prometheus", "from-external")
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

	got := make([]map[string]string, 0, len(resp.LabelSets))
	for _, labelSet := range resp.LabelSets {
		got = append(got, labelpb.ZLabelsToPromLabels(labelSet.Labels).Map())
	}
	want := []map[string]string{
		{"prometheus": "prom-a"},
		{"prometheus": "prom-b"},
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

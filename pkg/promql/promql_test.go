package promql

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func TestQuerySelectorFromMatchers(t *testing.T) {
	selector, err := QuerySelectorFromMatchers([]storepb.LabelMatcher{
		{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"},
		{Type: storepb.LabelMatcher_RE, Name: "job", Value: "api|worker"},
	})
	if err != nil {
		t.Fatalf("QuerySelectorFromMatchers() returned error: %v", err)
	}

	want := `{__name__="up", job=~"api|worker"}`
	if selector != want {
		t.Fatalf("QuerySelectorFromMatchers() = %q, want %q", selector, want)
	}
}

func TestQuerySelectorFromMatchersDefaultsToAllSeries(t *testing.T) {
	selector, err := QuerySelectorFromMatchers(nil)
	if err != nil {
		t.Fatalf("QuerySelectorFromMatchers() returned error: %v", err)
	}

	want := `{__name__=~".+"}`
	if selector != want {
		t.Fatalf("QuerySelectorFromMatchers() = %q, want %q", selector, want)
	}
}

func TestLabelAPISelectorsFromMatchers(t *testing.T) {
	selectors, err := LabelAPISelectorsFromMatchers([]storepb.LabelMatcher{
		{Type: storepb.LabelMatcher_EQ, Name: "job", Value: "api"},
	})
	if err != nil {
		t.Fatalf("LabelAPISelectorsFromMatchers() returned error: %v", err)
	}

	want := []string{`{job="api"}`}
	if !reflect.DeepEqual(selectors, want) {
		t.Fatalf("LabelAPISelectorsFromMatchers() = %v, want %v", selectors, want)
	}
}

func TestTimeFromMillis(t *testing.T) {
	got := TimeFromMillis(1710000000123)
	want := time.Unix(1710000000, 123*int64(time.Millisecond)).UTC()
	if !got.Equal(want) {
		t.Fatalf("TimeFromMillis() = %s, want %s", got, want)
	}

	zero := TimeFromMillis(0)
	if !zero.IsZero() {
		t.Fatalf("TimeFromMillis(0) = %s, want zero time", zero)
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
			if got := SeriesStep(tt.request, tt.fallback); got != tt.want {
				t.Fatalf("SeriesStep() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestSeriesStepForRangeClampsToMaxPoints(t *testing.T) {
	start := time.Unix(0, 0)
	end := start.Add(28 * 24 * time.Hour)

	got := SeriesStepForRange(
		&storepb.SeriesRequest{QueryHints: &storepb.QueryHints{StepMillis: int64((15 * time.Second) / time.Millisecond)}},
		time.Minute,
		start,
		end,
		11000,
	)

	want := minStepForMaxPoints(28*24*time.Hour, 11000)
	if got != want {
		t.Fatalf("SeriesStepForRange() = %s, want %s", got, want)
	}
	if got < 3*time.Minute {
		t.Fatalf("SeriesStepForRange() = %s, want clamped multi-minute step", got)
	}
}

func TestSeriesStepForRangeKeepsStepWhenUnderMaxPoints(t *testing.T) {
	start := time.Unix(0, 0)
	end := start.Add(time.Hour)

	got := SeriesStepForRange(&storepb.SeriesRequest{}, time.Minute, start, end, 11000)
	if got != time.Minute {
		t.Fatalf("SeriesStepForRange() = %s, want 1m", got)
	}
}

func TestSeriesStepForRangeCanDisableMaxPointsClamp(t *testing.T) {
	start := time.Unix(0, 0)
	end := start.Add(28 * 24 * time.Hour)

	got := SeriesStepForRange(&storepb.SeriesRequest{}, time.Minute, start, end, 0)
	if got != time.Minute {
		t.Fatalf("SeriesStepForRange() = %s, want unclamped 1m", got)
	}
}

func TestChunksFromModelSamples(t *testing.T) {
	chunks, err := ChunksFromModelSamples([]model.SamplePair{
		{Timestamp: 1000, Value: 1},
		{Timestamp: 2000, Value: 2},
	})
	if err != nil {
		t.Fatalf("ChunksFromModelSamples() returned error: %v", err)
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

func TestQueryStringFromRequestPlanPreservesSelectorFilters(t *testing.T) {
	queryPlan, err := querypb.NewJSONEncodedPlan(&logicalplan.VectorSelector{
		VectorSelector: &parser.VectorSelector{
			Name: "up",
			LabelMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up"),
				labels.MustNewMatcher(labels.MatchEqual, "job", "api"),
			},
		},
		Filters: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "instance", "server-1"),
		},
	})
	if err != nil {
		t.Fatalf("NewJSONEncodedPlan() returned error: %v", err)
	}

	got, err := QueryStringFromRequestPlan("", queryPlan)
	if err != nil {
		t.Fatalf("QueryStringFromRequestPlan() returned error: %v", err)
	}
	if !strings.Contains(got, `instance="server-1"`) {
		t.Fatalf("QueryStringFromRequestPlan() = %q, want selector filter preserved", got)
	}
}

func TestLabelDropSetFiltersLabelsButKeepsPrometheusLabel(t *testing.T) {
	dropLabels := NewLabelDropSet([]string{"__tenant_id__"})

	got := dropLabels.ZLabelsFromMetric(model.Metric{
		"__name__":      "up",
		"__tenant_id__": "tenant-a",
		"prometheus":    "tenant-a-prometheus",
	}, labels.EmptyLabels(), nil)

	labelMap := ZLabelsToPromLabels(got).Map()
	want := map[string]string{
		"__name__":   "up",
		"prometheus": "tenant-a-prometheus",
	}
	if !reflect.DeepEqual(labelMap, want) {
		t.Fatalf("ZLabelsFromMetric() = %v, want %v", labelMap, want)
	}
}

func TestLabelDropSetFiltersLabelNames(t *testing.T) {
	dropLabels := NewLabelDropSet([]string{"__tenant_id__"})

	got := dropLabels.FilterNames([]string{"__name__", "__tenant_id__", "prometheus"})
	want := []string{"__name__", "prometheus"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("FilterNames() = %v, want %v", got, want)
	}
}

func TestMatchesExternalLabelsFiltersMatchingExternalMatchers(t *testing.T) {
	match, got, err := MatchesExternalLabels(
		[]storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_EQ, Name: "prometheus", Value: "tenant-prometheus"},
			{Type: storepb.LabelMatcher_EQ, Name: "__name__", Value: "up"},
		},
		labels.FromStrings("prometheus", "tenant-prometheus"),
	)
	if err != nil {
		t.Fatalf("MatchesExternalLabels() returned error: %v", err)
	}
	if !match {
		t.Fatal("MatchesExternalLabels() match = false, want true")
	}
	if len(got) != 1 || got[0].Name != "__name__" || got[0].Value != "up" {
		t.Fatalf("MatchesExternalLabels() matchers = %v, want only __name__ matcher", got)
	}
}

func TestMatchesExternalLabelsRejectsMismatchedExternalMatchers(t *testing.T) {
	match, got, err := MatchesExternalLabels(
		[]storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "prometheus", Value: "other"}},
		labels.FromStrings("prometheus", "tenant-prometheus"),
	)
	if err != nil {
		t.Fatalf("MatchesExternalLabels() returned error: %v", err)
	}
	if match {
		t.Fatal("MatchesExternalLabels() match = true, want false")
	}
	if got != nil {
		t.Fatalf("MatchesExternalLabels() matchers = %v, want nil", got)
	}
}

func TestRewriteQueryForExternalLabelsStripsMatchingMatcher(t *testing.T) {
	got, match, err := RewriteQueryForExternalLabels(
		`irate(frr_bgp_peer_message_received_total{afi="ipv4",prometheus="gcp-project"}[1m])`,
		labels.FromStrings("prometheus", "gcp-project"),
	)
	if err != nil {
		t.Fatalf("RewriteQueryForExternalLabels() returned error: %v", err)
	}
	if !match {
		t.Fatal("RewriteQueryForExternalLabels() match = false, want true")
	}
	if strings.Contains(got, "prometheus") {
		t.Fatalf("RewriteQueryForExternalLabels() = %q, want prometheus matcher stripped", got)
	}
	if !strings.Contains(got, `afi="ipv4"`) {
		t.Fatalf("RewriteQueryForExternalLabels() = %q, want non-external matcher kept", got)
	}
}

func TestRewriteQueryForExternalLabelsRejectsMismatchedMatcher(t *testing.T) {
	_, match, err := RewriteQueryForExternalLabels(
		`up{prometheus="other"}`,
		labels.FromStrings("prometheus", "gcp-project"),
	)
	if err != nil {
		t.Fatalf("RewriteQueryForExternalLabels() returned error: %v", err)
	}
	if match {
		t.Fatal("RewriteQueryForExternalLabels() match = true, want false")
	}
}

func TestRewriteQueryForExternalLabelsKeepsSelectorValid(t *testing.T) {
	got, match, err := RewriteQueryForExternalLabels(
		`{prometheus="gcp-project"}`,
		labels.FromStrings("prometheus", "gcp-project"),
	)
	if err != nil {
		t.Fatalf("RewriteQueryForExternalLabels() returned error: %v", err)
	}
	if !match {
		t.Fatal("RewriteQueryForExternalLabels() match = false, want true")
	}
	if got != `{__name__=~".+"}` {
		t.Fatalf("RewriteQueryForExternalLabels() = %q, want all-series selector", got)
	}
}

func TestExternalLabelsOverrideResultLabels(t *testing.T) {
	got := LabelDropSet(nil).ZLabelsFromMetric(
		model.Metric{"__name__": "up", "prometheus": "from-result"},
		labels.FromStrings("prometheus", "from-external"),
		nil,
	)

	labelMap := ZLabelsToPromLabels(got).Map()
	want := map[string]string{"__name__": "up", "prometheus": "from-external"}
	if !reflect.DeepEqual(labelMap, want) {
		t.Fatalf("ZLabelsFromMetric() = %v, want %v", labelMap, want)
	}
}

func TestSampleHistogramToFloatHistogram(t *testing.T) {
	sh := &model.SampleHistogram{
		Count: model.FloatString(10),
		Sum:   model.FloatString(25.5),
		Buckets: model.HistogramBuckets{
			{Boundaries: 3, Lower: model.FloatString(-0.1), Upper: model.FloatString(0.1), Count: model.FloatString(2)},
			{Boundaries: 0, Lower: model.FloatString(1), Upper: model.FloatString(2), Count: model.FloatString(5)},
			{Boundaries: 0, Lower: model.FloatString(2), Upper: model.FloatString(4), Count: model.FloatString(3)},
		},
	}

	fh := SampleHistogramToFloatHistogram(sh)
	if fh == nil {
		t.Fatal("SampleHistogramToFloatHistogram() returned nil")
	}

	if fh.Count != 10 {
		t.Fatalf("fh.Count = %f, want 10", fh.Count)
	}
	if fh.Sum != 25.5 {
		t.Fatalf("fh.Sum = %f, want 25.5", fh.Sum)
	}
	if fh.ZeroCount != 2 {
		t.Fatalf("fh.ZeroCount = %f, want 2", fh.ZeroCount)
	}
	if fh.ZeroThreshold != 0.1 {
		t.Fatalf("fh.ZeroThreshold = %f, want 0.1", fh.ZeroThreshold)
	}
	if len(fh.PositiveBuckets) != 2 {
		t.Fatalf("len(fh.PositiveBuckets) = %d, want 2", len(fh.PositiveBuckets))
	}
}

func TestChunksFromModelHistogramSamples(t *testing.T) {
	shPair := model.SampleHistogramPair{
		Timestamp: 1000,
		Histogram: &model.SampleHistogram{
			Count: model.FloatString(5),
			Sum:   model.FloatString(10),
			Buckets: model.HistogramBuckets{
				{Boundaries: 0, Lower: model.FloatString(1), Upper: model.FloatString(2), Count: model.FloatString(5)},
			},
		},
	}

	chunks, err := ChunksFromModelHistogramSamples([]model.SampleHistogramPair{shPair})
	if err != nil {
		t.Fatalf("ChunksFromModelHistogramSamples() returned error: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("len(chunks) = %d, want 1", len(chunks))
	}

	chunk := chunks[0]
	if chunk.MinTime != 1000 || chunk.MaxTime != 1000 {
		t.Fatalf("chunk time range = [%d,%d], want [1000,1000]", chunk.MinTime, chunk.MaxTime)
	}
	if chunk.Raw == nil {
		t.Fatal("chunk.Raw is nil")
	}
	if chunk.Raw.Type != storepb.Chunk_FLOAT_HISTOGRAM {
		t.Fatalf("chunk.Raw.Type = %s, want FLOAT_HISTOGRAM", chunk.Raw.Type)
	}
}

func TestSampleHistogramToFloatHistogramNegativeBuckets(t *testing.T) {
	sh := &model.SampleHistogram{
		Count: model.FloatString(5),
		Sum:   model.FloatString(-7.5),
		Buckets: model.HistogramBuckets{
			{Boundaries: 0, Lower: model.FloatString(-2), Upper: model.FloatString(-1), Count: model.FloatString(5)},
		},
	}

	fh := SampleHistogramToFloatHistogram(sh)
	if fh == nil {
		t.Fatal("SampleHistogramToFloatHistogram() returned nil")
	}
	if fh.Count != 5 {
		t.Fatalf("fh.Count = %f, want 5", fh.Count)
	}
	if fh.Sum != -7.5 {
		t.Fatalf("fh.Sum = %f, want -7.5", fh.Sum)
	}
	if len(fh.NegativeBuckets) != 1 {
		t.Fatalf("len(fh.NegativeBuckets) = %d, want 1", len(fh.NegativeBuckets))
	}
	if fh.Schema != 0 {
		t.Fatalf("fh.Schema = %d, want 0", fh.Schema)
	}
}

func TestChunksFromModelHistogramSamplesMultipleSamples(t *testing.T) {
	var samples []model.SampleHistogramPair
	for i := 0; i < 30; i++ {
		samples = append(samples, model.SampleHistogramPair{
			Timestamp: model.Time(1000 + i*60000),
			Histogram: &model.SampleHistogram{
				Count: model.FloatString(float64(5 + i)),
				Sum:   model.FloatString(float64(10 + i*2)),
				Buckets: model.HistogramBuckets{
					{Boundaries: 0, Lower: model.FloatString(1), Upper: model.FloatString(2), Count: model.FloatString(float64(5 + i))},
				},
			},
		})
	}

	chunks, err := ChunksFromModelHistogramSamples(samples)
	if err != nil {
		t.Fatalf("ChunksFromModelHistogramSamples() returned error: %v", err)
	}
	if len(chunks) == 0 {
		t.Fatal("ChunksFromModelHistogramSamples() returned 0 chunks")
	}

	totalSamplesRead := 0
	for _, chunk := range chunks {
		fhChunk, err := chunkenc.FromData(chunkenc.EncFloatHistogram, chunk.Raw.Data)
		if err != nil {
			t.Fatalf("Failed to parse FloatHistogramChunk from chunk data: %v", err)
		}
		it := fhChunk.Iterator(nil)
		for it.Next() == chunkenc.ValFloatHistogram {
			totalSamplesRead++
		}
		if it.Err() != nil {
			t.Fatalf("Chunk iterator error: %v", it.Err())
		}
	}

	if totalSamplesRead != 30 {
		t.Fatalf("totalSamplesRead = %d, want 30", totalSamplesRead)
	}
}
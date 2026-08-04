package labelstore

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"

	"main.go/pkg/config"
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

func TestExternalLabelsFromConfigYAML(t *testing.T) {
	got, err := ExternalLabelsFromConfigYAML(`
global:
  scrape_interval: 1m
  external_labels:
    prometheus: tenant-prometheus
    region: eu
`)
	if err != nil {
		t.Fatalf("ExternalLabelsFromConfigYAML() returned error: %v", err)
	}

	want := map[string]string{"prometheus": "tenant-prometheus", "region": "eu"}
	if gotMap := got.Map(); !reflect.DeepEqual(gotMap, want) {
		t.Fatalf("ExternalLabelsFromConfigYAML() = %v, want %v", gotMap, want)
	}
}

func TestAnnouncedLabelSetsFromValues(t *testing.T) {
	got := AnnouncedLabelSetsFromValues("prometheus", model.LabelValues{
		"prom-b",
		"",
		"prom-a",
		"prom-a",
	})

	if len(got) != 2 {
		t.Fatalf("len(AnnouncedLabelSetsFromValues()) = %d, want 2", len(got))
	}
	if got[0].Get("prometheus") != "prom-a" || got[1].Get("prometheus") != "prom-b" {
		t.Fatalf("AnnouncedLabelSetsFromValues() = %v, want prom-a then prom-b", got)
	}
}

func TestAnnouncedLabelSourceConfigsSplitTenantHeader(t *testing.T) {
	cfg := config.QueryBackendConfig{
		QueryTargetURL: "http://mimir-querier:8080/prometheus",
		Headers: map[string]string{
			"Authorization": "Bearer token",
			"x-scope-orgid": "tenant-a| tenant-b ||",
		},
		Auth: config.QueryBackendAuthConfig{
			CredentialsFile: "/key.json",
			Scopes:          []string{"scope-a"},
		},
	}

	got := AnnouncedLabelSourceConfigs(cfg)

	if len(got) != 2 {
		t.Fatalf("len(AnnouncedLabelSourceConfigs()) = %d, want 2", len(got))
	}
	wantNames := []string{"tenant-a", "tenant-b"}
	for i, wantName := range wantNames {
		if got[i].Name != wantName {
			t.Fatalf("source[%d].Name = %q, want %q", i, got[i].Name, wantName)
		}
		if got[i].Config.QueryTargetURL != cfg.QueryTargetURL {
			t.Fatalf("source[%d].QueryTargetURL = %q, want %q", i, got[i].Config.QueryTargetURL, cfg.QueryTargetURL)
		}
		if got[i].Config.Headers["x-scope-orgid"] != wantName {
			t.Fatalf("source[%d] x-scope-orgid = %q, want %q", i, got[i].Config.Headers["x-scope-orgid"], wantName)
		}
		if got[i].Config.Headers["Authorization"] != "Bearer token" {
			t.Fatalf("source[%d] Authorization = %q, want Bearer token", i, got[i].Config.Headers["Authorization"])
		}
		if !reflect.DeepEqual(got[i].Config.Auth, cfg.Auth) {
			t.Fatalf("source[%d].Auth = %v, want %v", i, got[i].Config.Auth, cfg.Auth)
		}
	}
	if cfg.Headers["x-scope-orgid"] != "tenant-a| tenant-b ||" {
		t.Fatalf("original header mutated to %q", cfg.Headers["x-scope-orgid"])
	}
}

func TestAnnouncedLabelSetsUpdateFromSourcesSkipsFailedSources(t *testing.T) {
	store := NewAnnouncedLabelSetsStore()
	sources := []AnnouncedLabelSource{
		{
			Name: "tenant-a",
			Client: fakeQueryBackendAPI{labelValues: map[string]model.LabelValues{
				"prometheus": {"prom-a"},
			}},
		},
		{
			Name:   "tenant-b",
			Client: fakeQueryBackendAPI{err: errors.New("empty ring")},
		},
	}

	failures, err := store.UpdateFromSources(context.Background(), sources, []string{"prometheus"}, 0, 0)

	if err != nil {
		t.Fatalf("UpdateFromSources() returned error: %v", err)
	}
	if len(failures) != 1 || failures[0].Source != "tenant-b" {
		t.Fatalf("failures = %v, want tenant-b failure", failures)
	}
	got := store.LabelSets()
	if len(got) != 1 || got[0].Get("prometheus") != "prom-a" {
		t.Fatalf("LabelSets() = %v, want prometheus=prom-a", got)
	}
}

func TestAnnouncedLabelSetsUpdateFromSourcesKeepsExistingLabelsWhenAllSourcesFail(t *testing.T) {
	store := NewAnnouncedLabelSetsStore()
	_, err := store.UpdateFromSources(context.Background(), []AnnouncedLabelSource{
		{
			Name: "tenant-a",
			Client: fakeQueryBackendAPI{labelValues: map[string]model.LabelValues{
				"prometheus": {"prom-a"},
			}},
		},
	}, []string{"prometheus"}, 0, 0)
	if err != nil {
		t.Fatalf("initial UpdateFromSources() returned error: %v", err)
	}

	failures, err := store.UpdateFromSources(context.Background(), []AnnouncedLabelSource{
		{
			Name:   "tenant-a",
			Client: fakeQueryBackendAPI{err: errors.New("empty ring")},
		},
	}, []string{"prometheus"}, 0, 0)

	if err == nil {
		t.Fatal("UpdateFromSources() succeeded, want error")
	}
	if len(failures) != 1 || failures[0].Source != "tenant-a" {
		t.Fatalf("failures = %v, want tenant-a failure", failures)
	}
	got := store.LabelSets()
	if len(got) != 1 || got[0].Get("prometheus") != "prom-a" {
		t.Fatalf("LabelSets() = %v, want previous prometheus=prom-a", got)
	}
}

func TestAnnouncedLabelSetsUpdateFromSourcesUsesLookback(t *testing.T) {
	store := NewAnnouncedLabelSetsStore()
	calls := make([]fakeLabelValuesCall, 0, 1)
	lookback := 6 * time.Hour
	before := time.Now().UTC()

	failures, err := store.UpdateFromSources(context.Background(), []AnnouncedLabelSource{
		{
			Name: "tenant-a",
			Client: fakeQueryBackendAPI{
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
	store := NewAnnouncedLabelSetsStore()

	_, err := store.UpdateFromSources(context.Background(), []AnnouncedLabelSource{
		{
			Name:   "tenant-a",
			Client: fakeQueryBackendAPI{},
		},
	}, []string{"prometheus"}, 0, -time.Second)

	if err == nil {
		t.Fatal("UpdateFromSources() succeeded with negative lookback, want error")
	}
}
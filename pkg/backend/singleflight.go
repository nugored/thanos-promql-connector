package backend

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"golang.org/x/sync/singleflight"
)

type SingleflightClient struct {
	client QueryBackendAPI
	group  singleflight.Group
}

func NewSingleflightClient(client QueryBackendAPI) QueryBackendAPI {
	if client == nil {
		return nil
	}
	return &SingleflightClient{
		client: client,
	}
}

func (s *SingleflightClient) Config(ctx context.Context) (v1.ConfigResult, error) {
	key := "Config"
	v, err, _ := s.group.Do(key, func() (interface{}, error) {
		return s.client.Config(ctx)
	})
	if err != nil {
		return v1.ConfigResult{}, err
	}
	return v.(v1.ConfigResult), nil
}

func (s *SingleflightClient) LabelNames(ctx context.Context, matches []string, startTime, endTime time.Time, opts ...v1.Option) ([]string, v1.Warnings, error) {
	key := fmt.Sprintf("LabelNames|%s|%d|%d", formatMatches(matches), startTime.UnixMilli(), endTime.UnixMilli())
	type result struct {
		names    []string
		warnings v1.Warnings
	}
	v, err, _ := s.group.Do(key, func() (interface{}, error) {
		names, warnings, err := s.client.LabelNames(ctx, matches, startTime, endTime, opts...)
		if err != nil {
			return nil, err
		}
		return result{names: names, warnings: warnings}, nil
	})
	if err != nil {
		return nil, nil, err
	}
	res := v.(result)
	return cloneStrings(res.names), cloneWarnings(res.warnings), nil
}

func (s *SingleflightClient) LabelValues(ctx context.Context, label string, matches []string, startTime, endTime time.Time, opts ...v1.Option) (model.LabelValues, v1.Warnings, error) {
	key := fmt.Sprintf("LabelValues|%s|%s|%d|%d", label, formatMatches(matches), startTime.UnixMilli(), endTime.UnixMilli())
	type result struct {
		values   model.LabelValues
		warnings v1.Warnings
	}
	v, err, _ := s.group.Do(key, func() (interface{}, error) {
		values, warnings, err := s.client.LabelValues(ctx, label, matches, startTime, endTime, opts...)
		if err != nil {
			return nil, err
		}
		return result{values: values, warnings: warnings}, nil
	})
	if err != nil {
		return nil, nil, err
	}
	res := v.(result)
	return cloneLabelValues(res.values), cloneWarnings(res.warnings), nil
}

func (s *SingleflightClient) Query(ctx context.Context, query string, ts time.Time, opts ...v1.Option) (model.Value, v1.Warnings, error) {
	key := fmt.Sprintf("Query|%s|%d", query, ts.UnixMilli())
	type result struct {
		val      model.Value
		warnings v1.Warnings
	}
	v, err, _ := s.group.Do(key, func() (interface{}, error) {
		val, warnings, err := s.client.Query(ctx, query, ts, opts...)
		if err != nil {
			return nil, err
		}
		return result{val: val, warnings: warnings}, nil
	})
	if err != nil {
		return nil, nil, err
	}
	res := v.(result)
	return res.val, cloneWarnings(res.warnings), nil
}

func (s *SingleflightClient) QueryRange(ctx context.Context, query string, r v1.Range, opts ...v1.Option) (model.Value, v1.Warnings, error) {
	key := fmt.Sprintf("QueryRange|%s|%d|%d|%d", query, r.Start.UnixMilli(), r.End.UnixMilli(), r.Step.Milliseconds())
	type result struct {
		val      model.Value
		warnings v1.Warnings
	}
	v, err, _ := s.group.Do(key, func() (interface{}, error) {
		val, warnings, err := s.client.QueryRange(ctx, query, r, opts...)
		if err != nil {
			return nil, err
		}
		return result{val: val, warnings: warnings}, nil
	})
	if err != nil {
		return nil, nil, err
	}
	res := v.(result)
	return res.val, cloneWarnings(res.warnings), nil
}

func (s *SingleflightClient) Series(ctx context.Context, matches []string, startTime, endTime time.Time, opts ...v1.Option) ([]model.LabelSet, v1.Warnings, error) {
	key := fmt.Sprintf("Series|%s|%d|%d", formatMatches(matches), startTime.UnixMilli(), endTime.UnixMilli())
	type result struct {
		labelSets []model.LabelSet
		warnings  v1.Warnings
	}
	v, err, _ := s.group.Do(key, func() (interface{}, error) {
		labelSets, warnings, err := s.client.Series(ctx, matches, startTime, endTime, opts...)
		if err != nil {
			return nil, err
		}
		return result{labelSets: labelSets, warnings: warnings}, nil
	})
	if err != nil {
		return nil, nil, err
	}
	res := v.(result)
	return cloneLabelSets(res.labelSets), cloneWarnings(res.warnings), nil
}

func formatMatches(matches []string) string {
	if len(matches) == 0 {
		return ""
	}
	sorted := append([]string(nil), matches...)
	sort.Strings(sorted)
	return strings.Join(sorted, "\x00")
}

func cloneStrings(s []string) []string {
	if s == nil {
		return nil
	}
	res := make([]string, len(s))
	copy(res, s)
	return res
}

func cloneWarnings(w v1.Warnings) v1.Warnings {
	if w == nil {
		return nil
	}
	res := make(v1.Warnings, len(w))
	copy(res, w)
	return res
}

func cloneLabelValues(v model.LabelValues) model.LabelValues {
	if v == nil {
		return nil
	}
	res := make(model.LabelValues, len(v))
	copy(res, v)
	return res
}

func cloneLabelSets(l []model.LabelSet) []model.LabelSet {
	if l == nil {
		return nil
	}
	res := make([]model.LabelSet, len(l))
	for i, ls := range l {
		res[i] = ls.Clone()
	}
	return res
}

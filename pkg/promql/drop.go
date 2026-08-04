package promql

import (
	"sort"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
)

type LabelDropSet map[string]struct{}

func NewLabelDropSet(lbls []string) LabelDropSet {
	if len(lbls) == 0 {
		return nil
	}

	result := make(LabelDropSet, len(lbls))
	for _, label := range lbls {
		if label == "" {
			continue
		}
		result[label] = struct{}{}
	}
	return result
}

func (s LabelDropSet) Has(name string) bool {
	_, ok := s[name]
	return ok
}

func (s LabelDropSet) FilterNames(names []string) []string {
	if len(s) == 0 {
		return names
	}

	filtered := make([]string, 0, len(names))
	for _, name := range names {
		if !s.Has(name) {
			filtered = append(filtered, name)
		}
	}
	return filtered
}

func (s LabelDropSet) LabelNames(names []string, externalLabels labels.Labels, withoutLabels []string) []string {
	remove := s.With(withoutLabels)
	nameSet := make(map[string]struct{}, len(names)+externalLabels.Len())
	for _, name := range names {
		if !remove.Has(name) {
			nameSet[name] = struct{}{}
		}
	}
	if len(names) > 0 {
		externalLabels.Range(func(label labels.Label) {
			if !remove.Has(label.Name) {
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

func (s LabelDropSet) With(names []string) LabelDropSet {
	if len(names) == 0 {
		return s
	}
	result := make(LabelDropSet, len(s)+len(names))
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

// ZLabelsFromMetric converts model.Metric to labelpb.ZLabel.
func (s LabelDropSet) ZLabelsFromMetric(metric model.Metric, externalLabels labels.Labels, withoutLabels []string) []labelpb.ZLabel {
	labelSet := make(model.LabelSet, len(metric))
	for name, value := range metric {
		labelSet[name] = value
	}
	return s.ZLabelsFromLabelSet(labelSet, externalLabels, withoutLabels)
}

func (s LabelDropSet) ZLabelsFromLabelSet(labelSet model.LabelSet, externalLabels labels.Labels, withoutLabels []string) []labelpb.ZLabel {
	remove := s.With(withoutLabels)
	values := make(map[string]string, len(labelSet))
	for name, value := range labelSet {
		if remove.Has(string(name)) {
			continue
		}
		values[string(name)] = string(value)
	}
	merged := labelpb.ExtendSortedLabels(labels.FromMap(values), externalLabels)
	if len(remove) == 0 {
		return ZLabelsFromPromLabels(merged)
	}

	filtered := make(map[string]string, merged.Len())
	merged.Range(func(label labels.Label) {
		if !remove.Has(label.Name) {
			filtered[label.Name] = label.Value
		}
	})
	return ZLabelsFromPromLabels(labels.FromMap(filtered))
}
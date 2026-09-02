package promql

import (
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func LabelAPISelectorsFromMatchers(matchers []storepb.LabelMatcher) ([]string, error) {
	if len(matchers) == 0 {
		return []string{`{__name__=~".+"}`}, nil
	}
	selector, err := QuerySelectorFromMatchers(matchers)
	if err != nil {
		return nil, err
	}
	return []string{selector}, nil
}

func LabelAPISelectorsFromPromMatchers(matchers []*labels.Matcher) []string {
	if len(matchers) == 0 {
		return []string{`{__name__=~".+"}`}
	}
	return []string{QuerySelectorFromPromMatchers(matchers)}
}

func QuerySelectorFromMatchers(matchers []storepb.LabelMatcher) (string, error) {
	if len(matchers) == 0 {
		return `{__name__=~".+"}`, nil
	}
	promMatchers, err := storepb.MatchersToPromMatchers(matchers...)
	if err != nil {
		return "", err
	}
	return storepb.PromMatchersToString(promMatchers...), nil
}

func QuerySelectorFromPromMatchers(matchers []*labels.Matcher) string {
	if len(matchers) == 0 {
		return `{__name__=~".+"}`
	}
	return storepb.PromMatchersToString(matchers...)
}

// MatchesExternalLabels follows Thanos sidecar StoreAPI semantics: external
// label matchers select this store and are removed before querying the backend.
func MatchesExternalLabels(matchers []storepb.LabelMatcher, externalLabels labels.Labels) (bool, []*labels.Matcher, error) {
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

// RewriteQueryForExternalLabels applies StoreAPI-style external label matching
// to raw PromQL QueryAPI requests.
func RewriteQueryForExternalLabels(query string, externalLabels labels.Labels) (string, bool, error) {
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
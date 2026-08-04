package promql

import (
	"fmt"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
)

func QueryStringFromRequestPlan(query string, plan *querypb.QueryPlan) (string, error) {
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

	node = MergeQueryPlanSelectorFilters(node)
	plannedQuery := strings.TrimSpace(node.String())
	if plannedQuery == "" {
		return "", fmt.Errorf("query plan rendered an empty query")
	}
	if _, err := parser.ParseExpr(plannedQuery); err != nil {
		return "", fmt.Errorf("query plan rendered invalid PromQL %q: %w", plannedQuery, err)
	}
	return plannedQuery, nil
}

func MergeQueryPlanSelectorFilters(node logicalplan.Node) logicalplan.Node {
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
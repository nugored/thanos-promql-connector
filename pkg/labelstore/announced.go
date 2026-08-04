package labelstore

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"main.go/pkg/backend"
	"main.go/pkg/config"
)

type AnnouncedLabelSetsStore struct {
	mtx       sync.RWMutex
	labelSets []labels.Labels
}

type AnnouncedLabelSource struct {
	Name   string
	Client backend.QueryBackendAPI
}

type AnnouncedLabelSourceConfig struct {
	Name   string
	Config config.QueryBackendConfig
}

type AnnouncedLabelSourceFailure struct {
	Source string
	Err    error
}

func NewAnnouncedLabelSetsStore() *AnnouncedLabelSetsStore {
	return &AnnouncedLabelSetsStore{}
}

func (s *AnnouncedLabelSetsStore) LabelSets() []labels.Labels {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	result := make([]labels.Labels, 0, len(s.labelSets))
	for _, labelSet := range s.labelSets {
		result = append(result, labelSet.Copy())
	}
	return result
}

func (s *AnnouncedLabelSetsStore) UpdateFromBackend(ctx context.Context, client backend.QueryBackendAPI, labelNames []string) error {
	_, err := s.UpdateFromSources(ctx, []AnnouncedLabelSource{{Name: "backend", Client: client}}, labelNames, 0, 0)
	return err
}

func (s *AnnouncedLabelSetsStore) UpdateFromSources(ctx context.Context, sources []AnnouncedLabelSource, labelNames []string, sourceTimeout, lookback time.Duration) ([]AnnouncedLabelSourceFailure, error) {
	if len(sources) == 0 {
		return nil, fmt.Errorf("no announced label sources configured")
	}
	startTime, endTime, err := AnnouncedLabelTimeRange(lookback)
	if err != nil {
		return nil, err
	}

	labelSets := make([]labels.Labels, 0)
	seen := make(map[string]struct{})
	failures := make([]AnnouncedLabelSourceFailure, 0)
	successfulSources := 0

	for _, source := range sources {
		sourceCtx := ctx
		cancel := func() {}
		if sourceTimeout > 0 {
			sourceCtx, cancel = context.WithTimeout(ctx, sourceTimeout)
		}
		sourceLabelSets, err := AnnouncedLabelSetsFromBackend(sourceCtx, source.Client, labelNames, startTime, endTime)
		cancel()
		if err != nil {
			failures = append(failures, AnnouncedLabelSourceFailure{Source: source.Name, Err: err})
			continue
		}

		successfulSources++
		for _, labelSet := range sourceLabelSets {
			key := labelSet.String()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			labelSets = append(labelSets, labelSet)
		}
	}

	if successfulSources == 0 {
		return failures, announcedLabelSourceFailuresError(failures)
	}

	sort.Slice(labelSets, func(i, j int) bool {
		return labelSets[i].String() < labelSets[j].String()
	})

	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.labelSets = labelSets
	return failures, nil
}

func AnnouncedLabelTimeRange(lookback time.Duration) (time.Time, time.Time, error) {
	if lookback < 0 {
		return time.Time{}, time.Time{}, fmt.Errorf("query.announce-label-lookback must be greater than or equal to zero")
	}
	if lookback == 0 {
		return time.Time{}, time.Time{}, nil
	}

	endTime := time.Now().UTC()
	return endTime.Add(-lookback), endTime, nil
}

func AnnouncedLabelSetsFromBackend(ctx context.Context, client backend.QueryBackendAPI, labelNames []string, startTime, endTime time.Time) ([]labels.Labels, error) {
	labelSets := make([]labels.Labels, 0)
	seen := make(map[string]struct{})

	for _, labelName := range labelNames {
		if !model.LabelName(labelName).IsValid() {
			return nil, fmt.Errorf("invalid announced label name %q", labelName)
		}

		values, _, err := client.LabelValues(ctx, labelName, nil, startTime, endTime)
		if err != nil {
			return nil, fmt.Errorf("read values for announced label %q: %w", labelName, err)
		}
		for _, labelSet := range AnnouncedLabelSetsFromValues(labelName, values) {
			key := labelSet.String()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			labelSets = append(labelSets, labelSet)
		}
	}

	sort.Slice(labelSets, func(i, j int) bool {
		return labelSets[i].String() < labelSets[j].String()
	})
	return labelSets, nil
}

func CreateAnnouncedLabelSources(queryConfig config.QueryBackendConfig) ([]AnnouncedLabelSource, error) {
	sourceConfigs := AnnouncedLabelSourceConfigs(queryConfig)
	sources := make([]AnnouncedLabelSource, 0, len(sourceConfigs))
	for _, sourceConfig := range sourceConfigs {
		client, err := backend.CreateQueryBackendClient(sourceConfig.Config)
		if err != nil {
			return nil, fmt.Errorf("create announced label source %q: %w", sourceConfig.Name, err)
		}
		sources = append(sources, AnnouncedLabelSource{
			Name:   sourceConfig.Name,
			Client: client,
		})
	}
	return sources, nil
}

func AnnouncedLabelSourceConfigs(queryConfig config.QueryBackendConfig) []AnnouncedLabelSourceConfig {
	headerName, headerValue, ok := headerValue(queryConfig.Headers, "X-Scope-OrgID")
	if !ok {
		return []AnnouncedLabelSourceConfig{{Name: "backend", Config: queryConfig}}
	}

	tenants := SplitTenantHeader(headerValue)
	if len(tenants) == 0 {
		return []AnnouncedLabelSourceConfig{{Name: "backend", Config: queryConfig}}
	}

	sourceConfigs := make([]AnnouncedLabelSourceConfig, 0, len(tenants))
	for _, tenant := range tenants {
		sourceConfig := queryConfig
		sourceConfig.Headers = cloneHeaders(queryConfig.Headers)
		sourceConfig.Headers[headerName] = tenant
		sourceConfigs = append(sourceConfigs, AnnouncedLabelSourceConfig{
			Name:   tenant,
			Config: sourceConfig,
		})
	}
	return sourceConfigs
}

func headerValue(headers map[string]string, name string) (string, string, bool) {
	for headerName, headerValue := range headers {
		if strings.EqualFold(headerName, name) {
			return headerName, headerValue, true
		}
	}
	return "", "", false
}

func SplitTenantHeader(value string) []string {
	parts := strings.Split(value, "|")
	tenants := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		tenants = append(tenants, part)
	}
	return tenants
}

func cloneHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return nil
	}

	result := make(map[string]string, len(headers))
	for name, value := range headers {
		result[name] = value
	}
	return result
}

func announcedLabelSourceFailuresError(failures []AnnouncedLabelSourceFailure) error {
	if len(failures) == 0 {
		return fmt.Errorf("all announced label sources failed")
	}

	messages := make([]string, 0, len(failures))
	for _, failure := range failures {
		messages = append(messages, fmt.Sprintf("%s: %s", failure.Source, failure.Err))
	}
	return fmt.Errorf("all announced label sources failed: %s", strings.Join(messages, "; "))
}

func LogAnnouncedLabelSourceFailures(logger log.Logger, failures []AnnouncedLabelSourceFailure) {
	for _, failure := range failures {
		level.Warn(logger).Log("msg", "loading announced label sets from source failed", "source", failure.Source, "err", failure.Err)
	}
}

func AnnouncedLabelSourceNames(sources []AnnouncedLabelSource) []string {
	names := make([]string, 0, len(sources))
	for _, source := range sources {
		names = append(names, source.Name)
	}
	return names
}

func AnnouncedLabelSetsFromValues(labelName string, values model.LabelValues) []labels.Labels {
	uniqueValues := make(map[string]struct{}, len(values))
	for _, value := range values {
		value := string(value)
		if value == "" {
			continue
		}
		uniqueValues[value] = struct{}{}
	}

	sortedValues := make([]string, 0, len(uniqueValues))
	for value := range uniqueValues {
		sortedValues = append(sortedValues, value)
	}
	sort.Strings(sortedValues)

	labelSets := make([]labels.Labels, 0, len(sortedValues))
	for _, value := range sortedValues {
		labelSets = append(labelSets, labels.FromStrings(labelName, value))
	}
	return labelSets
}
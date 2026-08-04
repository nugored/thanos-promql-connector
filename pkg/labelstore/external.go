package labelstore

import (
	"context"
	"fmt"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"gopkg.in/yaml.v2"

	"main.go/pkg/backend"
)

type ExternalLabelsStore struct {
	mtx    sync.RWMutex
	labels labels.Labels
}

func NewExternalLabelsStore() *ExternalLabelsStore {
	return &ExternalLabelsStore{labels: labels.EmptyLabels()}
}

func (s *ExternalLabelsStore) Labels() labels.Labels {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.labels.Copy()
}

func (s *ExternalLabelsStore) UpdateFromBackend(ctx context.Context, client backend.QueryBackendAPI) error {
	config, err := client.Config(ctx)
	if err != nil {
		return err
	}
	externalLabels, err := ExternalLabelsFromConfigYAML(config.YAML)
	if err != nil {
		return err
	}

	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.labels = externalLabels
	return nil
}

func ExternalLabelsFromConfigYAML(configYAML string) (labels.Labels, error) {
	var cfg struct {
		GlobalConfig struct {
			ExternalLabels map[string]string `yaml:"external_labels"`
		} `yaml:"global"`
	}
	if err := yaml.Unmarshal([]byte(configYAML), &cfg); err != nil {
		return labels.EmptyLabels(), fmt.Errorf("parse Prometheus config: %w", err)
	}
	return labels.FromMap(cfg.GlobalConfig.ExternalLabels), nil
}
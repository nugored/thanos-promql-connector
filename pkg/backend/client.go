package backend

import (
	"context"
	"fmt"
	"net/http"
	"time"

	authcredentials "cloud.google.com/go/auth/credentials"
	authhttptransport "cloud.google.com/go/auth/httptransport"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/store/labelpb"

	"main.go/pkg/config"
)

const GoogleMonitoringReadScope = "https://www.googleapis.com/auth/monitoring.read"

type QueryBackendAPI interface {
	Config(ctx context.Context) (v1.ConfigResult, error)
	LabelNames(ctx context.Context, matches []string, startTime, endTime time.Time, opts ...v1.Option) ([]string, v1.Warnings, error)
	LabelValues(ctx context.Context, label string, matches []string, startTime, endTime time.Time, opts ...v1.Option) (model.LabelValues, v1.Warnings, error)
	Query(ctx context.Context, query string, ts time.Time, opts ...v1.Option) (model.Value, v1.Warnings, error)
	QueryRange(ctx context.Context, query string, r v1.Range, opts ...v1.Option) (model.Value, v1.Warnings, error)
	Series(ctx context.Context, matches []string, startTime, endTime time.Time, opts ...v1.Option) ([]model.LabelSet, v1.Warnings, error)
}

type QueryBackendEndpoint struct {
	Name           string
	QueryBackend   string
	Client         QueryBackendAPI
	ExternalLabels func() labels.Labels
}

func (b QueryBackendEndpoint) Labels() labels.Labels {
	if b.ExternalLabels == nil {
		return labels.EmptyLabels()
	}
	return b.ExternalLabels()
}

func StaticExternalLabelsFunc(value labels.Labels) func() labels.Labels {
	value = value.Copy()
	return func() labels.Labels {
		return value.Copy()
	}
}

type HeaderRoundTripper struct {
	base    http.RoundTripper
	headers http.Header
}

func NewHeaderRoundTripper(base http.RoundTripper, headers map[string]string) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	if len(headers) == 0 {
		return base
	}

	rt := &HeaderRoundTripper{
		base:    base,
		headers: make(http.Header, len(headers)),
	}
	for name, value := range headers {
		rt.headers.Set(name, value)
	}
	return rt
}

func (rt *HeaderRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	outgoing := req.Clone(req.Context())
	for name, values := range rt.headers {
		outgoing.Header.Del(name)
		for _, value := range values {
			outgoing.Header.Add(name, value)
		}
	}

	return rt.base.RoundTrip(outgoing)
}

type RetryRoundTripper struct {
	base       http.RoundTripper
	maxRetries int
}

func NewRetryRoundTripper(base http.RoundTripper, maxRetries int) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	if maxRetries <= 0 {
		maxRetries = 3
	}
	return &RetryRoundTripper{
		base:       base,
		maxRetries: maxRetries,
	}
}

func (rt *RetryRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	var resp *http.Response
	var err error

	for attempt := 0; attempt <= rt.maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(100*(1<<attempt)) * time.Millisecond
			select {
			case <-req.Context().Done():
				if resp != nil {
					return resp, nil
				}
				return nil, req.Context().Err()
			case <-time.After(backoff):
			}

			if req.GetBody != nil {
				newBody, err := req.GetBody()
				if err == nil {
					req.Body = newBody
				}
			}
		}

		resp, err = rt.base.RoundTrip(req)
		if err != nil {
			if req.Context().Err() != nil {
				return nil, err
			}
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests ||
			resp.StatusCode == http.StatusBadGateway ||
			resp.StatusCode == http.StatusServiceUnavailable ||
			resp.StatusCode == http.StatusGatewayTimeout {
			if attempt < rt.maxRetries {
				resp.Body.Close()
				continue
			}
		}

		return resp, nil
	}

	return resp, err
}

func NewQueryBackendRoundTripper(queryConfig config.QueryBackendConfig) (http.RoundTripper, error) {
	transport := http.RoundTripper(http.DefaultTransport)
	if QueryAuthEnabled(queryConfig.Auth) {
		client, err := authhttptransport.NewClient(&authhttptransport.Options{
			BaseRoundTripper: transport,
			DetectOpts: &authcredentials.DetectOptions{
				CredentialsFile: queryConfig.Auth.CredentialsFile,
				Scopes:          GoogleAuthScopes(queryConfig.Auth),
			},
		})
		if err != nil {
			return nil, fmt.Errorf("error creating Google auth HTTP transport: %w", err)
		}
		transport = client.Transport
	}

	transport = NewHeaderRoundTripper(transport, queryConfig.Headers)
	return NewRetryRoundTripper(transport, 3), nil
}

func QueryAuthEnabled(auth config.QueryBackendAuthConfig) bool {
	return auth.Google || auth.CredentialsFile != "" || len(auth.Scopes) > 0
}

func GoogleAuthScopes(auth config.QueryBackendAuthConfig) []string {
	if len(auth.Scopes) > 0 {
		return auth.Scopes
	}
	if auth.Google {
		return []string{GoogleMonitoringReadScope}
	}
	return nil
}

func NewGCPQueryBackends(baseConfig config.QueryBackendConfig, projects []string, staticExternalLabels labels.Labels) ([]QueryBackendEndpoint, error) {
	backends := make([]QueryBackendEndpoint, 0, len(projects))
	for _, project := range projects {
		targetURL, err := config.GooglePrometheusTargetURL(project)
		if err != nil {
			return nil, err
		}

		backendConfig := baseConfig
		backendConfig.QueryTargetURL = targetURL
		backendConfig.Auth.Google = true

		client, err := CreateQueryBackendClient(backendConfig)
		if err != nil {
			return nil, fmt.Errorf("create Google project backend %q: %w", project, err)
		}

		projectLabels := labelpb.ExtendSortedLabels(staticExternalLabels, labels.FromStrings("prometheus", "gcp-"+project))
		backends = append(backends, QueryBackendEndpoint{
			Name:           project,
			QueryBackend:   targetURL,
			Client:         client,
			ExternalLabels: StaticExternalLabelsFunc(projectLabels),
		})
	}
	return backends, nil
}

func QueryBackendLabelSets(backends []QueryBackendEndpoint) []labels.Labels {
	labelSets := make([]labels.Labels, 0, len(backends))
	for _, backend := range backends {
		externalLabels := backend.Labels()
		if externalLabels.IsEmpty() {
			continue
		}
		labelSets = append(labelSets, externalLabels)
	}
	return labelSets
}

func CreateQueryBackendClient(queryConfig config.QueryBackendConfig) (QueryBackendAPI, error) {
	transport, err := NewQueryBackendRoundTripper(queryConfig)
	if err != nil {
		return nil, err
	}
	targetURL, err := config.BackendTargetURL(queryConfig)
	if err != nil {
		return nil, err
	}
	client, err := api.NewClient(api.Config{
		Address:      targetURL,
		RoundTripper: transport,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating client: %s", err)
	}
	return NewSingleflightClient(v1.NewAPI(client)), nil
}
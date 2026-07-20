package main

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
)

// queryBackendConfig represents the configuration for a backend query target.
type queryBackendConfig struct {
	QueryTargetURL string
	Headers        map[string]string
	QueryParams    map[string][]string
	Auth           queryBackendAuthConfig
}

type queryBackendAuthConfig struct {
	CredentialsFile string
	Scopes          []string
}

type headerFlags map[string]string

func (h *headerFlags) String() string {
	if h == nil || len(*h) == 0 {
		return ""
	}

	pairs := make([]string, 0, len(*h))
	for name, value := range *h {
		pairs = append(pairs, fmt.Sprintf("%s=%s", name, value))
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}

func (h *headerFlags) Set(value string) error {
	name, headerValue, ok := splitHeaderFlag(value)
	if !ok {
		return fmt.Errorf("query.header must be in Name=Value or Name: Value format")
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("query.header cannot contain an empty header name")
	}

	if *h == nil {
		*h = make(map[string]string)
	}
	(*h)[name] = strings.TrimSpace(headerValue)
	return nil
}

func (h headerFlags) values() map[string]string {
	if len(h) == 0 {
		return nil
	}

	result := make(map[string]string, len(h))
	for name, value := range h {
		result[name] = value
	}
	return result
}

type queryParamFlags map[string][]string

func (p *queryParamFlags) String() string {
	if p == nil || len(*p) == 0 {
		return ""
	}

	pairs := make([]string, 0)
	for name, values := range *p {
		for _, value := range values {
			pairs = append(pairs, fmt.Sprintf("%s=%s", name, value))
		}
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}

func (p *queryParamFlags) Set(value string) error {
	equalsIndex := strings.Index(value, "=")
	if equalsIndex < 0 {
		return fmt.Errorf("query.param must be in Name=Value format")
	}

	name := strings.TrimSpace(value[:equalsIndex])
	if name == "" {
		return fmt.Errorf("query.param cannot contain an empty parameter name")
	}

	if *p == nil {
		*p = make(map[string][]string)
	}
	(*p)[name] = append((*p)[name], strings.TrimSpace(value[equalsIndex+1:]))
	return nil
}

func (p queryParamFlags) values() map[string][]string {
	return cloneQueryParams(p)
}

func splitHeaderFlag(value string) (string, string, bool) {
	equalsIndex := strings.Index(value, "=")
	colonIndex := strings.Index(value, ":")

	switch {
	case equalsIndex < 0 && colonIndex < 0:
		return "", "", false
	case equalsIndex < 0:
		return value[:colonIndex], value[colonIndex+1:], true
	case colonIndex < 0:
		return value[:equalsIndex], value[equalsIndex+1:], true
	case equalsIndex < colonIndex:
		return value[:equalsIndex], value[equalsIndex+1:], true
	default:
		return value[:colonIndex], value[colonIndex+1:], true
	}
}

type stringListFlag []string

func (s *stringListFlag) String() string {
	if s == nil {
		return ""
	}
	return strings.Join(*s, ",")
}

func (s *stringListFlag) Set(value string) error {
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		*s = append(*s, part)
	}
	return nil
}

func (s stringListFlag) values() []string {
	if len(s) == 0 {
		return nil
	}

	result := make([]string, len(s))
	copy(result, s)
	return result
}

func newQueryBackendConfig(queryTargetURL string, headers map[string]string, queryParams map[string][]string, credentialsFile string, scopes []string) (*queryBackendConfig, error) {
	normalizedHeaders, err := normalizeHeaders(headers)
	if err != nil {
		return nil, err
	}
	normalizedQueryParams, err := normalizeQueryParams(queryParams)
	if err != nil {
		return nil, err
	}

	cfg := &queryBackendConfig{
		QueryTargetURL: strings.TrimSpace(queryTargetURL),
		Headers:        normalizedHeaders,
		QueryParams:    normalizedQueryParams,
		Auth: queryBackendAuthConfig{
			CredentialsFile: strings.TrimSpace(credentialsFile),
			Scopes:          normalizeStrings(scopes),
		},
	}

	if cfg.QueryTargetURL == "" {
		return nil, fmt.Errorf("query.target-url flag must be set")
	}
	return cfg, nil
}

func backendTargetURL(config queryBackendConfig) (string, error) {
	targetURL := strings.TrimSpace(config.QueryTargetURL)
	if len(config.QueryParams) == 0 {
		return targetURL, nil
	}

	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		return "", fmt.Errorf("parse query.target-url: %w", err)
	}
	values := parsedURL.Query()
	for name, paramValues := range config.QueryParams {
		for _, value := range paramValues {
			values.Add(name, value)
		}
	}
	parsedURL.RawQuery = values.Encode()
	return parsedURL.String(), nil
}

func normalizeHeaders(headers map[string]string) (map[string]string, error) {
	if len(headers) == 0 {
		return nil, nil
	}

	result := make(map[string]string, len(headers))
	for name, value := range headers {
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, fmt.Errorf("headers cannot contain an empty header name")
		}
		result[name] = strings.TrimSpace(value)
	}
	return result, nil
}

func normalizeQueryParams(params map[string][]string) (map[string][]string, error) {
	if len(params) == 0 {
		return nil, nil
	}

	result := make(map[string][]string, len(params))
	for name, values := range params {
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, fmt.Errorf("query params cannot contain an empty parameter name")
		}
		for _, value := range values {
			result[name] = append(result[name], strings.TrimSpace(value))
		}
	}
	return result, nil
}

func cloneQueryParams(params map[string][]string) map[string][]string {
	if len(params) == 0 {
		return nil
	}

	result := make(map[string][]string, len(params))
	for name, values := range params {
		result[name] = append([]string(nil), values...)
	}
	return result
}

func normalizeStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			result = append(result, value)
		}
	}
	return result
}

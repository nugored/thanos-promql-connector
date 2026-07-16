package main

import (
	"fmt"
	"sort"
	"strings"
)

// queryBackendConfig represents the configuration for a backend query target.
type queryBackendConfig struct {
	QueryTargetURL string
	Headers        map[string]string
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

func newQueryBackendConfig(queryTargetURL string, headers map[string]string, credentialsFile string, scopes []string) (*queryBackendConfig, error) {
	normalizedHeaders, err := normalizeHeaders(headers)
	if err != nil {
		return nil, err
	}

	cfg := &queryBackendConfig{
		QueryTargetURL: strings.TrimSpace(queryTargetURL),
		Headers:        normalizedHeaders,
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

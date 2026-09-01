package backend

import (
	"net/http"
	"reflect"
	"testing"

	"main.go/pkg/config"
)

func TestNewQueryBackendRoundTripperSkipsGoogleAuthWhenAuthEmpty(t *testing.T) {
	rt, err := NewQueryBackendRoundTripper(config.QueryBackendConfig{})
	if err != nil {
		t.Fatalf("NewQueryBackendRoundTripper() returned error: %v", err)
	}
	retryRT, ok := rt.(*RetryRoundTripper)
	if !ok {
		t.Fatalf("NewQueryBackendRoundTripper() = %T, want *RetryRoundTripper", rt)
	}
	if retryRT.base != http.DefaultTransport {
		t.Fatalf("RetryRoundTripper base = %T, want http.DefaultTransport", retryRT.base)
	}
}

func TestNewQueryBackendRoundTripperKeepsHeadersWithoutAuth(t *testing.T) {
	rt, err := NewQueryBackendRoundTripper(config.QueryBackendConfig{
		Headers: map[string]string{"X-Scope-OrgID": "tenant1|tenant2"},
	})
	if err != nil {
		t.Fatalf("NewQueryBackendRoundTripper() returned error: %v", err)
	}

	retryRT, ok := rt.(*RetryRoundTripper)
	if !ok {
		t.Fatalf("NewQueryBackendRoundTripper() = %T, want *RetryRoundTripper", rt)
	}
	headerRT, ok := retryRT.base.(*HeaderRoundTripper)
	if !ok {
		t.Fatalf("RetryRoundTripper base = %T, want *HeaderRoundTripper", retryRT.base)
	}
	if headerRT.base != http.DefaultTransport {
		t.Fatalf("header round tripper base = %T, want http.DefaultTransport", headerRT.base)
	}
}

func TestQueryAuthEnabled(t *testing.T) {
	tests := []struct {
		name string
		auth config.QueryBackendAuthConfig
		want bool
	}{
		{name: "empty"},
		{name: "google adc", auth: config.QueryBackendAuthConfig{Google: true}, want: true},
		{name: "credentials file", auth: config.QueryBackendAuthConfig{CredentialsFile: "/key.json"}, want: true},
		{name: "scopes", auth: config.QueryBackendAuthConfig{Scopes: []string{"https://www.googleapis.com/auth/monitoring.read"}}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := QueryAuthEnabled(tt.auth); got != tt.want {
				t.Fatalf("QueryAuthEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGoogleAuthScopes(t *testing.T) {
	tests := []struct {
		name string
		auth config.QueryBackendAuthConfig
		want []string
	}{
		{name: "empty"},
		{name: "google adc default", auth: config.QueryBackendAuthConfig{Google: true}, want: []string{GoogleMonitoringReadScope}},
		{name: "explicit scopes", auth: config.QueryBackendAuthConfig{Google: true, Scopes: []string{"scope-a"}}, want: []string{"scope-a"}},
		{name: "credentials file without google flag keeps empty scopes", auth: config.QueryBackendAuthConfig{CredentialsFile: "/key.json"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GoogleAuthScopes(tt.auth); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("GoogleAuthScopes() = %v, want %v", got, tt.want)
			}
		})
	}
}

type fakeRoundTripper struct {
	attempts  int
	responses []*http.Response
}

func (f *fakeRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	resp := f.responses[f.attempts]
	f.attempts++
	return resp, nil
}

func TestRetryRoundTripperRetries429ThenSucceeds(t *testing.T) {
	fake := &fakeRoundTripper{
		responses: []*http.Response{
			{StatusCode: http.StatusTooManyRequests, Body: http.NoBody},
			{StatusCode: http.StatusOK, Body: http.NoBody},
		},
	}
	rt := NewRetryRoundTripper(fake, 3)
	req, _ := http.NewRequest("GET", "http://example.com", nil)
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d, want 200", resp.StatusCode)
	}
	if fake.attempts != 2 {
		t.Fatalf("attempts = %d, want 2", fake.attempts)
	}
}
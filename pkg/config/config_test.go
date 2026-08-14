package config

import (
	"reflect"
	"testing"
)

func TestHeaderFlagsSet(t *testing.T) {
	var headers HeaderFlags

	if err := headers.Set("X-Scope-OrgID=tenant1|tenant2"); err != nil {
		t.Fatalf("headers.Set() returned error: %v", err)
	}
	if err := headers.Set("Authorization: Bearer token:with-colon=and-equals"); err != nil {
		t.Fatalf("headers.Set() returned error: %v", err)
	}

	want := map[string]string{
		"Authorization": "Bearer token:with-colon=and-equals",
		"X-Scope-OrgID": "tenant1|tenant2",
	}
	if got := headers.Values(); !reflect.DeepEqual(got, want) {
		t.Fatalf("headers.Values() = %v, want %v", got, want)
	}
}

func TestHeaderFlagsSetRejectsInvalidValue(t *testing.T) {
	var headers HeaderFlags

	if err := headers.Set("missing-separator"); err == nil {
		t.Fatal("headers.Set() succeeded, want error")
	}
	if err := headers.Set(" =value"); err == nil {
		t.Fatal("headers.Set() succeeded with empty header name, want error")
	}
}

func TestQueryParamFlagsSet(t *testing.T) {
	var params QueryParamFlags

	if err := params.Set(`storeMatch[]={connector!="thanos-promql-connector"}`); err != nil {
		t.Fatalf("params.Set() returned error: %v", err)
	}
	if err := params.Set("dedup=false"); err != nil {
		t.Fatalf("params.Set() returned error: %v", err)
	}

	want := map[string][]string{
		"dedup":        {"false"},
		"storeMatch[]": {`{connector!="thanos-promql-connector"}`},
	}
	if got := params.Values(); !reflect.DeepEqual(got, want) {
		t.Fatalf("params.Values() = %v, want %v", got, want)
	}
}

func TestQueryParamFlagsSetRejectsInvalidValue(t *testing.T) {
	var params QueryParamFlags

	if err := params.Set("missing-separator"); err == nil {
		t.Fatal("params.Set() succeeded, want error")
	}
	if err := params.Set(" =value"); err == nil {
		t.Fatal("params.Set() succeeded with empty parameter name, want error")
	}
}

func TestLabelFlagsSet(t *testing.T) {
	var values LabelFlags

	if err := values.Set("prometheus=gcp-project"); err != nil {
		t.Fatalf("values.Set() returned error: %v", err)
	}
	if err := values.Set("region=global"); err != nil {
		t.Fatalf("values.Set() returned error: %v", err)
	}

	want := map[string]string{"prometheus": "gcp-project", "region": "global"}
	if got := values.Values(); !reflect.DeepEqual(got, want) {
		t.Fatalf("values.Values() = %v, want %v", got, want)
	}
}

func TestLabelFlagsSetRejectsInvalidValue(t *testing.T) {
	var values LabelFlags

	for _, value := range []string{"missing-separator", "=value", "invalid-name=value", "prometheus="} {
		t.Run(value, func(t *testing.T) {
			if err := values.Set(value); err == nil {
				t.Fatal("values.Set() succeeded, want error")
			}
		})
	}
}

func TestStringListFlagSet(t *testing.T) {
	var values StringListFlag

	if err := values.Set("scope-a, scope-b"); err != nil {
		t.Fatalf("values.Set() returned error: %v", err)
	}
	if err := values.Set("scope-c"); err != nil {
		t.Fatalf("values.Set() returned error: %v", err)
	}

	want := []string{"scope-a", "scope-b", "scope-c"}
	if got := values.Values(); !reflect.DeepEqual(got, want) {
		t.Fatalf("values.Values() = %v, want %v", got, want)
	}
}

func TestNormalizeGCPProjectsDeduplicates(t *testing.T) {
	got, err := NormalizeGCPProjects([]string{"my-gcp-project", " other-gcp-project ", "my-gcp-project"})
	if err != nil {
		t.Fatalf("NormalizeGCPProjects() returned error: %v", err)
	}

	want := []string{"my-gcp-project", "other-gcp-project"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("NormalizeGCPProjects() = %v, want %v", got, want)
	}
}

func TestGooglePrometheusTargetURL(t *testing.T) {
	got, err := GooglePrometheusTargetURL("my-gcp-project")
	if err != nil {
		t.Fatalf("GooglePrometheusTargetURL() returned error: %v", err)
	}

	want := "https://monitoring.googleapis.com/v1/projects/my-gcp-project/location/global/prometheus"
	if got != want {
		t.Fatalf("GooglePrometheusTargetURL() = %q, want %q", got, want)
	}
}

func TestNewQueryBackendConfigRequiresTargetURL(t *testing.T) {
	if _, err := NewQueryBackendConfig("", nil, nil, false, "", nil); err == nil {
		t.Fatal("NewQueryBackendConfig() succeeded without target URL, want error")
	}
}

func TestNewQueryBackendConfigBuildsConfigFromStartupParameters(t *testing.T) {
	cfg, err := NewQueryBackendConfig(
		" http://127.0.0.1:18080/prometheus ",
		map[string]string{"X-Scope-OrgID": "tenant1|tenant2"},
		map[string][]string{"storeMatch[]": {`{connector!="thanos-promql-connector"}`}},
		true,
		" /key.json ",
		[]string{" https://www.googleapis.com/auth/monitoring.read ", ""},
	)
	if err != nil {
		t.Fatalf("NewQueryBackendConfig() returned error: %v", err)
	}

	if cfg.QueryTargetURL != "http://127.0.0.1:18080/prometheus" {
		t.Fatalf("QueryTargetURL = %q", cfg.QueryTargetURL)
	}
	if got := cfg.Headers; !reflect.DeepEqual(got, map[string]string{"X-Scope-OrgID": "tenant1|tenant2"}) {
		t.Fatalf("Headers = %v", got)
	}
	if got := cfg.QueryParams; !reflect.DeepEqual(got, map[string][]string{"storeMatch[]": {`{connector!="thanos-promql-connector"}`}}) {
		t.Fatalf("QueryParams = %v", got)
	}
	if !cfg.Auth.Google {
		t.Fatal("Auth.Google = false, want true")
	}
	if cfg.Auth.CredentialsFile != "/key.json" {
		t.Fatalf("CredentialsFile = %q", cfg.Auth.CredentialsFile)
	}
	if got := cfg.Auth.Scopes; !reflect.DeepEqual(got, []string{"https://www.googleapis.com/auth/monitoring.read"}) {
		t.Fatalf("Scopes = %v", got)
	}
}

func TestBackendTargetURLAppendsQueryParams(t *testing.T) {
	got, err := BackendTargetURL(QueryBackendConfig{
		QueryTargetURL: "http://thanos-query:10902/prometheus?dedup=true",
		QueryParams: map[string][]string{
			"storeMatch[]": {`{connector!="thanos-promql-connector"}`, `{cluster="prod"}`},
		},
	})
	if err != nil {
		t.Fatalf("BackendTargetURL() returned error: %v", err)
	}

	want := "http://thanos-query:10902/prometheus?dedup=true&storeMatch%5B%5D=%7Bconnector%21%3D%22thanos-promql-connector%22%7D&storeMatch%5B%5D=%7Bcluster%3D%22prod%22%7D"
	if got != want {
		t.Fatalf("BackendTargetURL() = %q, want %q", got, want)
	}
}
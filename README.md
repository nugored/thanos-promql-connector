# thanos-promql-connector
This tool bridges a PromQL HTTP API query backend to Thanos Querier, exposing a gRPC server that can be queried by [Thanos Querier](https://github.com/thanos-io/thanos).

# How to Run

See the `examples` directory for `docker compose` setups.

## Test and Debugging instructions
1. Start the connector with your desired query target:

```bash
go run -tags slicelabels . \
    --query.target-url=http://127.0.0.1:18080/prometheus
```

2. Launch Thanos Querier:
```bash
git clone https://github.com/thanos-io/thanos.git
cd thanos
go run -tags slicelabels ./cmd/thanos query --endpoint :8081 --query.mode distributed --query.promql-engine thanos
```

3. View Thanos Querier UI at address [0.0.0.0:10902](http://localhost:10902/).

## Backend Request Headers

Static headers can be added to every proxied PromQL backend request with `--query.header`.
The flag can be repeated and accepts `Name=Value` or `Name: Value`:

```bash
go run -tags slicelabels . \
    --query.target-url=http://127.0.0.1:18080/prometheus \
    '--query.header=X-Scope-OrgID=tenant1|tenant2'
```

## Build

The connector uses Thanos `v0.42.2` libraries. Build and test with the
`slicelabels` Go tag; this is the default in the Dockerfile and Makefile:

```bash
make test
make build
make build-push
```

For direct local commands:

```bash
go test -tags slicelabels ./...
go build -tags slicelabels .
```

## Backend Query Parameters

Static query parameters can be added to every backend Prometheus API request
with `--query.param`. The flag can be repeated and accepts `Name=Value`.

This is mainly useful when the backend is a Thanos Querier and you need to pass
Thanos-specific parameters such as `storeMatch[]`:

```bash
go run -tags slicelabels . \
    --query.target-url=http://thanos-query-backend:10902 \
    '--query.param=storeMatch[]={cluster="prod"}'
```

Do not point `--query.target-url` at the same Thanos Querier that has this
connector configured as an endpoint. That creates a recursive path:

```text
Thanos Querier -> StoreAPI connector -> backend HTTP query_range -> same Thanos Querier -> StoreAPI connector -> ...
```

With the Thanos PromQL engine, that loop can surface as a warning like
`runtime error: invalid memory address or nil pointer dereference`, even for a
plain selector such as `prometheus_tsdb_head_max_time_seconds{prometheus=~".*"}`.
Point the connector at Mimir Querier directly, or at a separate Thanos Querier
that does not include the connector as an endpoint. If you intentionally use a
Thanos Querier backend, use `storeMatch[]` to select only stores whose announced
external labels cannot match the connector.

## Google Managed Service for Prometheus

The connector can query Google Cloud Managed Service for Prometheus through the
Prometheus HTTP API. Use `--query.gcp-project` for Google projects. The flag
can be repeated or comma-separated.

For each project, the connector derives:

```text
--query.target-url=https://monitoring.googleapis.com/v1/projects/<PROJECT_ID>/location/global/prometheus
--query.external-label=prometheus=gcp-<PROJECT_ID>
```

Example:

```bash
go run -tags slicelabels . \
    --query.gcp-project=itk8s-208609 \
    --query.gcp-project=space-prod
```

When `--query.gcp-project` is set, the connector enables Google Application
Default Credentials automatically. The Google auth library detects credentials
from `GOOGLE_APPLICATION_CREDENTIALS`, local gcloud ADC, or the metadata server
such as GKE Workload Identity. If no scope is set, the connector uses the
read-only Cloud Monitoring scope:

```bash
https://www.googleapis.com/auth/monitoring.read
```

You can override scopes, or still provide a credentials file explicitly:

```bash
go run -tags slicelabels . \
    --query.gcp-project=<PROJECT_ID> \
    --query.auth.scope=https://www.googleapis.com/auth/cloud-platform

go run -tags slicelabels . \
    --query.gcp-project=<PROJECT_ID> \
    --query.auth.credentials-file=/key.json \
    --query.auth.scope=https://www.googleapis.com/auth/monitoring.read
```

Do not combine `--query.gcp-project` with `--query.target-url`,
`--query.external-label=prometheus=...`, `--query.external-labels`, or
`--query.announce-label`; those values are derived per project in Google mode.

In multi-project mode, Thanos `Info` advertises one label set per project.
Queries without a `prometheus` matcher fan out to all configured projects.
Queries with `prometheus="gcp-<PROJECT_ID>"` are routed only to that project,
and the connector strips the virtual matcher before calling Google. Grafana
variable requests for `/api/v1/label/prometheus/values`, including requests with
`match[]`, return the configured `gcp-<PROJECT_ID>` values directly.
Variable requests for other labels are sent to the selected Google backend, with
the virtual `prometheus` matcher removed and the remaining `match[]` selectors
preserved.

The Kubernetes service account used by the connector pod must be able to obtain
Google credentials and must have read access to the scoping project, for example
`roles/monitoring.viewer`.

## Static External Labels

Use `--query.external-label=Name=Value` when the connector should own a virtual
external label that the backend does not physically store. The flag can be
repeated. Static external labels are advertised in Thanos `Info`, attached to
all gRPC StoreAPI and QueryAPI results.

For Google Managed Service for Prometheus, prefer `--query.gcp-project`; it
derives the `prometheus=gcp-<PROJECT_ID>` label per project.

The label participates in routing. StoreAPI label matchers for matching static
external labels are removed before the backend request. QueryAPI PromQL requests
also strip matching virtual label matchers before forwarding to Google, so this
query:

```promql
up{prometheus="gcp-<PROJECT_ID>"}
```

is sent to Google as:

```promql
up
```

and the connector adds `prometheus="gcp-<PROJECT_ID>"` to the returned series.
If a query asks for a different value, the connector returns no series for that
endpoint. Static external labels override backend labels with the same name in
connector responses.

When Thanos or Grafana asks for values of a static external label, for example
`/api/v1/label/prometheus/values` with a `match[]` selector, the connector
returns the configured static value after checking only external-label matchers.
It does not ask the backend to discover values for labels that only exist after
connector-side injection.

## StoreAPI Series

When Thanos Querier calls the StoreAPI `Series` method, the connector translates
the request into a PromQL HTTP `query_range` call and encodes returned samples as
Thanos XOR chunks. If Thanos does not send a step hint, the connector uses
`--query.series-step`, which defaults to `1m`.

For long StoreAPI ranges, the connector also protects the backend from
Prometheus-compatible `query_range` point limits. `--query.max-points-per-series`
defaults to `11000`; when a requested range would exceed that many points per
series, the connector increases only the backend `query_range` step enough to
stay within the limit.

Setting `--query.max-points-per-series=0` disables only this connector-side step
clamp. It does not disable Mimir's own query limit, so long high-resolution
queries can still fail with backend errors such as `exceeded maximum resolution
of 11,000 points per timeseries`. Keep this value aligned with the backend limit,
or increase the limit in Mimir if you need finer resolution over long ranges.

## Thanos Endpoint Discovery

The connector registers both the Thanos StoreAPI and QueryAPI gRPC services. The
`--grpc-info-api-mode` flag controls which API is advertised to Thanos Querier in
the Info response:

```bash
--grpc-info-api-mode=store # default: advertise StoreAPI only
--grpc-info-api-mode=query # advertise QueryAPI only
--grpc-info-api-mode=both  # advertise both StoreAPI and QueryAPI
```

Use `query` mode when you want Thanos Querier to avoid creating a StoreAPI client
for the connector. This can be useful for testing Thanos versions where StoreAPI
series consumption regresses.

One caveat: current Thanos versions derive remote QueryAPI time and label routing
metadata from `Info.store.tsdbInfos`. In `query` mode the connector intentionally
omits `Info.store`, so QueryAPI-only discovery can bypass StoreAPI but may lose
announced labelset/time metadata in Thanos' remote execution planner. If Thanos
does not query the endpoint in `query` mode, use `store` mode with a Thanos build
that handles StoreAPI correctly, for example `v0.39.1` or a fixed `v0.41.x`
build.

The old `--grpc-info-advertise-query-api` flag is kept for compatibility. When
used with the default mode, it maps to `--grpc-info-api-mode=both`.

## gRPC TLS

The connector can serve the Thanos QueryAPI, StoreAPI, and Info gRPC services
over TLS:

```bash
--grpc-server-tls-cert=/tls/tls.crt
--grpc-server-tls-key=/tls/tls.key
```

For strict mTLS, also configure a client CA. When this flag is set, the
connector requires and verifies the client certificate presented by Thanos
Querier:

```bash
--grpc-server-tls-client-ca=/tls/ca.crt
```

With one-way TLS and your Querier-side settings, Thanos Querier can connect with:

```bash
--endpoint=thanos-promql-connector:8081
--grpc-client-tls-secure
--grpc-client-tls-skip-verify
--grpc-client-tls-cert=/tls/tls.crt
--grpc-client-tls-key=/tls/tls.key
```

The connector only validates the Querier client certificate when
`--grpc-server-tls-client-ca` is configured.

The connector also registers a `snappy` gRPC compressor, so it can accept
metadata and query calls from Querier when Querier is started with:

```bash
--grpc-compression=snappy
```

## Response Label Dropping

Labels can be removed from QueryAPI and StoreAPI responses with `--query.drop-label`.
This is useful with Mimir tenant federation when you already have another stable
tenant-disambiguating label, for example an external label named `prometheus`:

```bash
--query.drop-label=__tenant_id__
```

## Announced LabelSets From Backend Labels

For a federated Mimir backend that already stores a per-series external label
such as `prometheus`, the connector can advertise the backend values of that
label in StoreAPI `Info`:

```bash
--query.announce-label=prometheus
--query.announce-label-refresh=1m
--query.announce-label-lookback=6h
```

With a Mimir target URL such as `http://mimir-querier:8080/prometheus`, this
uses the Prometheus-compatible label values endpoint:

```text
/prometheus/api/v1/label/prometheus/values
```

The connector sends the same static headers configured with `--query.header`, so
tenant federation headers such as `X-Scope-OrgID` are included in this request.

When `--query.announce-label-lookback` is greater than zero, the connector adds
`start=now-lookback` and `end=now` to the label values request. This is useful
with Mimir backends where unbounded label discovery may try to inspect old
blocks through store-gateway. The lookback affects only StoreAPI `Info`
labelset discovery; it does not limit QueryAPI or StoreAPI data queries.

This changes only the label sets shown by Thanos Querier for this endpoint. The
announced label remains a normal series label, and StoreAPI matchers for it are
still forwarded to the backend. This is the recommended mode for Mimir tenant
federation together with `--query.drop-label=__tenant_id__`.

## Backend External Labels

The connector can read `global.external_labels` from a Prometheus-compatible
`/api/v1/status/config` endpoint and apply them the same way Thanos sidecar does:
external labels are advertised in StoreAPI `Info`, attached to returned series,
and removed from StoreAPI matchers before backend queries are sent.

```bash
--query.external-labels
```

By default the connector reads external labels from `--query.target-url`. Use a
separate source URL when the query target is not the Prometheus-compatible source
whose external labels should be applied:

```bash
--query.external-labels \
--query.external-labels-url=http://prometheus:9090
```

Do not enable this against a Mimir tenant-federation endpoint unless its
`/api/v1/status/config` exposes the exact single external label set you want the
connector to advertise.

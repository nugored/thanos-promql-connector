# thanos-promql-connector

Bridges PromQL-compatible HTTP API query backends (e.g. Mimir, Google Managed Prometheus) to Thanos Querier via gRPC (exposing Thanos StoreAPI and QueryAPI).

## Quick Start

```bash
# Run connector against a Prometheus or Mimir backend
go run -tags slicelabels . --query.target-url=http://127.0.0.1:18080/prometheus

# Connect Thanos Querier
thanos query --endpoint 127.0.0.1:8081 --query.mode=distributed
```

---

## Features & Usage

### 1. Google Managed Service for Prometheus (GMP)
Query Cloud Managed Prometheus directly across one or more GCP projects:
```bash
go run -tags slicelabels . \
    --query.gcp-project=my-gcp-project \
    --query.gcp-project=other-gcp-project
```
* Automatically uses Google Application Default Credentials (`GOOGLE_APPLICATION_CREDENTIALS`, gcloud ADC, or GKE Workload Identity).
* Derives target URLs and virtual `prometheus=gcp-<PROJECT_ID>` labels automatically.
* Strips virtual `prometheus` matchers before calling GCP and attaches project labels to returned series.

### 2. Caching & In-Flight Request Deduplication
Optimized to minimize backend requests and Google Monitoring API costs:
* **Singleflight Deduplication**: Concurrent identical requests (`Query`, `QueryRange`, `LabelNames`, `LabelValues`, `Series`) are coalesced into a single backend HTTP call.
* **Metadata Caching**:
  * `--query.label-names-cache-ttl=5m`: Caches `/api/v1/labels` responses (default: 5m, set `0` to disable).
  * `--query.label-values-cache-ttl=5m`: Caches `/api/v1/label/<name>/values` responses (default: 5m, set `0` to disable).
  * `--query.label-cache-ttl=5m`: Caches backend label matcher existence checks for external labels.

### 3. Static & External Labels
* **Static External Labels**: Attach virtual labels to all returned series and use them for routing:
  ```bash
  --query.external-label=env=prod
  ```
* **Backend External Labels**: Read `global.external_labels` from Prometheus `/api/v1/status/config`:
  ```bash
  --query.external-labels [--query.external-labels-url=http://prometheus:9090]
  ```

### 4. Custom Headers & Query Parameters
Inject static headers (e.g., Mimir tenant IDs) or query parameters:
```bash
--query.header="X-Scope-OrgID=tenant1|tenant2"
--query.param="storeMatch[]={cluster=\"prod\"}"
```

### 5. gRPC Server & TLS
Serve Thanos StoreAPI and QueryAPI with optional TLS/mTLS and Snappy compression:
```bash
--connector-address=:8081
--grpc-server-tls-cert=/tls/tls.crt
--grpc-server-tls-key=/tls/tls.key
--grpc-server-tls-client-ca=/tls/ca.crt # Optional for strict mTLS
--grpc-info-api-mode=store # store, query, or both
```

---

## CLI Reference

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--query.target-url` | `""` | PromQL HTTP API backend URL. |
| `--query.gcp-project` | `""` | GCP project ID(s) for Google Managed Prometheus. |
| `--query.header` | `""` | Static headers for backend requests (`Name=Value`). |
| `--query.param` | `""` | Static query parameters (`Name=Value`). |
| `--query.external-label` | `""` | Static virtual external label (`Name=Value`). |
| `--query.drop-label` | `""` | Label(s) to remove from responses. |
| `--query.label-names-cache-ttl` | `5m` | TTL for caching backend `LabelNames` responses. |
| `--query.label-values-cache-ttl` | `5m` | TTL for caching backend `LabelValues` responses. |
| `--query.label-cache-ttl` | `5m` | TTL for caching backend matcher existence checks. |
| `--query.series-step` | `1m` | Default step for StoreAPI series requests. |
| `--query.max-points-per-series` | `11000` | Max backend points per series before step scaling. |
| `--connector-address` | `:8081` | gRPC server listen address. |
| `--metrics-address` | `:9090` | HTTP metrics listen address. |

---

## Build & Test

Build and test using the required `slicelabels` tag:

```bash
make test
make build
```

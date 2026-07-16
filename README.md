# thanos-promql-connector
This tool bridges a PromQL HTTP API query backend to Thanos Querier, exposing a gRPC server that can be queried by [Thanos Querier](https://github.com/thanos-io/thanos).

# How to Run

See the `examples` directory for `docker compose` setups.

## Test and Debugging instructions
1. Start the connector with your desired query target:

```bash
go run . \
    --query.target-url=http://127.0.0.1:18080/prometheus
```

2. Launch Thanos Querier:
```bash
git clone https://github.com/thanos-io/thanos.git
cd thanos
go run ./cmd/thanos query --endpoint :8081 --query.mode distributed --query.promql-engine thanos
```

3. View Thanos Querier UI at address [0.0.0.0:10902](http://localhost:10902/).

## Backend Request Headers

Static headers can be added to every proxied PromQL backend request with `--query.header`.
The flag can be repeated and accepts `Name=Value` or `Name: Value`:

```bash
go run . \
    --query.target-url=http://127.0.0.1:18080/prometheus \
    '--query.header=X-Scope-OrgID=tenant1|tenant2'
```

## Google Auth

Google auth can be enabled with startup parameters:

```bash
go run . \
    --query.target-url=https://monitoring.googleapis.com/v1/projects/<PROJECT_ID>/location/global/prometheus \
    --query.auth.credentials-file=/key.json \
    --query.auth.scope=https://www.googleapis.com/auth/monitoring.read
```

## StoreAPI Series

When Thanos Querier calls the StoreAPI `Series` method, the connector translates
the request into a PromQL HTTP `query_range` call and encodes returned samples as
Thanos XOR chunks. If Thanos does not send a step hint, the connector uses
`--query.series-step`, which defaults to `1m`.

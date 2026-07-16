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

The connector also registers Thanos' `snappy` gRPC compressor, so it can accept
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
```

With a Mimir target URL such as `http://mimir-querier:8080/prometheus`, this
uses the Prometheus-compatible label values endpoint:

```text
/prometheus/api/v1/label/prometheus/values
```

The connector sends the same static headers configured with `--query.header`, so
tenant federation headers such as `X-Scope-OrgID` are included in this request.

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

## FAQ
› Does this mechanism allows to use Mimir as backend for thanos-promql-connector? I use Mimir Querier and it contains metrics from several Prometheuses with external labels set. Will the implemented logic
  work?


• Short answer: not the sidecar-style external-label mechanism.

  For your setup:

  Thanos Querier -> thanos-promql-connector -> Mimir Querier

  with Mimir containing metrics from several Prometheuses, each already ingested with external label prometheus, the correct mechanism is:

  --query.drop-label=__tenant_id__
  --query.announce-label=prometheus

  and do not enable:

  --query.external-labels

  Why: Thanos sidecar external-label logic assumes one backend Prometheus behind one StoreAPI endpoint, with one stable global.external_labels set. Your Mimir Querier is a federated backend containing many
  Prometheus sources, so there is no single external label set the connector should advertise. If the connector advertised one prometheus=... label globally, Thanos would incorrectly treat the whole Mimir-
  backed endpoint as only that one source.

  The announce-label mechanism is different: it reads backend values for the prometheus label and advertises each value as an Info label set, without stripping prometheus matchers or overriding returned series labels.

  Your existing Mimir data already has prometheus on each series. The connector should preserve that per-series label and only drop Mimir’s federation label:

  args:
    - --query.target-url=http://mimir-querier:8080/prometheus
    - '--query.header=X-Scope-OrgID=grazie|jetbrains-ai|k8s|tempo-grazie|tempo-k8s|fus'
    - --query.drop-label=__tenant_id__
    - --query.announce-label=prometheus
    - --query.announce-label-refresh=1m

  That should work as long as prometheus uniquely distinguishes the original Prometheus sources.


› Is it right that now thanos-promql-connector parses external labels from Mimir and responds with this external labels to grpc requests from Thanos Queriers?


• No: it does not parse external labels from Mimir unless you enable the sidecar-style external-label mode.

  The connector only parses and advertises backend external labels if you explicitly start it with:

  --query.external-labels

  Your current deployment args are:

  - --query.target-url=http://mimir-querier:8080/prometheus
  - '--query.header=X-Scope-OrgID=grazie|jetbrains-ai|k8s|tempo-grazie|tempo-k8s|fus'
  - --query.drop-label=__tenant_id__
  - --query.announce-label=prometheus
  - --query.announce-label-refresh=1m

  So it does not parse Mimir external labels from /api/v1/status/config. It reads backend values for the prometheus label and advertises those values in StoreAPI Info.

  It only:

  1. Queries Mimir with your multi-tenant X-Scope-OrgID.
  2. Receives series labels from Mimir.
  3. Drops __tenant_id__.
  4. Keeps your existing per-series prometheus label.
  5. Advertises backend prometheus values as Info label sets.

  That is the behavior you want for Mimir federation. Do not enable --query.external-labels for this multi-Prometheus Mimir backend.


› And --query.external-labels I use... when? In what cases?


• Use --query.external-labels only when the connector is fronting one Prometheus-like backend that has one global external_labels set.

  Good cases:

  Thanos Querier -> thanos-promql-connector -> single Prometheus

  or maybe:

  Thanos Querier -> thanos-promql-connector -> single Prometheus-compatible proxy

  where /api/v1/status/config returns:

  global:
    external_labels:
      prometheus: prom-a
      cluster: prod

  Then the connector can behave like Thanos sidecar: advertise those labels to Thanos, attach them to returned series, and let Thanos route/filter by them.

  Do not use it for your current case:

  Thanos Querier -> connector -> Mimir Querier with many Prometheuses / tenants

  because Mimir represents many sources, not one external label set. For that case, use:

  --query.drop-label=__tenant_id__
  --query.announce-label=prometheus

  and rely on the existing per-series prometheus label already stored in Mimir for query results and matcher routing.

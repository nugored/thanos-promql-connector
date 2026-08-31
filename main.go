package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/run"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/api/query/querypb"
	"github.com/thanos-io/thanos/pkg/info/infopb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"google.golang.org/grpc"

	"main.go/pkg/backend"
	"main.go/pkg/config"
	"main.go/pkg/labelstore"
	"main.go/pkg/server"
	_ "main.go/pkg/snappy"
)

var (
	queryTargetURL      = flag.String("query.target-url", "", "PromQL HTTP API backend URL.")
	queryHeaders        config.HeaderFlags
	queryParams         config.QueryParamFlags
	queryAuthScopes     config.StringListFlag
	queryDropLabels     config.StringListFlag
	queryExternalLabel  config.LabelFlags
	queryGCPProjects    config.StringListFlag
	queryAnnounceLabels config.StringListFlag

	queryAuthGoogle             = flag.Bool("query.auth.google", false, "Enable Google Application Default Credentials for backend requests. Uses credentials from GOOGLE_APPLICATION_CREDENTIALS, gcloud ADC, or the metadata server such as GKE Workload Identity.")
	queryAuthCredentialsFile    = flag.String("query.auth.credentials-file", "", "Google auth credentials file for backend requests.")
	queryExternalLabels         = flag.Bool("query.external-labels", false, "Read and apply global.external_labels from the backend /api/v1/status/config endpoint, matching Thanos sidecar behavior.")
	queryExternalLabelsURL      = flag.String("query.external-labels-url", "", "Prometheus-compatible base URL for reading external labels. Defaults to query.target-url when query.external-labels is enabled.")
	queryExternalLabelsInterval = flag.Duration("query.external-labels.interval", 30*time.Second,
		"How often to refresh external labels when query.external-labels is enabled.")
	queryExternalLabelsTimeout = flag.Duration("query.external-labels.timeout", 5*time.Second,
		"Timeout for each external labels fetch when query.external-labels is enabled.")
	queryAnnounceLabelRefresh = flag.Duration("query.announce-label-refresh", time.Minute,
		"How often to refresh StoreAPI Info label sets from backend label values when query.announce-label is set.")
	queryAnnounceLabelTimeout = flag.Duration("query.announce-label-timeout", 5*time.Second,
		"Timeout for each announced label values fetch when query.announce-label is set.")
	queryAnnounceLabelLookback = flag.Duration("query.announce-label-lookback", 0,
		"Only read announced label values from this recent time range. Set 0 to read without start/end bounds.")
	querySeriesStep = flag.Duration("query.series-step", time.Minute,
		"Fallback step for StoreAPI series queries when Thanos does not send a query step hint.")
	queryMaxPointsPerSeries = flag.Int("query.max-points-per-series", 11000,
		"Maximum backend query_range points per series for StoreAPI series requests. The connector increases the backend step for long ranges when needed. Set 0 to disable connector-side clamping; backend limits still apply.")
	queryLabelCacheTTL = flag.Duration("query.label-cache-ttl", 5*time.Minute,
		"How long to cache backend label matcher search results for external label queries. Set 0 to disable caching.")
	queryLabelNamesCacheTTL = flag.Duration("query.label-names-cache-ttl", 5*time.Minute,
		"How long to cache backend LabelNames search results. Set 0 to disable caching.")
	queryLabelValuesCacheTTL = flag.Duration("query.label-values-cache-ttl", 5*time.Minute,
		"How long to cache backend LabelValues search results. Set 0 to disable caching.")
	connectorAddress = flag.String("connector-address", ":8081",
		"Address on which to expose the query grpc server.")
	grpcServerTLSCertFile = flag.String("grpc-server-tls-cert", "",
		"TLS certificate file for the gRPC server. Requires grpc-server-tls-key when set.")
	grpcServerTLSKeyFile = flag.String("grpc-server-tls-key", "",
		"TLS private key file for the gRPC server. Requires grpc-server-tls-cert when set.")
	grpcServerTLSClientCAFile = flag.String("grpc-server-tls-client-ca", "",
		"Client CA certificate file for mTLS on the gRPC server. When set, client certificates are required and verified.")
	grpcInfoAPIMode = flag.String("grpc-info-api-mode", string(server.InfoAPIModeStore),
		"API support to advertise in the Info response. Valid values: store, query, both.")
	grpcInfoAdvertiseQueryAPI = flag.Bool("grpc-info-advertise-query-api", false,
		"Deprecated: advertise both StoreAPI and QueryAPI support in the Info response when grpc-info-api-mode is left as store.")
	logLevel = flag.String("log.level", "info",
		"Log level. Valid values: debug, info, warn, error. Can also be set via LOG_LEVEL or LOGGING_LEVEL environment variables.")
	metricsAddress = flag.String("metrics-address", ":9090",
		"Address on which to expose metrics")
)

func init() {
	flag.Var(&queryHeaders, "query.header", "Static header to add to backend requests, in Name=Value or Name: Value format. May be repeated.")
	flag.Var(&queryParams, "query.param", "Static query parameter to add to every backend Prometheus API request, in Name=Value format. May be repeated.")
	flag.Var(&queryAuthScopes, "query.auth.scope", "Google auth OAuth scope for backend requests. May be repeated or comma-separated.")
	flag.Var(&queryDropLabels, "query.drop-label", "Label to remove from query and StoreAPI responses. May be repeated or comma-separated.")
	flag.Var(&queryExternalLabel, "query.external-label", "Static external label to announce and attach to every response, in Name=Value format. May be repeated.")
	flag.Var(&queryGCPProjects, "query.gcp-project", "Google Cloud project ID to query through Managed Service for Prometheus. Derives query.target-url and prometheus=gcp-<project> external label. May be repeated or comma-separated.")
	flag.Var(&queryAnnounceLabels, "query.announce-label", "Backend label whose values should be advertised as StoreAPI Info label sets. May be repeated or comma-separated.")
}

func main() {
	flag.Parse()

	logLevelVal := *logLevel
	if envVal := os.Getenv("LOG_LEVEL"); envVal != "" {
		logLevelVal = envVal
	} else if envVal := os.Getenv("LOGGING_LEVEL"); envVal != "" {
		logLevelVal = envVal
	}

	logger := log.NewJSONLogger(log.NewSyncWriter(os.Stderr))
	logger = level.NewFilter(logger, level.Allow(level.ParseDefault(logLevelVal, level.InfoValue())))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	logger = log.With(logger, "caller", log.DefaultCaller)

	var err error
	infoMode, err := server.ParseInfoAPIMode(*grpcInfoAPIMode, *grpcInfoAdvertiseQueryAPI)
	if err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}

	gcpProjects, err := config.NormalizeGCPProjects(queryGCPProjects.Values())
	if err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}
	staticExternalLabels := labels.FromMap(queryExternalLabel.Values())
	announcedLabelNames := queryAnnounceLabels.Values()

	// Configuration Loading.
	queryTargetURLValue := *queryTargetURL
	googleAuth := *queryAuthGoogle
	if len(gcpProjects) > 0 {
		if strings.TrimSpace(*queryTargetURL) != "" {
			level.Error(logger).Log("msg", "query.target-url cannot be used with query.gcp-project because the Google target URL is derived per project")
			os.Exit(1)
		}
		if *queryExternalLabels {
			level.Error(logger).Log("msg", "query.external-labels cannot be used with query.gcp-project because external labels are derived per project")
			os.Exit(1)
		}
		if strings.TrimSpace(*queryExternalLabelsURL) != "" {
			level.Error(logger).Log("msg", "query.external-labels-url cannot be used with query.gcp-project")
			os.Exit(1)
		}
		if len(announcedLabelNames) > 0 {
			level.Error(logger).Log("msg", "query.announce-label cannot be used with query.gcp-project because announced label sets are derived per project")
			os.Exit(1)
		}
		if staticExternalLabels.Get("prometheus") != "" {
			level.Error(logger).Log("msg", "query.external-label=prometheus=... cannot be used with query.gcp-project because prometheus=gcp-<project> is derived per project")
			os.Exit(1)
		}
		queryTargetURLValue, err = config.GooglePrometheusTargetURL(gcpProjects[0])
		if err != nil {
			level.Error(logger).Log("err", err)
			os.Exit(1)
		}
		googleAuth = true
	}

	queryConfig, err := config.NewQueryBackendConfig(queryTargetURLValue, queryHeaders.Values(), queryParams.Values(), googleAuth, *queryAuthCredentialsFile, queryAuthScopes.Values())
	if err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}
	// Query client setup.
	externalLabels := labelstore.NewExternalLabelsStore()
	externalLabelsForRequests := func() labels.Labels {
		return labelpb.ExtendSortedLabels(externalLabels.Labels(), staticExternalLabels)
	}
	if staticExternalLabels.Len() > 0 {
		level.Info(logger).Log("msg", "configured static external labels", "external_labels", staticExternalLabels.String())
	}

	var queryBackends []backend.QueryBackendEndpoint
	var queryBackendClient backend.QueryBackendAPI
	var externalLabelsClient backend.QueryBackendAPI
	if len(gcpProjects) > 0 {
		queryBackends, err = backend.NewGCPQueryBackends(*queryConfig, gcpProjects, staticExternalLabels)
		if err != nil {
			level.Error(logger).Log("msg", "Error creating Google project clients", "err", err)
			os.Exit(1)
		}
		level.Info(logger).Log("msg", "configured Google Managed Service for Prometheus projects", "projects", strings.Join(gcpProjects, ","), "auth_google", queryConfig.Auth.Google)
	} else {
		queryBackendClient, err = backend.CreateQueryBackendClient(*queryConfig)
		if err != nil {
			level.Error(logger).Log("msg", "Error creating client", "err", err)
			os.Exit(1)
		}
		queryBackends = []backend.QueryBackendEndpoint{{
			Name:           "backend",
			QueryBackend:   queryConfig.QueryTargetURL,
			Client:         queryBackendClient,
			ExternalLabels: externalLabelsForRequests,
		}}

		externalLabelsClient = queryBackendClient
		if *queryExternalLabelsURL != "" {
			externalLabelsConfig := *queryConfig
			externalLabelsConfig.QueryTargetURL = *queryExternalLabelsURL
			externalLabelsClient, err = backend.CreateQueryBackendClient(externalLabelsConfig)
			if err != nil {
				level.Error(logger).Log("msg", "Error creating external labels client", "err", err)
				os.Exit(1)
			}
		}
	}

	announcedLabelSets := labelstore.NewAnnouncedLabelSetsStore()
	var announcedLabelSources []labelstore.AnnouncedLabelSource

	if *queryExternalLabels {
		ctx, cancel := context.WithTimeout(context.Background(), *queryExternalLabelsTimeout)
		err = externalLabels.UpdateFromBackend(ctx, externalLabelsClient)
		cancel()
		if err != nil {
			level.Error(logger).Log("msg", "Error loading initial external labels", "err", err)
			os.Exit(1)
		}
		if externalLabels.Labels().Len() == 0 {
			level.Error(logger).Log("msg", "no external labels configured on backend")
			os.Exit(1)
		}
		level.Info(logger).Log("msg", "successfully loaded backend external labels", "external_labels", externalLabels.Labels().String())
	}
	if len(announcedLabelNames) > 0 {
		if *queryAnnounceLabelRefresh <= 0 {
			level.Error(logger).Log("msg", "query.announce-label-refresh must be greater than zero")
			os.Exit(1)
		}
		if *queryAnnounceLabelTimeout <= 0 {
			level.Error(logger).Log("msg", "query.announce-label-timeout must be greater than zero")
			os.Exit(1)
		}
		if *queryAnnounceLabelLookback < 0 {
			level.Error(logger).Log("msg", "query.announce-label-lookback must be greater than or equal to zero")
			os.Exit(1)
		}

		announcedLabelSources, err = labelstore.CreateAnnouncedLabelSources(*queryConfig)
		if err != nil {
			level.Error(logger).Log("msg", "Error creating announced label sources", "err", err)
			os.Exit(1)
		}

		failures, err := announcedLabelSets.UpdateFromSources(context.Background(), announcedLabelSources, announcedLabelNames, *queryAnnounceLabelTimeout, *queryAnnounceLabelLookback)
		labelstore.LogAnnouncedLabelSourceFailures(logger, failures)
		if err != nil {
			level.Error(logger).Log("msg", "Error loading initial announced label sets", "err", err)
			os.Exit(1)
		}
		if len(announcedLabelSets.LabelSets()) == 0 {
			level.Error(logger).Log("msg", "no announced label values found on backend", "labels", fmt.Sprint(announcedLabelNames))
			os.Exit(1)
		}
		level.Info(logger).Log("msg", "successfully loaded backend announced label sets", "labels", fmt.Sprint(announcedLabelNames), "sources", fmt.Sprint(labelstore.AnnouncedLabelSourceNames(announcedLabelSources)), "lookback", queryAnnounceLabelLookback.String(), "label_sets", fmt.Sprint(announcedLabelSets.LabelSets()))
	}

	queryBackendDescription := queryConfig.QueryTargetURL
	infoExternalLabels := externalLabelsForRequests
	infoAnnouncedLabelSets := announcedLabelSets.LabelSets
	if len(gcpProjects) > 0 {
		queryBackendDescription = "gcp-projects:" + strings.Join(gcpProjects, ",")
		infoExternalLabels = labels.EmptyLabels
		infoAnnouncedLabelSets = func() []labels.Labels {
			return backend.QueryBackendLabelSets(queryBackends)
		}
	}

	var g run.Group
	{
		term := make(chan os.Signal, 1)
		cancel := make(chan struct{})
		signal.Notify(term, os.Interrupt, syscall.SIGTERM)

		g.Add(
			func() error {
				select {
				case <-term:
					level.Info(logger).Log("msg", "received SIGTERM, exiting gracefully...")
				case <-cancel:
				}
				return nil
			},
			func(err error) {
				close(cancel)
			},
		)
	}
	if *queryExternalLabels {
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			ticker := time.NewTicker(*queryExternalLabelsInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					iterCtx, iterCancel := context.WithTimeout(context.Background(), *queryExternalLabelsTimeout)
					err := externalLabels.UpdateFromBackend(iterCtx, externalLabelsClient)
					iterCancel()
					if err != nil {
						level.Warn(logger).Log("msg", "updating external labels failed", "err", err)
						continue
					}
					level.Info(logger).Log("msg", "updated backend external labels", "external_labels", externalLabels.Labels().String())
				case <-ctx.Done():
					return nil
				}
			}
		}, func(err error) {
			cancel()
		})
	}
	if len(announcedLabelNames) > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			ticker := time.NewTicker(*queryAnnounceLabelRefresh)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					failures, err := announcedLabelSets.UpdateFromSources(ctx, announcedLabelSources, announcedLabelNames, *queryAnnounceLabelTimeout, *queryAnnounceLabelLookback)
					labelstore.LogAnnouncedLabelSourceFailures(logger, failures)
					if err != nil {
						level.Warn(logger).Log("msg", "updating announced label sets failed", "err", err)
						continue
					}
					level.Info(logger).Log("msg", "updated backend announced label sets", "labels", fmt.Sprint(announcedLabelNames), "sources", fmt.Sprint(labelstore.AnnouncedLabelSourceNames(announcedLabelSources)), "lookback", queryAnnounceLabelLookback.String(), "label_sets", fmt.Sprint(announcedLabelSets.LabelSets()))
				case <-ctx.Done():
					return nil
				}
			}
		}, func(err error) {
			cancel()
		})
	}
	{
		// grpc server.
		listener, err := net.Listen("tcp", *connectorAddress)
		if err != nil {
			panic(err)
		}
		serverOptions, tlsEnabled, err := server.NewGRPCServerOptions(server.GRPCServerTLSConfig{
			CertFile:     *grpcServerTLSCertFile,
			KeyFile:      *grpcServerTLSKeyFile,
			ClientCAFile: *grpcServerTLSClientCAFile,
		})
		if err != nil {
			level.Error(logger).Log("msg", "Error creating grpc server TLS config", "err", err)
			os.Exit(1)
		}
		grpcServer := grpc.NewServer(serverOptions...)
		queryServer := server.NewQueryServerWithCacheTTLs(logger, queryBackends, queryDropLabels.Values(), *querySeriesStep, *queryMaxPointsPerSeries, *queryLabelCacheTTL, *queryLabelNamesCacheTTL, *queryLabelValuesCacheTTL)
		storepb.RegisterStoreServer(grpcServer, queryServer)
		querypb.RegisterQueryServer(grpcServer, queryServer)
		infopb.RegisterInfoServer(grpcServer, &server.InfoServer{
			QueryBackend:       queryBackendDescription,
			ExternalLabels:     infoExternalLabels,
			AnnouncedLabelSets: infoAnnouncedLabelSets,
			APIMode:            infoMode,
		})
		g.Add(func() error {
			level.Info(logger).Log("msg", "Starting grpc server for query endpoint", "listen", *connectorAddress, "tls", tlsEnabled, "mtls", *grpcServerTLSClientCAFile != "")
			return grpcServer.Serve(listener)
		}, func(err error) {
			grpcServer.GracefulStop()
		})
	}
	{
		// http server.
		ctx, cancel := context.WithCancel(context.Background())
		webServer := &http.Server{Addr: *metricsAddress, Handler: server.NewWebHandler()}

		g.Add(func() error {
			level.Info(logger).Log("msg", "Starting web server", "listen", *metricsAddress)
			return webServer.ListenAndServe()
		}, func(err error) {
			webServer.Shutdown(ctx)
			cancel()
		})
	}

	if err := g.Run(); err != nil {
		level.Error(logger).Log("msg", "running reloader failed", "err", err)
		os.Exit(1)
	}
}
/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	"github.com/yelzhy/kubestream/internal/controller"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

// defaultWatchedGVKs is used when WATCHED_GVKS/--watched-gvks is unset or
// empty, so the operator watches something sensible out of the box.
const defaultWatchedGVKs = "v1/Pod,apps/v1/Deployment,v1/Service"

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	// +kubebuilder:scaffold:scheme
}

// getEnvOrDefault returns the value of the named environment variable, or
// def if it is unset. Used to let flags fall back to env vars (e.g. for
// ConfigMap/Secret-projected settings) while keeping flag overrides working.
func getEnvOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// getEnvDurationOrDefault is getEnvOrDefault for time.Duration flags. An
// unparsable value falls back to def rather than failing startup.
//
// This runs as a flag default-value expression, evaluated before
// flag.Parse()/ctrl.SetLogger() in main() — setupLog isn't wired to a real
// sink yet at this point, so a warning logged through it here would be
// silently discarded. fmt.Fprintf to stderr is used instead so a
// misconfigured env var is actually visible.
func getEnvDurationOrDefault(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kubestream: invalid duration %q for env var %s, using default %s: %v\n", v, key, def, err)
		return def
	}
	return d
}

// getEnvIntOrDefault is getEnvOrDefault for int flags. An unparsable value
// falls back to def rather than failing startup. See getEnvDurationOrDefault
// for why this logs via stderr rather than setupLog.
func getEnvIntOrDefault(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kubestream: invalid integer %q for env var %s, using default %d: %v\n", v, key, def, err)
		return def
	}
	return n
}

// getEnvBoolOrDefault is getEnvOrDefault for bool flags. An unparsable value
// falls back to def rather than failing startup. See getEnvDurationOrDefault
// for why this logs via stderr rather than setupLog.
func getEnvBoolOrDefault(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kubestream: invalid boolean %q for env var %s, using default %t: %v\n", v, key, def, err)
		return def
	}
	return b
}

// parseGVKList parses a comma-separated list of GroupVersionKinds, each
// given as "version/kind" (core group, e.g. "v1/Pod") or
// "group/version/kind" (e.g. "apps/v1/Deployment",
// "networking.k8s.io/v1/Ingress"), into the GVKs the operator should watch.
// Externalizing this list — rather than a hardcoded Go slice in
// resourcestream_controller.go — is what lets the operator watch a
// different set of resource types, including CRDs, purely through
// configuration; see ReconcilerConfig.WatchedGVKs.
//
// Returns an error naming the malformed entry on invalid input, rather than
// silently skipping it — a typo here should fail startup loudly, not
// quietly watch fewer resource types than intended.
func parseGVKList(raw string) ([]schema.GroupVersionKind, error) {
	var gvks []schema.GroupVersionKind
	for entry := range strings.SplitSeq(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		var gvk schema.GroupVersionKind
		switch parts := strings.Split(entry, "/"); len(parts) {
		case 2:
			gvk = schema.GroupVersionKind{Version: parts[0], Kind: parts[1]}
		case 3:
			gvk = schema.GroupVersionKind{Group: parts[0], Version: parts[1], Kind: parts[2]}
		default:
			return nil, fmt.Errorf(`invalid GVK %q: expected "version/kind" or "group/version/kind"`, entry)
		}
		if gvk.Version == "" || gvk.Kind == "" {
			return nil, fmt.Errorf("invalid GVK %q: version and kind must not be empty", entry)
		}
		gvks = append(gvks, gvk)
	}
	return gvks, nil
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var tlsOpts []func(*tls.Config)
	var chAddr, chDatabase, chUsername string
	var chDialTimeout, chReadTimeout time.Duration
	var chAutoCreateSchema bool
	flag.StringVar(&chAddr, "ch-addr", getEnvOrDefault("CH_ADDR", "127.0.0.1:9000"),
		"The ClickHouse server address (host:port). Can also be set via the CH_ADDR env var.")
	flag.StringVar(&chDatabase, "ch-database", getEnvOrDefault("CH_DATABASE", "kubestream"),
		"The ClickHouse database name. Can also be set via the CH_DATABASE env var.")
	flag.StringVar(&chUsername, "ch-username", getEnvOrDefault("CH_USERNAME", "default"),
		"The ClickHouse username. Can also be set via the CH_USERNAME env var.")
	flag.DurationVar(&chDialTimeout, "ch-dial-timeout", getEnvDurationOrDefault("CH_DIAL_TIMEOUT", 5*time.Second),
		"Timeout for establishing the ClickHouse connection. Can also be set via the CH_DIAL_TIMEOUT env var.")
	flag.DurationVar(&chReadTimeout, "ch-read-timeout", getEnvDurationOrDefault("CH_READ_TIMEOUT", 10*time.Second),
		"Timeout for a single ClickHouse query/insert round-trip. Can also be set via the CH_READ_TIMEOUT env var.")
	flag.BoolVar(&chAutoCreateSchema, "ch-auto-create-schema", getEnvBoolOrDefault("CH_AUTO_CREATE_SCHEMA", false),
		"If set, execute the shipped ClickHouse DDL (deploy/clickhouse/schema) idempotently at connect time. "+
			"Defaults to false. Can also be set via the CH_AUTO_CREATE_SCHEMA env var.")
	// CH_PASSWORD is intentionally env-only (no flag): flag values are
	// visible in `ps`/process listings, which a Secret-projected env var
	// avoids.
	var clusterID string
	var maxConcurrentReconciles int
	var watchedGVKsRaw string
	flag.StringVar(&clusterID, "cluster-id", getEnvOrDefault("CLUSTER_ID", "local-kind-cluster"),
		"Identifier for this cluster, recorded on every row written to ClickHouse. "+
			"Can also be set via the CLUSTER_ID env var.")
	flag.IntVar(&maxConcurrentReconciles, "reconciler-max-concurrent", getEnvIntOrDefault("RECONCILER_MAX_CONCURRENT", 5),
		"Maximum concurrent Reconciles per watched resource type. Can also be set via the RECONCILER_MAX_CONCURRENT env var.")
	flag.StringVar(&watchedGVKsRaw, "watched-gvks", getEnvOrDefault("WATCHED_GVKS", defaultWatchedGVKs),
		"Comma-separated list of resource types to watch, each as \"version/kind\" or \"group/version/kind\" "+
			"(e.g. \"v1/Pod,apps/v1/Deployment,networking.k8s.io/v1/Ingress\"). Adding a type outside the "+
			"operator's default RBAC grant also requires extending config/rbac/role.yaml. Can also be set via "+
			"the WATCHED_GVKS env var.")
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("Disabling HTTP/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts
	webhookServerOptions := webhook.Options{
		TLSOpts: webhookTLSOpts,
	}

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		webhookServerOptions.CertDir = webhookCertPath
		webhookServerOptions.CertName = webhookCertName
		webhookServerOptions.KeyName = webhookCertKey
	}

	webhookServer := webhook.NewServer(webhookServerOptions)

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		metricsServerOptions.CertDir = metricsCertPath
		metricsServerOptions.CertName = metricsCertName
		metricsServerOptions.KeyName = metricsCertKey
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "885d930f.kubestream.io",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	chConfig := controller.ClickHouseConfig{
		Addr:             chAddr,
		Database:         chDatabase,
		Username:         chUsername,
		Password:         os.Getenv("CH_PASSWORD"),
		DialTimeout:      chDialTimeout,
		ReadTimeout:      chReadTimeout,
		AutoCreateSchema: chAutoCreateSchema,
	}
	if chConfig.Password == "" {
		setupLog.Info("CH_PASSWORD is not set; connecting to ClickHouse without a password")
	}

	watchedGVKs, err := parseGVKList(watchedGVKsRaw)
	if err != nil {
		setupLog.Error(err, "Invalid --watched-gvks/WATCHED_GVKS configuration")
		os.Exit(1)
	}
	if len(watchedGVKs) == 0 {
		setupLog.Error(fmt.Errorf("--watched-gvks/WATCHED_GVKS resolved to an empty list"),
			"The operator must watch at least one resource type")
		os.Exit(1)
	}

	reconcilerConfig := controller.ReconcilerConfig{
		ClusterID:               clusterID,
		MaxConcurrentReconciles: maxConcurrentReconciles,
		WatchedGVKs:             watchedGVKs,
	}

	if err := (&controller.ResourceStreamReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr, chConfig, reconcilerConfig); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "resourcestream")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}

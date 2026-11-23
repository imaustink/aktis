// The mcp-broker process (docs/adr/0045).
//
// Two jobs in one Deployment, the only component that speaks MCP:
//
//   - DISCOVERY (a controller): watch MCPServer CRs, list each server's tools,
//     publish them to status, and materialize the EXPOSED ones as derived
//     MCPTool CRs — on load, on change, and on a periodic resync.
//   - INVOCATION (an HTTP API): proxy one tools/call to a server under the
//     caller's delegated token.
//
// A separate Deployment from the engines on purpose: the MCP wire protocol, its
// sessions and its outbound egress stay out of the deterministic agent loop,
// which keeps a malformed or hostile server from reaching the loop's process,
// secrets or cluster identity.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/auth"
	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/discovery"
	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/mcpclient"
	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/registry"
	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/resource"
	"github.com/aktisai/orchestrator/apps/mcp-broker/internal/server"
)

// config is everything the process reads from its environment.
type config struct {
	namespace         string
	group             string
	version           string
	port              int
	discoveryInterval time.Duration
	orchestratorToken string
}

func loadConfig(getenv func(string) string) (config, error) {
	cfg := config{
		namespace: envOr(getenv, "NAMESPACE", "default"),
		group:     envOr(getenv, "CRD_GROUP", resource.Group),
		version:   envOr(getenv, "CRD_VERSION", resource.Version),
	}
	port, err := strconv.Atoi(envOr(getenv, "PORT", "8080"))
	if err != nil || port <= 0 || port > 65535 {
		return cfg, fmt.Errorf("PORT must be a TCP port, got %q", getenv("PORT"))
	}
	cfg.port = port
	intervalMs, err := strconv.ParseInt(envOr(getenv, "DISCOVERY_INTERVAL_MS", "300000"), 10, 64)
	if err != nil || intervalMs <= 0 {
		return cfg, fmt.Errorf("DISCOVERY_INTERVAL_MS must be a positive number of milliseconds, got %q", getenv("DISCOVERY_INTERVAL_MS"))
	}
	cfg.discoveryInterval = time.Duration(intervalMs) * time.Millisecond
	// Fail at startup rather than on the first request: a broker running
	// without its token would reject everything and look like a networking
	// problem.
	cfg.orchestratorToken = getenv("ORCHESTRATOR_TOKEN")
	if cfg.orchestratorToken == "" {
		return cfg, errors.New("ORCHESTRATOR_TOKEN is required")
	}
	return cfg, nil
}

func envOr(getenv func(string) string, key, fallback string) string {
	if v := getenv(key); v != "" {
		return v
	}
	return fallback
}

// kubeConfig is in-cluster when deployed, KUBECONFIG/~/.kube/config locally.
func kubeConfig() (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
}

func main() {
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		log.Fatal(err)
	}
	if err := run(cfg); err != nil {
		log.Fatalf("mcp-broker failed to start: %v", err)
	}
}

func run(cfg config) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	restConfig, err := kubeConfig()
	if err != nil {
		return fmt.Errorf("load kube config: %w", err)
	}
	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("build dynamic client: %w", err)
	}
	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("build core client: %w", err)
	}

	client := &mcpclient.SDKClient{}

	toolWriter := discovery.NewToolWriter(discovery.ToolWriterOptions{
		Dynamic: dynamicClient, Namespace: cfg.namespace, Group: cfg.group, Version: cfg.version,
		OnError: func(tool string, err error) { log.Printf("Could not write MCPTool %s: %v", tool, err) },
	})
	statusWriter := discovery.NewStatusWriter(discovery.StatusWriterOptions{
		Dynamic: dynamicClient, Namespace: cfg.namespace, Group: cfg.group, Version: cfg.version,
		// Reported, never fatal: the discovery already happened and the tools
		// are already materialized; failing because the status write did not
		// land would throw away real work to protect a status blob.
		OnError: func(server string, err error) {
			log.Printf("Could not publish status for MCPServer %s: %v", server, err)
		},
	})
	disco := discovery.New(discovery.Options{
		Client: client, ToolWriter: toolWriter, StatusWriter: statusWriter,
		OnError: func(server string, err error) { log.Printf("Discovery for MCPServer %s failed: %v", server, err) },
	})

	reg := registry.NewCRD(registry.CRDOptions{
		Namespace: cfg.namespace, Group: cfg.group, Version: cfg.version,
		Dynamic: dynamicClient, Secrets: registry.KubeSecretReader(kubeClient),
		// A changed server is re-discovered immediately; the periodic resync
		// below is only the backstop for a change the watch never delivered.
		OnChange: func(b registry.Binding) { go disco.RunServer(ctx, b) },
		OnError:  func(server string, err error) { log.Printf("MCPServer %s could not be bound: %v", server, err) },
	})
	if err := reg.Start(ctx); err != nil {
		return fmt.Errorf("load MCPServers: %w", err)
	}
	log.Printf("bound %d MCPServer(s) in %s", len(reg.List()), cfg.namespace)

	// Initial sweep after load, then on the resync interval. In the
	// background, so one slow or unreachable server cannot hold the HTTP API
	// (and its health probe) down.
	go func() {
		disco.RunAll(ctx, reg.List())
		ticker := time.NewTicker(cfg.discoveryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				reg.Refresh(ctx)
				disco.RunAll(ctx, reg.List())
			}
		}
	}()

	httpServer := &http.Server{
		Addr: fmt.Sprintf(":%d", cfg.port),
		Handler: server.New(server.Options{
			Auth:     auth.Config{OrchestratorToken: cfg.orchestratorToken},
			Registry: reg,
			Client:   client,
		}),
		ReadHeaderTimeout: 10 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		log.Printf("mcp-broker listening on %d", cfg.port)
		serveErr <- httpServer.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

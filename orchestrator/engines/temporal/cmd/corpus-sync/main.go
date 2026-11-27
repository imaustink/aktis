// corpus-sync indexes each Corpus into its Qdrant collection, from one
// long-lived Temporal workflow per Corpus (docs/adr/0059). It reads sources
// only through connection-broker's HTTP API, with each Corpus's own sync
// token: the third-party credentials stay in the broker (docs/adr/0038 §3).
package main

import (
	"context"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/qdrant/go-client/qdrant"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"k8s.io/client-go/dynamic"

	"github.com/aktisai/orchestrator/engines/temporal/internal/corpussync"
	"github.com/aktisai/orchestrator/engines/temporal/internal/kubeconfig"
	"github.com/aktisai/orchestrator/engines/temporal/internal/llm"
	"github.com/aktisai/orchestrator/engines/temporal/internal/telemetry"
	"github.com/aktisai/orchestrator/engines/temporal/internal/temporal"
	"github.com/aktisai/orchestrator/engines/temporal/internal/vectorstore"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing := telemetry.Setup(context.Background(), "temporal-engine-corpus-sync")
	defer func() { _ = shutdownTracing(context.Background()) }()

	brokerURL := required("BROKER_URL")
	tokens := corpussync.SyncTokensFromEnv()
	if len(tokens) == 0 {
		// Not fatal: a token added later arrives with a restart, and the
		// legacy drain still has work to do.
		log.Printf("no SYNC_TOKEN_* configured: no Corpus will be indexed")
	}

	embedder := corpussync.BatchingEmbedder{Inner: llm.NewEmbedder(
		getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
		required("OPENAI_API_KEY"),
		getenv("EMBEDDING_MODEL", llm.DefaultEmbedModel),
	)}

	qdrantClient, err := dialQdrant(required("QDRANT_URL"))
	if err != nil {
		log.Fatalf("qdrant: %v", err)
	}
	defer qdrantClient.Close()

	kubeConfig, err := kubeconfig.Load()
	if err != nil {
		log.Fatalf("load kube config: %v", err)
	}
	dynamicClient, err := dynamic.NewForConfig(kubeConfig)
	if err != nil {
		log.Fatalf("build dynamic client: %v", err)
	}
	corpora := &corpussync.Corpora{
		Client:    dynamicClient,
		Namespace: getenv("NAMESPACE", "default"),
		Tokens:    tokens,
		OnMissingToken: func(corpus string) {
			log.Printf("corpus %s has no SYNC_TOKEN_* configured: not indexing it", corpus)
		},
	}

	cfg := temporal.ConfigFromEnv()
	cfg.TaskQueue = getenv("CORPUS_SYNC_TASK_QUEUE", corpussync.DefaultTaskQueue)
	c, err := temporal.NewClient(cfg)
	if err != nil {
		log.Fatalf("dial temporal at %s: %v", cfg.Address, err)
	}
	defer c.Close()

	// A pass holds an activity slot for its whole length, so the slots bound
	// how many corpora index at once on one replica; the rest wait for
	// another.
	w := worker.New(c, cfg.TaskQueue, worker.Options{
		MaxConcurrentActivityExecutionSize: 20,
		WorkerStopTimeout:                  10 * time.Second,
	})
	w.RegisterWorkflowWithOptions(corpussync.CorpusSyncWorkflow, workflow.RegisterOptions{Name: corpussync.WorkflowName})
	acts := &corpussync.Activities{
		Corpora: corpora,
		SourceFor: func(s *corpussync.Synced) corpussync.Source {
			return corpussync.NewHTTPSource(brokerURL, s.Token)
		},
		WriterFor: func(s *corpussync.Synced) corpussync.Writer {
			return &corpussync.QdrantWriter{
				Store: func(collection string) corpussync.PointStore {
					return vectorstore.NewQdrant(qdrantClient, collection, embedder, llm.DefaultEmbedDims)
				},
				AllowedRoles: s.AllowedRoles,
			}
		},
	}
	w.RegisterActivityWithOptions(acts.SyncTarget, activity.RegisterOptions{Name: corpussync.SyncTargetActivityName})
	w.RegisterActivityWithOptions(acts.RunPass, activity.RegisterOptions{Name: corpussync.RunPassActivityName})

	if err := w.Start(); err != nil {
		log.Fatalf("start worker: %v", err)
	}
	defer w.Stop()

	ensurer := &corpussync.Ensurer{
		Client:          c,
		TaskQueue:       cfg.TaskQueue,
		LegacyTaskQueue: getenvAllowEmpty("LEGACY_TASK_QUEUE", "connection-broker"),
		Synced:          corpora.List,
		All:             corpora.ListAll,
	}
	log.Printf("corpus-sync: indexing corpora in %s on Temporal %s (namespace %s, task queue %s), reading through %s",
		corpora.Namespace, cfg.Address, cfg.Namespace, cfg.TaskQueue, brokerURL)
	ensurer.Run(ctx, time.Minute)
}

// dialQdrant takes the broker's REST-style QDRANT_URL ("http://host:6333")
// and speaks gRPC to the same host, like the engine (docs/adr/0056).
func dialQdrant(raw string) (*qdrant.Client, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		log.Fatalf("invalid QDRANT_URL %q", raw)
	}
	port, err := strconv.Atoi(getenv("QDRANT_GRPC_PORT", "6334"))
	if err != nil {
		log.Fatalf("invalid QDRANT_GRPC_PORT: %v", err)
	}
	return qdrant.NewClient(&qdrant.Config{
		Host:   u.Hostname(),
		Port:   port,
		APIKey: os.Getenv("QDRANT_API_KEY"),
		UseTLS: u.Scheme == "https",
	})
}

func required(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("%s is required", key)
	}
	return v
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// getenvAllowEmpty distinguishes unset (the default) from set-to-empty (off).
func getenvAllowEmpty(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

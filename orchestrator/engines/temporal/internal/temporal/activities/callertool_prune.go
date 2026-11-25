package activities

import (
	"context"

	"github.com/qdrant/go-client/qdrant"

	"github.com/aktisai/orchestrator/engines/temporal/internal/callertools"
)

// PruneCallerToolsActivityName drops caller-supplied tool definitions nobody
// has sent for a while (docs/adr/0035, 0058 §4).
const PruneCallerToolsActivityName = "PruneCallerTools"

// PruneCallerToolsInput names the collection, which the gateway that owns it
// chose, and how long an unused definition survives.
type PruneCallerToolsInput struct {
	Collection       string `json:"collection"`
	RetentionSeconds int64  `json:"retentionSeconds"`
}

// CallerToolPruneActivities runs on the worker, against the same Qdrant the
// gateway indexes caller tools into (the chart gives both the same Qdrant
// settings).
type CallerToolPruneActivities struct {
	Client *qdrant.Client
}

// PruneCallerTools reports how many definitions it removed. Idempotent: a
// retry only finds fewer stale points.
func (a *CallerToolPruneActivities) PruneCallerTools(ctx context.Context, in PruneCallerToolsInput) (int, error) {
	// Pruning never embeds, so the store needs no embedder.
	return callertools.NewQdrantStore(a.Client, in.Collection, nil, 0).Prune(ctx, in.RetentionSeconds)
}

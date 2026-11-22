// Package activities holds all non-deterministic work invoked from
// workflows. Workflow code may import the types and name constants here,
// but never the implementations.
package activities

import (
	"context"
	"time"

	"github.com/aktisai/orchestrator/engines/temporal/internal/attachments"
	"github.com/aktisai/orchestrator/engines/temporal/internal/llm"
)

const CompleteTurnActivityName = "CompleteTurn"

type CompleteTurnInput struct {
	SystemPrompt string        `json:"systemPrompt"`
	Messages     []llm.Message `json:"messages"`
	// Images are attached to the LAST message (the user's turn) for a vision
	// model. Refs only: the bytes are fetched here, never carried in workflow
	// history (see package attachments).
	Images []attachments.Ref `json:"images,omitempty"`
}

type LLMActivities struct {
	Client *llm.Client
	// Attachments resolves Images. Nil: a turn carrying stored images fails
	// loudly rather than being answered as if the image were not there.
	Attachments *attachments.Resolver
}

// CompleteTurn runs one plain chat completion over the conversation so far.
func (a *LLMActivities) CompleteTurn(ctx context.Context, in CompleteTurnInput) (string, error) {
	messages := make([]llm.Message, 0, len(in.Messages)+1)
	if in.SystemPrompt != "" {
		messages = append(messages, llm.Message{Role: "system", Content: in.SystemPrompt})
	}
	messages = append(messages, in.Messages...)
	if len(in.Images) > 0 && len(in.Messages) > 0 {
		images, err := a.Attachments.DataURLs(ctx, in.Images)
		if err != nil {
			return "", err
		}
		messages[len(messages)-1].Images = images
	}
	return a.Client.Complete(ctx, messages)
}

const PresignAttachmentsActivityName = "PresignAttachments"

// DefaultPresignTTL is how long an agent pod's attachment links stay valid:
// long enough for a queued pod to start and fetch them.
const DefaultPresignTTL = 24 * time.Hour

// AttachmentActivities hands image attachments to consumers outside the
// worker. Presigning is an activity, not workflow code, because its output (a
// time-limited URL signed with live credentials) is not deterministic.
type AttachmentActivities struct {
	Attachments *attachments.Resolver
}

// PresignAttachments returns a download URL for each image, for an agent pod
// that receives its goal as text. Stored images are presigned; external ones
// pass through.
func (a *AttachmentActivities) PresignAttachments(ctx context.Context, refs []attachments.Ref) ([]string, error) {
	return a.Attachments.DownloadURLs(ctx, refs)
}

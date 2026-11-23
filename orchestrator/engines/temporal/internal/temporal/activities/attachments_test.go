package activities_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/aktisai/orchestrator/engines/temporal/internal/attachments"
	"github.com/aktisai/orchestrator/engines/temporal/internal/llm"
	"github.com/aktisai/orchestrator/engines/temporal/internal/temporal/activities"
)

// A real 1x1 PNG, so the inlined data URL is a genuine image.
var onePixelPNG, _ = base64.StdEncoding.DecodeString(
	"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==")

// imageStore holds the objects it was given; any other key is gone (as if a
// bucket lifecycle rule had expired it).
type imageStore map[string][]byte

func (s imageStore) Put(context.Context, string, string, []byte) error { return nil }
func (s imageStore) Get(_ context.Context, key string) ([]byte, error) {
	if data, ok := s[key]; ok {
		return data, nil
	}
	return nil, errors.New("NoSuchKey")
}
func (s imageStore) Presign(context.Context, string, time.Duration) (string, error) { return "", nil }

// imageLLM records the images on the user message it was sent.
type imageLLM struct{ images []string }

func (f *imageLLM) Complete(context.Context, []llm.Message) (string, error) { return "", nil }
func (f *imageLLM) CompleteJSON(_ context.Context, messages []llm.Message, _ llm.ResponseSchema) (json.RawMessage, error) {
	for _, m := range messages {
		if m.Role == "user" {
			f.images = m.Images
		}
	}
	return json.RawMessage(`{"action":"respond","response":"ok"}`), nil
}

var (
	livePhoto    = attachments.Ref{Key: "attachments/s/live.png", MediaType: "image/png"}
	expiredPhoto = attachments.Ref{Key: "attachments/s/expired.png", MediaType: "image/png"}
)

func imageActivities(fake *imageLLM) *activities.AgentLoopActivities {
	store := imageStore{livePhoto.Key: onePixelPNG}
	return &activities.AgentLoopActivities{LLM: fake, Attachments: &attachments.Resolver{Store: store}}
}

// An earlier turn's image that can no longer be loaded is skipped: the
// follow-up is still answered, with the images that do load.
func TestPlanActionSkipsAnEarlierImageThatFailsToLoad(t *testing.T) {
	fake := &imageLLM{}
	plan, err := imageActivities(fake).PlanAction(context.Background(), activities.PlanActionInput{
		Request:       "and what about this one?",
		Images:        []attachments.Ref{livePhoto},
		ContextImages: []attachments.Ref{expiredPhoto, livePhoto},
	})
	require.NoError(t, err)
	require.Equal(t, "ok", plan.Response)
	dataURL := "data:image/png;base64," + base64.StdEncoding.EncodeToString(onePixelPNG)
	require.Equal(t, []string{dataURL, dataURL}, fake.images, "this turn's image first, then the earlier one that loaded")
}

// An image attached to THIS turn still fails loudly: answering as if the user
// had shown nothing would be wrong.
func TestPlanActionFailsWhenThisTurnsImageFailsToLoad(t *testing.T) {
	_, err := imageActivities(&imageLLM{}).PlanAction(context.Background(), activities.PlanActionInput{
		Request: "what is this?",
		Images:  []attachments.Ref{expiredPhoto},
	})
	require.ErrorContains(t, err, "NoSuchKey")
}

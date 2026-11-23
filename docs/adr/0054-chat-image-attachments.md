# 0054. Chat image attachments: upload once to S3, carry references through Temporal

Date: 2026-11-22

## Status

Proposed.

## Context

Open WebUI sends any message carrying a picture in OpenAI's multi-part form:
`content` is an array of `{"type":"text"}` and `{"type":"image_url"}` parts
instead of a string. The facade (`agent-orchestrator`) read only string
content. An image-bearing user message therefore looked empty, and the turn
was refused with `messages must include a non-empty "user" message`. The
engine's own chat endpoint tolerated the array but kept only the text, so the
image was silently dropped. Nothing downstream could carry an image anyway:
`llm.Message.Content` is a string, a skill's planner sees one request string,
and an agent pod receives its goal as text.

The motivating case: extracting a TikTok recipe whose ingredient list lives
only in a screenshot of the comments. The general need is that any skill or
agent can be handed an image along with the text.

Inlining image bytes in the turn would put them in Temporal history. Temporal
caps one payload at 2 MB by default and a workflow's history at 50 MB. A
skill turn also hands the request to several LLM activities, and each
activity's input is recorded, so one inlined photo would be written to
history many times per turn.

## Decision

1. **Parse multi-part content at both edges.** The facade's `buildAgentRequest`
   and the engine gateway's `splitMessages` read the text parts as the message
   and the `image_url` parts as images. A message with images and no text is
   valid. Only the latest user message's images are forwarded. Earlier ones
   are already referenced by the conversation (see 4).
2. **Upload once, at the engine gateway.** `/invoke` accepts
   `images: [{url}]` (and `/v1/chat/completions` reads them off the
   message). The gateway validates each image and stores it in S3 under
   `<prefix>/<session>/<uuid>.<ext>`. Validation covers: `data:` base64 of
   png/jpeg/gif/webp whose sniffed type matches the declared one, at most
   5 MB each and four per message. Only a `Ref` (`key`, `mediaType`) enters
   the workflow. An `https://` image URL passes through as an external `Ref`
   and is never fetched by the engine (no SSRF); the model provider fetches
   it. Images on an event envelope are refused. Chat clients resend the
   whole transcript, earlier images included, on every
   `/v1/chat/completions` request, so that endpoint streams its body and
   keeps only the newest four large `data:image/` strings (the last user
   message's), discarding earlier ones as they pass. Its body may be up to
   256 MB on the wire, of which at most 8 MB of non-image content is held;
   more is a 413. `/invoke` carries one turn's images and keeps the ~31 MB
   cap.
3. **Resolve at the point of use.** The worker resolves `Ref`s only inside
   activities:
   - `PlanAction` (skill planning) and `CompleteTurn` (plain chat) inline them
     as base64 data URLs on the user message. Inlining, rather than handing
     over a presigned URL, means a bucket the provider cannot reach (a private
     MinIO) still works. `llm.Message.Images` is folded into the
     chat-completions wire format by the client and never serialized with the
     message itself, so it cannot leak into a Temporal payload.
   - Agent delegation runs a `PresignAttachments` activity and appends an
     `<attached_images>` block of 24-hour download links to the agent's goal,
     or to a follow-up prompt with that turn's own images.
   - Routing, retrieval and fit checks stay text-only. Instead, the workflow
     appends a deterministic `[The user attached N image(s) to this message.]`
     marker to the message.
4. **Keep references in the transcript.** `ChatMessage.Images` holds a user
   message's `Ref`s, so a follow-up ("convert it to metric") still shows the
   model the image. A turn uses up to three images, newest messages first,
   while their messages remain in the bounded history. The current turn's
   images are resolved strictly. Earlier ones are best-effort: one that can
   no longer be loaded (expired by the lifecycle rule, say) is skipped, and an
   earlier external `https://` image is not sent again, because a provider
   rejects the whole request over one URL it cannot fetch. One bad earlier
   image therefore cannot break every later turn.
5. **Fail loudly.** A turn carrying a `data:` image on a deployment without a
   store is refused (400, "image attachments are not enabled"). It is not
   answered as if the image had been seen. The facade now relays the engine's
   400 reason to the user. A store outage on upload is a 502.
6. **Configuration reuses the image-gen profile.** The temporal-engine chart's
   `attachments.s3.secretName` mounts one Secret, via `envFrom`, on the gateway
   and the worker. The Secret uses the image-gen tool's key shape
   (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `IMAGE_S3_BUCKET`,
   `IMAGE_S3_REGION`, `IMAGE_S3_ENDPOINT`, `IMAGE_S3_FORCE_PATH_STYLE`, and
   optionally `ATTACHMENTS_S3_PREFIX`), so a deployment can reuse that bucket
   and its PutObject/GetObject credentials. With no static keys, the AWS
   default credential chain (IRSA) applies.

## Consequences

- Image bytes never enter Temporal history, whatever the image size or the
  number of LLM activities.
- The engine never deletes objects. Each deployment needs a bucket lifecycle
  rule expiring the prefix.
- Presigned links are written into an AgentRun's goal and are readable by
  anyone who can read that CR until they expire.
- A conversation workflow that idles out and restarts loses earlier images,
  because the facade forwards only the latest message's images. The text
  transcript still carries the marker line.
- The configured `OPENAI_MODEL` must accept image input. The default
  (`gpt-4o`) does.
- Memory limits rose to cover the worst case: the gateway to 256 Mi (it
  holds an `/invoke` body of up to about 31 MB, or a chat body's 8 MB of text
  plus four images) and the worker to 384 Mi (it
  holds up to three inlined images).
- Tools (ToolRun Jobs) still take one string input and do not receive images.
  A skill reads the image itself in its planner and passes on what it needs
  as text.

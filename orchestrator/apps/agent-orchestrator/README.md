# agent-orchestrator

The facade in front of the Temporal agent engine
([orchestrator/engines/temporal](../../engines/temporal/)). It speaks the two
APIs callers use — the async `/invoke` contract (ADR 0006) and an
OpenAI-compatible chat API (ADR 0007) — and forwards every turn to the engine's
gateway, which runs the agent loop as a durable workflow (ADR 0036).

Until [ADR 0053](../../../docs/adr/0053-remove-langgraph-engine.md) this app
also contained an in-process LangGraph agent loop, selected with
`AGENT_ENGINE`. That engine, and everything only it used (skill/tool/agent
retrieval, the planners, the ToolRun/AgentRun launchers, the Job callback
receiver, LocalTool sidecars, the NATS agent channel), is gone; the Temporal
engine is the only engine.

Design rationale lives in [docs/orchestrator.md](../../../docs/orchestrator.md)
and [docs/adr/](../../../docs/adr/).

## What it does

1. Accepts a turn over HTTP: `POST /invoke` (`202 { id }`, then poll
   `GET /invoke/:id`) or `POST /v1/chat/completions` (blocking or SSE).
2. Matches an `/invoke` `event` envelope against the `EventTrigger` table
   (ADR 0048) and forwards the **resolved** turn — target, params, read-only,
   identity mode — as a signed dispatch assertion. An event that matches no
   trigger is a clean `200 ignored`.
3. Verifies who is asking, where it can, and signs that across the hop:
   - a chat user's Open WebUI per-request JWT (`X-OpenWebUI-User-Jwt`) becomes
     a signed caller-identity assertion, so each human resolves to their own
     subject rather than the gateway's shared one;
   - a webhook sender's login arrives as integration-gateway's signed sender
     assertion (ADR 0030 §6) and is re-signed for the engine.
4. Resolves consumer-supplied tools (`tools`/`tool_choice`, ADR 0035), ranking
   them in their own Qdrant collection when there are more than
   `AGENT_CALLER_TOOL_TOP_K`, and hands `tool_calls` back to the caller.
5. Applies the conversation's sticky **read-only** flag (docs/event-triggers.md
   §1) before the turn reaches the engine.
6. Answers Open WebUI's internal housekeeping completions (titles, tags) with a
   direct model call, so they never reach the engine.

Everything else about a conversation — skill continuity, continuation tokens,
pending identity links, approvals — lives in the engine's workflow.

## Layout

```
src/
├── index.ts                      # service entry: wires the stores, the engine adapter and InvokeServer
├── server.ts                     # InvokeServer: /invoke (ADR 0006), /v1/models + /v1/chat/completions (ADR 0007)
├── config.ts                     # env-driven configuration
├── engine/
│   ├── types.ts                  # TurnInput / TurnOutcome / TurnEngine — the facade↔engine contract
│   └── temporal-engine.ts        # HTTP client for the engine gateway: accept, poll, auto-resume a link
├── routing/
│   └── crd-event-trigger-registry.ts  # EventTrigger CRs, watched live (ADR 0048)
├── caller-tools/                 # consumer-supplied tools (ADR 0035): parse, rank, Qdrant cache
├── openai/                       # OpenAI wire format, SSE helpers, heartbeat, housekeeping completer
├── rbac/                         # assertions signed/verified across hops; identity resolvers
├── identity-link/                # Connections-page principal recording (ADR 0046)
├── session/                      # sticky read-only flag per conversation (Redis or in-memory)
├── invocation/                   # /invoke accept-then-poll records (Redis or in-memory)
├── vector-store/                 # embedder + Qdrant point ids for the caller-tool cache
├── k8s/crd-watcher.ts            # list-then-watch helper for CRDs
├── telemetry.ts, traceparent.ts  # spans per /invoke and per engine call (ADR 0050)
└── retry.ts                      # startup retry for Redis/Qdrant
```

## Calling it

```bash
# Kick off a request
curl -s -X POST http://localhost:8081/invoke \
  -H 'authorization: Bearer dev-token' \
  -H 'content-type: application/json' \
  -d '{"request": "scrape https://example.com/some-recipe"}'
# => 202 {"id":"<uuid>","status":"pending"}

# Poll for the result
curl -s http://localhost:8081/invoke/<uuid>
# => 200 {"id":"<uuid>","status":"succeeded","result":"..."}
```

### OpenAI-compatible chat interface

```bash
curl -s http://localhost:8081/v1/models

# Streaming (SSE) — recommended; relays the engine's narration live
curl -N -s -X POST http://localhost:8081/v1/chat/completions \
  -H 'authorization: Bearer dev-token' -H 'content-type: application/json' \
  -d '{"model": "agent-orchestrator", "stream": true, "messages": [{"role": "user", "content": "scrape https://example.com/some-recipe"}]}'
```

Open WebUI forwards a conversation id (`X-OpenWebUI-Chat-Id`) when its
deployment sets `ENABLE_FORWARD_USER_INFO_HEADERS=true`; it becomes the engine
workflow's session id. `/invoke` callers pass `"session_id"` instead. A bounded
window of the prior conversation is folded into each request as a
`<conversation_history>` block.

## Configuration

See [.env.example](.env.example). Required: `OPENAI_API_KEY` (caller-tool
embeddings and housekeeping completions) and `AGENT_TEMPORAL_ENGINE_URL`.
Startup refuses `AGENT_ENGINE` set to anything other than `temporal`, so a
deployment still asking for the removed engine fails loudly.

## Caller authentication

Every `POST /invoke` and `POST /v1/chat/completions` is authenticated before
anything about the turn runs (`src/rbac/caller-authenticator.ts`), with the
rule the removed in-process engine applied:

- A chat turn carrying Open WebUI's signed `X-OpenWebUI-User-Jwt`, with
  `AGENT_OPENWEBUI_USER_JWT_SECRET` set, is authenticated by that JWT. A JWT
  that does not verify is refused even when the bearer is valid.
- Every other turn needs a bearer token the identity map resolves:
  `AGENT_IDENTITY_RESOLVER=static` (`AGENT_STATIC_IDENTITIES`) or `oidc`
  (`AGENT_OIDC_*`, with the static map as a fallback for the tokens in it).

Anything else gets a `401` before trigger matching, caller tools or the engine
run. `AGENT_CALLER_AUTH=warn` logs `would reject` instead, as a rollout aid
while confirming every caller's token is in the map. An unrecognised value
stops the pod.

This decides whether a turn runs, not who it runs as. That is still the
forwarded user JWT or the signed sender assertion, else the engine token's
subject: a bearer token is one value every caller of a client shares, so it
says nothing about which human is asking.

`GET /invoke/:id`, `GET /v1/models` and `GET /sessions/live` start nothing and
are not gated.

## Known gaps

- **`/invoke` can only offer caller tools, not resume from their results** — it
  takes one `request` string with nowhere to put a `role: "tool"` message. The
  full round trip is chat-facade-only (ADR 0035).
- **`GET /sessions/live` always answers `{ live: false }`.** The live-session
  tunnel (ADR 0026) proxied into agent Pods the in-process engine launched; the
  route is kept so integration-gateway's label reconciler still gets a definite
  answer.

## Commands

- Build: `npm run build` (run from repo root first: `npm run build:libs`)
- Typecheck: `npm run typecheck` | Test: `npm test` (vitest)
- Docker build (from repo root): `docker build -f orchestrator/apps/agent-orchestrator/Dockerfile -t agent-orchestrator:latest .`
- Hardened run: `OPENAI_API_KEY=... AGENT_TEMPORAL_ENGINE_URL=... ./run.sh`

## Exit codes (index.ts)

A non-zero exit only signals a **startup** failure (e.g. missing
`AGENT_TEMPORAL_ENGINE_URL`, or `AGENT_ENGINE=langgraph`), not a per-request
outcome. Per-request failures are reported via `GET /invoke/:id`
(`status: "failed"`, `error` message).

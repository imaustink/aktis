# 0053. Remove the in-process LangGraph engine; the Temporal engine is the only agent loop

Date: 2026-11-21

## Status

Proposed. Supersedes [0002](0002-langgraph-agent-loop.md) (the LangGraph loop).
Completes [0036](0036-temporal-execution-engine.md) (the `AGENT_ENGINE` switch)
and is the deprecation ADR that
[0049](0049-configurable-agent-identity-mode.md) said would follow its freeze.

## Context

[0036](0036-temporal-execution-engine.md) added a second agent loop: Temporal
workflows in a Go worker (`orchestrator/engines/temporal`). `agent-orchestrator`
picked one per process with `AGENT_ENGINE=langgraph|temporal`, and kept the
in-process LangGraph graph so the switch could be rolled back.

The rollout finished. [0049](0049-configurable-agent-identity-mode.md) froze
the LangGraph engine. Identity modes, observability
([0050](0050-agent-observability-with-langfuse.md)) and the repository read
gate ([0042](0042-repository-read-gate.md)) were built on Temporal only, and
LangGraph fails closed on what it doesn't implement. 0049 left deprecation to
its own ADR, covering the e2e suite and any deployment still on `langgraph`.

There are no such deployments. The chart default, Bitovi production's values,
and homelab (since 2026-11-18) all run `temporal`. What remains costs us:

- **Two implementations of every behaviour.** The graph and its helpers are
  thousands of lines kept in step with Go by `PARITY` comments. A parity audit
  (`orchestrator/engines/temporal/docs/langgraph-parity-audit.md`) found 18
  gaps.
- **A frozen engine that still has to be secured.** 0049 kept one security fix
  on LangGraph despite the freeze. Every new identity-bearing feature needs a
  fail-closed refusal written for an engine nobody runs.
- **Two ways the e2e suite runs.** `E2E_ENGINE` selected an engine, and specs
  gated Temporal-only behaviour on `isTemporal()`. Nothing ran the LangGraph
  half.
- **A rollback target that no longer works.** The chart's "TO ROLL BACK"
  guidance points at a loop without identity modes or the read gate. Following
  it today would be a security regression, not a rollback.

## Decision

Remove the LangGraph engine and the `AGENT_ENGINE` switch in one step. This ADR
is the deprecation. There is no transitional release that warns first, because
no deployment would see the warning.

1. **`agent-orchestrator` is the facade in front of the Temporal gateway.** It
   keeps the OpenAI-compatible API ([0007](0007-openai-compatible-chat-facade.md)),
   `/invoke` ([0006](0006-async-http-invoke-interface.md)), event-trigger
   routing ([0048](0048-generic-event-triggers.md)), identity resolution and
   forwarding, and sender assertion. It forwards every turn to the gateway.
2. **Everything only the graph used is deleted**, as determined by reachability
   from the remaining entry points. That includes:
   - the graph nodes, and the planners, selectors and fit checkers they called;
   - TS-side Qdrant catalog indexing (the Go `catalog-sync` owns the catalog);
   - the ToolRun/AgentRun launchers, callback receivers and local tool executor;
   - the NATS agent channel;
   - TS session-side approval and sub-agent HITL state, and the session
     record's skill/agent-run anchors and continuation tokens — the record now
     holds only the sticky read-only flag;
   - the knowledge-base, MCP and LocalTool clients, and the Tool/Skill/Agent/
     IdentityProvider registries (the engine watches its own catalog);
   - the live-session tunnel routes (`/agent-runs/:id/events|opencode`).
     `GET /sessions/live` stays and always answers `{ live: false }`, because
     integration-gateway's label reconciler only clears a label on a definite
     "not live";
   - the agent-orchestrator chart's callback Service, LocalTool sidecars, and
     every RBAC grant except reading `EventTrigger`s;
   - `@langchain/*`, `nats` and `zod` from the app's dependencies.
3. **Misconfiguration fails fast.**
   - `AGENT_TEMPORAL_ENGINE_URL` is required.
   - The app refuses to start if `AGENT_ENGINE` is set to anything but `""` or
     `"temporal"`. A stale `langgraph` rollback setting stops the pod instead
     of being ignored.
   - The chart stops emitting `AGENT_ENGINE`. Its template `required`s
     `config.temporalEngineUrl`, and `fail`s the render when `config.agentEngine`
     is anything but `""` or `"temporal"`. `agentEngine: temporal` is still
     accepted, so existing values files render unchanged.
4. **The e2e suite has one engine.** `E2E_ENGINE`, `support/engine.ts` and every
   `isTemporal()` gate are removed. Specs that were Temporal-only now always
   run, and LangGraph-only assertions are deleted.

## Consequences

- **No rollback target.** Recovering from a Temporal-engine problem means
  rolling back the release, not flipping a flag. The temporal-engine subchart is
  required, not optional.
- **The Redis session store is smaller.** It keeps only what the facade still
  needs. Skill continuity, continuation tokens, pending links and approvals
  already lived in workflow state for every conversation, per 0036.
- **Parity scaffolding goes.** The Go engine's `PARITY:` comments that pointed
  at TS files are rewritten or removed.
  `orchestrator/engines/temporal/docs/langgraph-parity-audit.md` is kept as a
  historical record, marked superseded.
- **ADR status changes.** [0002](0002-langgraph-agent-loop.md) is superseded by
  this ADR. [0036](0036-temporal-execution-engine.md) and
  [0049](0049-configurable-agent-identity-mode.md) carry a note pointing here.
  The other historical ADRs that describe "both engines" stay as written.

## Follow-ups

Found while removing the engine. Each is a gap on every Temporal deployment
today, not one this change opens.

1. **The facade does not authenticate the caller's bearer token.** Only the
   LangGraph graph resolved it (`resolveIdentity`, failing closed). The
   Temporal adapter never forwards it: the engine gateway authenticates
   agent-orchestrator's own `AGENT_TEMPORAL_ENGINE_TOKEN`. So a turn that
   carries no Open WebUI user JWT and no sender assertion runs as that token's
   subject, whoever sent it. That includes an `/invoke` `event` envelope, which
   the orchestrator then signs as a dispatch. The invoke Service is
   cluster-internal and NetworkPolicy-scoped. The static/OIDC resolvers and
   their `AGENT_IDENTITY_RESOLVER` / `AGENT_STATIC_IDENTITIES` / `AGENT_OIDC_*`
   settings are kept, parsed and chart-wired, so the fix is to resolve the
   bearer in `InvokeServer` and refuse unresolved callers. That is a behaviour
   change for any caller whose token is not in the map, so it needs each
   deployment's caller tokens checked first.
2. **integration-gateway still carries LangGraph-only paths.** These never fire
   against the Temporal engine:
   - the `/agent-runs/*` live-session proxy and live badge;
   - the `identityLinkPending` / `identityLink` wait-and-resume relay on
     `GET /invoke/:id`, a field this app no longer sets.

   They are harmless, but dead.
3. **LocalTools have no executors under Temporal.** The worker can dispatch
   them (`LOCALTOOL_SOCKET_DIR`), but the temporal-engine chart deploys no
   executor sidecars. The orchestrator's sidecars, which only the removed
   engine used, are gone with it.
4. **`agent-orchestrator.config.agentEngine` is still accepted** (as `""` or
   `"temporal"`) so existing values files render. Drop it once consumers have
   removed it from their values.

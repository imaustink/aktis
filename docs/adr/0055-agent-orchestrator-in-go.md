# 0055. agent-orchestrator and mcp-broker move to Go; one CEL evaluator

Date: 2026-11-23

## Status

Proposed. Follows [0053](0053-remove-langgraph-engine.md), which left
agent-orchestrator as a TypeScript facade in front of the Go Temporal engine.

## Context

After 0053, agent-orchestrator no longer ran an agent loop. Every turn it
accepted was forwarded over HTTP to the engine gateway
(`orchestrator/engines/temporal`), which is Go. What the facade still did —
request parsing, EventTrigger matching, caller-supplied tools, the sticky
read-only flag, caller authentication, and signing identity and dispatch
assertions across the hop — was in most cases already implemented a second
time in the engine module:

- EventTrigger matching, params and templates (`internal/catalog`), kept in
  step with the TS `@aktis/event-triggers` package by a shared CEL parity
  fixture;
- caller-tool parsing, ranking and the Qdrant store (`internal/callertools`),
  ported from the TS app with "matches TS" tests;
- the sender, caller-identity and dispatch assertions (`internal/rbac`), with
  byte-compatibility tests against TS-minted vectors.

Two implementations of one contract is the cost 0053 removed for the agent
loop. The same cost remained here, most sharply for CEL: event predicates were
evaluated by `@marcbachmann/cel-js` in agent-orchestrator and
integration-gateway and by `cel-go` in the engine and core-controller. The
parity fixture caught drift; it could not prevent it, and the documented CEL
subset (§3 of docs/event-triggers.md) existed mainly to keep the two runtimes
agreeing.

mcp-broker (ADR 0045) is a small service that only the Go engines call.

## Decision

1. **agent-orchestrator is a Go binary in the engine module**
   (`cmd/agent-orchestrator`, package `internal/facade`). It is a drop-in
   replacement: same Service, port, routes, request and response bodies,
   status codes, environment variables, Redis key spaces (`sess:`, `inv:`)
   and assertion headers. The chart changes only its image. It reuses the
   engine's `catalog`, `callertools` and `rbac` packages instead of carrying
   its own copies. It still talks to the engine gateway over HTTP, unchanged:
   folding the two processes together is a separate decision.
2. **There is one CEL evaluator, `cel-go`.** integration-gateway's pre-sender
   checks (could any trigger match this delivery; what do the source's
   `sender.*From` expressions evaluate to) move behind a new
   agent-orchestrator endpoint, `POST /events/prefilter`
   (docs/event-triggers.md §7). The TS package keeps the envelope builders and
   the CR types; its CEL, matching and template code, and the `cel-js`
   dependency, are deleted. The parity fixture moves to the engine's
   `internal/catalog/testdata`.
3. **mcp-broker is a Go module** (`orchestrator/apps/mcp-broker`), same
   contract, on the official Go MCP SDK.
4. **Everything else stays TypeScript**: integration-gateway (OAuth and
   identity-link flows, `node-pty`), connection-broker (document parsing),
   the SWE agents and `@aktis/agent-runtime` (they wrap Node CLIs, and the
   runtime is the SDK agent authors use), and the catalog tools, which sit
   behind the tool protocol where language does not matter.

## Consequences

- **A webhook with a `sender` block costs one extra in-cluster round trip**
  before sender resolution. A prefilter that cannot be reached (after the
  same short retry the `/invoke` relay uses) drops the delivery: what it
  computes selects whose credentials a run gets, so "couldn't evaluate" never
  means "let it through". A delivery the relay could not have handed to the
  orchestrator anyway is lost either way.
- **A sender expression that does not compile no longer makes its source's
  routes 404.** The gateway has no compiler, so the source is served and each
  delivery's evaluation fails, which drops it (`202` + log line). Both are
  fail-closed; core-controller's `Ready=False / InvalidCEL` is how an operator
  finds out, as it already was for triggers.
- **The caller-tool store speaks Qdrant's gRPC API** (the engine's client)
  rather than REST. The host comes from `AGENT_QDRANT_URL`; the port is
  `AGENT_QDRANT_GRPC_PORT` (default 6334, the Qdrant default). It reuses the
  existing collection, but derives point ids differently and stores
  `lastSeenAt` in seconds where the TS store used milliseconds. So each
  definition is embedded once more on first sight after the rollout, and
  `Prune` also ages out millisecond timestamps, so the TS-written points
  are swept on the normal TTL instead of living forever.
- The TS orchestrator's unit tests are ported to Go beside the code; the e2e
  suite, which is black-box over HTTP and Kubernetes, stays TypeScript and is
  the compatibility check for the swap.
- `docs/event-triggers.md` §3's CEL subset is still the contract for authors,
  but nothing now depends on two runtimes agreeing on it.

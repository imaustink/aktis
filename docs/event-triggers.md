# Event triggers (`EventSource` + `EventTrigger`)

The implementation contract for [ADR 0048](adr/0048-generic-event-triggers.md)
and its run-completion extension, [ADR 0052](adr/0052-run-events-and-software-factory.md).
Every component that produces, matches or acts on an event follows this
document; where code and this document disagree, one of them is a bug.

```
                ┌──────────────────────── integration-gateway ────────────────────────┐
 HTTP webhook ─►│ POST /webhooks/:source  verify ─► parse ─► sender ─► envelope ──┐    │
 CronJob tick ─►│ POST /events/:source/tick  bearer ─► envelope (body = payload) ─┤    │
 run finished ─►│ onComplete.emitTo ─► run envelope (§9), sender re-verified ─────┤    │
                │                                                                  ▼    │
                │                       POST /invoke { event: <envelope> } ──► orchestrator
                │                                                                  │    │
                │  ◄── 200 ignored | 202 { id, trigger: { sessionId, reply } } ◄───┘    │
                │  reply adapters: ack ─► poll ─► result ─► onComplete                 │
                └──────────────────────────────────────────────────────────────────────┘
 orchestrator ─► Temporal engine: match EventTriggers (CEL) ─► params ─► target
```

All CEL runs in Go, in one evaluator (`orchestrator/engines/temporal/internal/catalog`):
agent-orchestrator matches triggers with it, core-controller compiles against
the same environment for `Ready`, and integration-gateway — which has no CEL
runtime of its own — asks agent-orchestrator over `POST /events/prefilter` (§7).

## 1. CRDs (`core.aktis.ai/v1alpha1`, namespaced)

Both kinds live in the release namespace (the one agent-orchestrator,
integration-gateway and the Temporal gateway watch).

### `EventSource`

How events arrive and how far they are trusted. Exactly one of `webhook` / `cron` / `run` / `conflicts`.

```yaml
apiVersion: core.aktis.ai/v1alpha1
kind: EventSource
metadata: { name: github }          # the name IS the route: POST /webhooks/github
spec:
  webhook:
    verify:
      scheme: hmacSha256            # hmacSha256 | slackV0 | bearer   (no "none": fail closed)
      header: x-hub-signature-256   # hmacSha256: signature header (required); bearer: default "authorization"
      prefix: "sha256="             # hmacSha256 only; stripped before hex compare
      secretRef: { name: github-webhook, key: secret }
    deliveryIdHeader: x-github-delivery   # optional; else a random uuid
  sender:                           # optional — who caused the event
    provider: github                # only "github" today: verified against GitHub team/collaborator roles
    loginFrom: body.sender.login    # CEL -> string
    isBotFrom: body.sender.type == "Bot"   # CEL -> bool (optional; default false)
    repositoryFrom: body.repository.full_name  # CEL -> "owner/name" (optional)
    accountIdFrom: body.sender.id   # CEL -> immutable account id (optional; ADR 0049 §5)
    dropBots: true                  # default true: bot-authored events are dropped (self-trigger guard)
---
kind: EventSource
metadata: { name: nightly-sweep }
spec:
  cron:
    schedule: "0 7 * * *"           # standard 5-field cron
    timeZone: America/Chicago       # optional
    suspend: false                  # optional
    payload: { repo: imaustink/aktis }   # optional, any JSON; becomes envelope.body
---
kind: EventSource
metadata: { name: runs }
spec:
  run: {}                           # run-completion events (§9); no route, no Secret, no sender
---
kind: EventSource
metadata: { name: pr-conflicts }
spec:
  conflicts:                        # conflict events from a sweep of open PRs (§10)
    author: "app/k5s-bot"           # GitHub search author: qualifier (login, login[bot] or app/<slug>)
    intervalSeconds: 300            # min 60, default 300
    skipLabels: [ai-triage, ai-review, ai-waiting]
```

Rules:

- `sender` is only valid with `webhook` (admission rejects it on `cron` and
  `run`). A sender whose provider cannot verify the
  login (resolver returns nothing) drops the event. **An unverified login is never
  forwarded** — a source without `sender` runs with no per-user principal, as the
  gateway's own service identity.
- `slackV0` verifies `x-slack-signature` = `v0=hex(hmac(secret, "v0:" + ts + ":" + rawBody))`
  with `x-slack-request-timestamp` within 300s, and answers Slack's
  `{"type":"url_verification"}` handshake with its `challenge` synchronously
  (nothing is relayed).
- `bearer` compares `header` (default `authorization`, value `Bearer <secret>`;
  a non-`authorization` header carries the raw secret) in constant time.
- Verification happens on the raw bytes **before** parsing; failure is `401`.
- A `run` source receives nothing from outside: integration-gateway synthesizes
  its events (§9). It is always `Ready` (`RunEvents`) and owns no CronJob.
- A `conflicts` source likewise has no route: the gateway sweeps it (§10). It
  is always `Ready` (`ConflictSweep`); `sender` is rejected on it too.
- Status: `conditions[type=Ready]`; cron sources also `status.cronJobName`.

### `EventTrigger`

What a matching event dispatches.

```yaml
apiVersion: core.aktis.ai/v1alpha1
kind: EventTrigger
metadata: { name: github-pr-review }
spec:
  sourceRef: github                 # optional; restricts to one EventSource
  priority: 0                       # optional; higher wins when several match
  match:
    cel: >-                         # optional; omitted = every event from sourceRef
      headers["x-github-event"] == "pull_request" &&
      body.action == "labeled" && body.label.name == "ai-review"
  params:                           # name -> CEL, evaluated against the envelope
    repo: body.repository.full_name
    number: body.pull_request.number
    label: body.label.name
  sessionKey: "github:{{repo}}#{{number}}"   # optional template (see §4)
  target:                           # exactly one of orchestrator/skillRef/agentRef/toolRef/none
    agentRef: claude-code-swe-agent
    readOnly: true                  # deterministic write-gate for every run in the turn
    outcome: verdict                # optional: verdict | coordination (read-only agentRef only; §9, §10)
    promptTemplate: |               # required unless toolRef; {{param}} substitution
      Review pull request {{repo}}#{{number}} ...
  reply:                            # optional, executed by integration-gateway
    github:
      repository: "{{repo}}"
      issueNumber: "{{number}}"     # issue or PR number
      ack: "Starting a review of this pull request…"   # template; posted before the run
      sessionPage: true             # append a session-page link to ack (needs GATEWAY_PUBLIC_URL)
      result: true                  # post the turn's result as a comment
      removeLabel: "{{label}}"      # removed when the run ends, success or failure
      addLabels: ["ai-reviewing"]   # templates; added at acceptance (max 10; "" entries dropped)
      assignees: ["{{assignee}}"]   # templates; assigned at acceptance (max 10; "" entries dropped)
    http:
      url: http://ci-bridge.tools.svc/results   # LITERAL only — never templated (SSRF)
      secretRef: { name: ci-bridge, key: secret }  # optional: x-event-signature: sha256=<hmac(body)>
  onComplete:                       # optional (§9)
    emitTo: runs                    # a run-kind EventSource in the same namespace
```

Rules:

- Target `orchestrator: {}` runs an ordinary conversational turn (RAG/skill
  selection) on the rendered prompt. `skillRef` / `agentRef` force that Skill /
  Agent (still re-resolved under the caller's roles; a miss is a turn failure, not
  a fallback). `toolRef` runs that Tool once with `params` as its input, skipping the
  planner; `promptTemplate` is optional for it. Tools take one string input, so the **tool input** is: the string form of `params.input` when
  `params` has exactly one key named `input`; otherwise the params object as
  compact JSON with sorted keys (`{}` when there are no params).
- `readOnly: true` sets `readOnly` on every `AgentRun` and `ToolRun` the turn
  launches (including ones a forced skill delegates to) and suppresses write
  credentials. It is a turn property, not a prompt — it can only restrict.
- Read-only is also the **session's mode**. A trigger turn sets the mode to its
  trigger's `readOnly`, `false` included. A turn no trigger produced (a
  session-page follow-up, a chat prompt on the same session id) inherits it
  and can only restrict it further. So a follow-up in a review's session stays
  read-only, and a writable trigger sharing that session key (triage on the
  same issue) makes it writable again. A read-only turn is never forwarded
  into an agent run that was launched writable; the turn fails instead.
- `identity: agent | delegated | hybrid` overrides the identity mode of every
  agent the turn authorizes, in place of the Agent's `defaultIdentity`
  ([ADR 0049](adr/0049-configurable-agent-identity-mode.md)). Unlike
  `readOnly` it can **widen** what a run reaches, so it must be in the Agent's
  `allowedIdentities` (the trigger is not `Ready` otherwise), it is invalid with
  `toolRef`, and it is honoured only by the Temporal engine. A turn that parks
  on an account link resumes under the same override. It is not a session mode.
- `outcome: verdict | coordination` names the structured outcome the forced
  agent's run reports (§9, §10). Valid only with `agentRef` and
  `readOnly: true`; unset means `verdict`. It reaches the run as
  `AgentRun.spec.outcome` inside the signed dispatch, never through the
  prompt, so no text in an event body can switch it. It applies to the
  trigger's own turn only: the forced agent's launch (and an account-link
  resume of it), not sub-agents and not later turns in the session.
- `none: {}` runs nothing: no AgentRun, skill, tool, LLM call or identity /
  credential check (so never an identity `403`). The event is matched and
  accepted (`202` + invocation id) and polling returns
  `{status:"succeeded", result:""}` at once; only the `reply` side effects run.
  Admission rejects `none` together with `promptTemplate`, `readOnly`,
  `outcome` or `identity` (nothing runs to receive them). It is how a handoff such as
  "assign the PR to a human" is expressed.
- `reply.github.addLabels` / `assignees` are rendered like `removeLabel`
  (`{{param}}`), and an entry that renders to `""` is dropped; when nothing is
  left the field is omitted from the rendered reply. Labels the bot adds this
  way never re-enter routing (its own webhooks are dropped as bot-authored), so
  they are safe as status markers; a human applying the same label still fires
  label triggers.
- `onComplete.emitTo` must name an existing `run` EventSource in the same
  namespace, else the trigger is `Ready=False` (`EmitToNotFound` /
  `EmitToNotRunSource`). core-controller re-checks it when that source appears
  or changes.
- `onComplete.waitUntilClosed: {repositoryParam, numberParam}` and
  `onComplete.serialize: {group, keyParam}` hold the completion event in the
  gateway instead of publishing it at once (§10). Each `*Param` must name a key
  of `spec.params` and `group` must be a DNS label, else the trigger is
  `Ready=False` (`InvalidOnComplete`).
- A trigger whose CEL fails to compile is `Ready=False` and never matches.

## 2. The envelope

Every source produces exactly this JSON object. It is both the `event` field of
`/invoke` and the CEL activation.

| Field | Type | Webhook | Cron | Run (§9) |
| --- | --- | --- | --- | --- |
| `source` | string | EventSource name | EventSource name | the `emitTo` source name |
| `kind` | string | `"webhook"` | `"cron"` | `"run"` |
| `id` | string | `deliveryIdHeader` value, else uuid | `<source>-<unix seconds>-<8 random hex>` | uuid |
| `time` | string | RFC 3339 receipt time (UTC) | RFC 3339 tick time (UTC) | RFC 3339 completion time (UTC) |
| `headers` | map<string,string> | request headers, **lower-cased**, with the verify header, `authorization` and `cookie` removed; repeated headers joined with `", "` | `{}` | `{}` |
| `body` | any JSON | `application/json` → parsed; `application/x-www-form-urlencoded` → object of string fields; anything else → `{"text": "<raw>"}` | `spec.cron.payload`, else `{}` | the run envelope (§9) |
| `sender` | map | `{login, isBot}` when `spec.sender` resolved, else `{}` | `{}` | `{login, isBot: false}`: the chain's originating human |
| `repository` | string | `repositoryFrom` result, else `""` | `""` | the chain's origin repository |

## 3. CEL

One environment and one runtime, `cel-go`: core-controller compiles against
it for `Ready`; agent-orchestrator and the Temporal gateway match with it
(`orchestrator/engines/temporal/internal/catalog`); integration-gateway's
pre-sender checks are evaluated by agent-orchestrator on its behalf (§7).
Until [ADR 0056](adr/0056-agent-orchestrator-in-go.md) a second, TypeScript
runtime (`@marcbachmann/cel-js`) evaluated the same expressions, which is why
the rules below are written to mean the same in both:

- Variables: `source`, `kind`, `id`, `time`, `repository` (string), `headers`
  (map<string,string>), `body` (dyn), `sender` (map<string,dyn>).
- JSON numbers are normalized before evaluation: integral → `int`, otherwise
  `double`, in both runtimes. Integrality is decided on the value (JSON `1.0`
  and `1e3` are ints; integral values outside int64 stay `double`).
- Heterogeneous list/map literals (`[1, "a"]`) are allowed, as in cel-go.
- `matches` patterns must mean the same in RE2 and ECMAScript regex (no inline
  flags like `(?i)`, no lookaround/backreferences).
- Supported, parity-tested subset: literals; `== != < <= > >=`; `&& || !`;
  `?:`; field and index access; `has()`; `in` (list/map); `size()`; string
  `startsWith` / `endsWith` / `contains` / `matches`; `int()` / `string()`;
  macros `exists` / `all` / `map` / `filter`. Anything else is unsupported even if
  one runtime happens to accept it.
- `match.cel` must yield `bool` (a statically non-bool match is a compile error
  → `Ready=False`; a runtime non-bool is a non-match); an evaluation error (e.g.
  missing field) is a **non-match**, logged with the trigger name. A `params`
  expression that fails to compile also makes the trigger `Ready=False`. A param
  value must be JSON-representable (null/bool/int/double/string/list/map, finite
  doubles) — anything else is a `params` error. A `params` evaluation error after a
  match fails the event (`422`, §5) — never a silent empty value.
- The fixture file `orchestrator/engines/temporal/internal/catalog/testdata/cel-parity.json`
  is the executable definition of this section: every case runs in the Go
  tests and must produce the listed verdict / value.

## 4. Matching, params, templates

1. Candidates: `Ready` triggers whose `sourceRef` is empty or equals
   `envelope.source`, and whose `match.cel` is absent or evaluates to `true`.
   `Ready` means `Ready=True` with `observedGeneration` equal to the trigger's
   current `metadata.generation`: after an edit, the previous spec's status
   does not count, and the trigger does not match until core-controller has
   checked the new spec.
2. Winner: highest `priority`, then lexicographically smallest name. (Single
   winner; one event is one turn.)
3. `params` are evaluated to typed values.
4. Templates (`promptTemplate`, `sessionKey`, every `reply.github` string) are
   rendered with `{{ name }}` → the param's string form: string as-is, int in
   decimal, double in shortest round-trip form exactly as ECMAScript
   `Number#toString` (`5`, `0.1`, `1e+21`, `1e-7`), bool `true`/`false`, null `""`,
   list/map as compact JSON with map keys sorted at every level and no HTML
   escaping. A placeholder is `\{\{[ \t\n\f\r]*([^{} \t\n\f\r]+)[ \t\n\f\r]*\}\}`;
   rendering is a single pass (substituted text is never re-expanded) and an
   unknown placeholder is left verbatim. `reply.github.issueNumber` must render
   to a positive integer, else the event fails (`422`).
5. Session id: rendered `sessionKey` (when non-empty), else the caller's
   `session_id`, else `event:<source>:<id>`.

## 5. `/invoke` with an event

Same endpoint and accept/poll contract as ADR 0006, on agent-orchestrator (both
engine modes) and on the Temporal gateway (for direct callers). Request:
`{ "event": <envelope>, "session_id"?: string, "identity_link_flow"?: ... }` —
`request` is optional when `event` is present.

| Outcome | Response |
| --- | --- |
| No trigger matched (including no EventTriggers at all) | `200 {"status":"ignored"}` — no turn, no RAG fallback |
| Matched, params/template error | `422 {"status":"failed","error":"..."}` |
| Matched | `202 {"id","status":"pending","trigger":{"name","sessionId","reply","params","onComplete"?}}` |

`trigger.reply` is the trigger's `reply` with every template already rendered
(`issueNumber` as a number) or `null`. `trigger.params` is the §4 string form of
every rendered param (`{name: string}`); `trigger.onComplete` (`{emitTo}`) is
present only when the trigger sets it. The gateway needs both to build the run
event (§9).

Polling `GET /invoke/:id` is unchanged except for `outcome`: on a succeeded turn
whose forced `agentRef` run's final reply carried a structured outcome, the poll
response includes it as `outcome` (a JSON object whose compact serialization is
at most 8192 bytes; anything else is dropped and logged). Otherwise it is
omitted. `result` stays the continuation token.

An event is always matched against the registry. Dispatch is never taken from
the body: `forcedSkillId`, `forcedAgentId`, `forcedToolId`, `toolInput`,
`params`, `triggerName` and `identity` in an `/invoke` body are ignored by
agent-orchestrator and refused with `400` by the Temporal gateway. The Temporal
gateway honours a body `readOnly: true`, which can only restrict.

agent-orchestrator → Temporal gateway carries the resolved turn, never the
envelope. Body: `request` (rendered prompt), `sessionId`, `readOnly` (only
`true`), `targetRepository` (`envelope.repository`). The dispatch itself
travels in the signed `x-orchestrator-dispatch-assertion` header, an
HMAC-SHA256 `<payload>.<signature>` under the sender-assertion secret with
payload `{typ:"dispatch", triggerName, forcedSkillId | forcedAgentId |
forcedToolId, toolInput, readOnly, outcome, params, identity, sessionId, requestSha256, exp}`.
It is bound to the turn's session and prompt and expires after 300 s. The
gateway refuses (`400`) an invalid, expired or mis-bound assertion and one sent
with an `event`. Without the secret, agent-orchestrator fails a Temporal-mode
trigger turn instead of sending it unsigned.

## 6. Runs

| CR field | Container env |
| --- | --- |
| `AgentRun.spec.readOnly` | `AGENT_READ_ONLY=true` |
| `AgentRun.spec.params` (map<string,string>) | `AGENT_PARAMS=<json object>` |
| `AgentRun.spec.outcome` (`verdict` \| `coordination`) | `AGENT_OUTCOME=<value>` |
| `ToolRun.spec.readOnly` | `TOOL_READ_ONLY=true` |

`@aktis/agent-runtime` exposes them as `session.readOnly` /
`session.params` / `session.outcome`. The SWE agents enforce read-only (deny `git push`, `gh pr
create|merge|close|reopen|ready|edit`, mint a `contents:read` token) from
`session.readOnly` alone.

Params are free-form, but the SWE agents (claude-code-swe-agent,
opencode-swe-agent) act on one by name:

| Param | Effect |
| --- | --- |
| `closesIssue` (with `repository`) | On a write-capable run that ends with an open PR, the runner checks GitHub's `closingIssuesReferences` for that PR and, if `repository#closesIssue` isn't among them, appends `Closes #<n>` (`Closes owner/repo#<n>` for a PR in another repo) to the description, so merging the PR closes the issue. A mention like "fixes the bug in #12" that GitHub doesn't parse as closing doesn't count. Set it on issue-triage triggers: `closesIssue: body.issue.number`. |

## 7. Gateway endpoints

- `POST /webhooks/:source` — `404` unknown or non-webhook source, or one whose
  `sender.*From` expressions don't compile; `401` failed verification; `202`
  otherwise (relay continues in the background). Dropped events (bot,
  unresolved sender) are `202` + a log line, so senders don't retry.
  Compilation is asked of agent-orchestrator's `POST /events/compile`
  (`{expressions} → {errors: {<expr>: <message>}}`) once per distinct
  expression and cached (a "does not compile" answer for 60s, since it also
  depends on the orchestrator's CEL environment). It is asked once, with no
  retry, and times out after 3s, keeping the 202 inside providers' ~10s
  delivery timeout. If it can't answer, the source is served for the next 10s without
  asking again, and each delivery's prefilter (below) still fails closed.
  Before resolving a source's `sender` (live GitHub API calls), the gateway
  sends the preliminary envelope (`sender {}`, `repository ""`) and the
  source's `sender.*From` expressions to agent-orchestrator's
  `POST /events/prefilter` (its `/invoke` bearer):
  `{envelope, sender?} → {couldMatch, sender: {<field>: {ok, type, value} | {ok:false, error}}}`.
  `couldMatch:false` — no Ready trigger for this source could match — drops the
  delivery (`202` + log line), so pushes, check runs and other unwatched events
  cost no API calls. A trigger whose `match.cel` references `sender` or
  `repository` can't be decided before resolution and counts as a possible
  match. The sender results (an `int` as its decimal string) are then checked
  for type exactly as before; an evaluation error is an `ok:false` result and
  drops the delivery, as does a prefilter that still fails after a few retries
  (connection errors and `5xx` are retried — the call has no side effects).
  This only ever drops early; matching stays the
  orchestrator's, and an event that survives is still sender-verified.
- `POST /events/:source/tick` — `Authorization: Bearer <projected SA token>`.
  The gateway verifies it against the cluster's ServiceAccount issuer keys
  (`/.well-known/openid-configuration`, `/openid/v1/jwks`) for audience
  `integration-gateway.events`, a lifetime of at most an hour, and
  requires the username `system:serviceaccount:<CRD_NAMESPACE>:eventsource-<source>`
  — anything else (including another source's tick) is `401`, before the source
  is looked up; then `404` unless the source is `cron`; `202`.
- **The webhook is answered once the relay's accept is known** (ADR 0049 §6).
  An agent that must act as its sender, who cannot be acted as, is a `403`
  carrying `{status:"failed", error:{code, message, identityMode, target,
  sender, missing[{provider,label,linkUrl}], trigger}}`. `message` is safe to
  print to the sender. `code` is `identity_link_required`,
  `identity_ambiguous` or `identity_unavailable`. Every other outcome is `202`.
  With `reply.github.onIdentityError` the message is also posted as a comment,
  since GitHub never reads a response body. A source's `sender.accountIdFrom`
  is what resolves a sender to the subject their links live under. The gateway
  signs `{login, accountId, subject?, subjectStatus}` into the sender assertion.
- Reply adapters run only for the `reply` the orchestrator returned: `github.ack`
  (+ session page link) before polling. When the delegated agent reports a
  Remote Control URL mid-run, it is edited into that ack comment (a separate
  comment only if there is no ack to edit, the edit fails, or a "link your
  account" prompt has been posted below the ack since);
  `github.addLabels` / `github.assignees` at acceptance, alongside the ack,
  with the same retry policy as `removeLabel`;
  `github.result` / `http` after;
  `github.removeLabel` after, on every outcome, through the durable pending-action
  store so a crash mid-run still clears it.

## 8. Cron

core-controller turns each `cron` EventSource into an owned `ServiceAccount`
and `CronJob`, both `eventsource-<name>`. The ServiceAccount is the tick's
identity: `automountServiceAccountToken: false`, bound to no Role (it
authenticates; it is authorized for nothing). The CronJob
(`concurrencyPolicy: Forbid`, hardened pod) runs as it, mounting only a
projected token for audience `integration-gateway.events` (600s, pod-bound), and
runs `curl -fsS -X POST --variable token@<file> --expand-header "Authorization:
Bearer {{token:trim}}" $EVENT_TICK_URL/events/<name>/tick` — no shell, no
token on a command line. There is no shared secret: nothing to generate, store
or rotate, and it renders identically under Helm and Argo CD.

The controller never adopts a same-named ServiceAccount it does not own (that
would let someone else's pods tick the source) and deletes both objects when the
source stops being cron. Controller env: `EVENT_TICK_URL` (gateway base URL),
`EVENT_TICK_IMAGE` (curl ≥ 8.3; default `curlimages/curl:8.11.1`). Without
`EVENT_TICK_URL` a cron source is `Ready=False` (`TickNotConfigured`). The
gateway needs no extra RBAC for this (every ServiceAccount may read the
issuer keys, via `system:service-account-issuer-discovery`) and reads the
payload from the CR, not from the tick. Verifying locally, unlike
TokenReview, does not notice a ServiceAccount deleted after the token was
minted; the tick token's 10-minute life bounds that.

## 9. Run events and the software factory

[ADR 0052](adr/0052-run-events-and-software-factory.md). A trigger with
`onComplete.emitTo: <run source>` publishes each of its runs' completions as an
event, so another trigger can chain off it. The gateway builds the event at the
end of the run's lifecycle (after the poll, result comment and label removal)
and feeds it through the **same** `/invoke` matching path as a webhook. Run
events are only published for trigger-dispatched runs, never for chat or
session-page turns, and triggers without `onComplete` behave as before.

The envelope is §2's, with `kind: "run"` and this body:

```
body: {
  trigger: "github-issue-triage",            # the trigger whose run finished
  status:  "succeeded" | "failed" | "timed_out",
  params:  { repository: "o/r", number: "12", ... },  # that run's params, §4 string form
  outcome: { ... },                          # the run's structured outcome, {} when none
  chain: {
    id:     "<id of the envelope that started the chain>",
    depth:  1,                               # 1 for the first run event in a chain
    counts: { "github-issue-triage": 1 },    # runs of each trigger completed in this chain, this one included
    origin: { login, accountId?, repository },
  },
}
```

`chain.depth` and every `chain.counts` value are JSON integers (CEL `int`, §3).
The SWE agents (claude-code-swe-agent, opencode-swe-agent) report the outcome
from the runner, not the model:
`{pullRequest?: {number, url}, madeNewCommits, verdict?}`. `pullRequest` is set
when the turn ended with an open PR. `madeNewCommits` is true only when a
non-`readOnly` turn moved HEAD **and** the new HEAD reached a remote (some
remote-tracking branch contains it), so a commit whose push failed counts as no
new work. `verdict` is set only on `readOnly` runs (unless the trigger's
`target.outcome` is `coordination`, §10):
the runner tells the model to run `aktis-verdict changes-requested` or
`aktis-verdict ready` once after posting its review, validates what it recorded,
and reports `"none"` for a missing or invalid one.

**Identity.** A run event never has a bot sender: it carries the sender of the
webhook that started the chain. Before relaying each hop the gateway re-runs that
login through the source's identity resolver and `senderClaims`, so a human whose
access was revoked mid-chain stops the chain, and every hop acts as, and is
audited to, a verified person (ADR 0049 identity modes apply unchanged). The
bot-drop guard (§1) is untouched.

**Loop bounds.** Two, both deterministic:

- Per trigger, in CEL: a trigger caps itself on `body.chain.counts`, e.g.
  `!("factory-fix" in body.chain.counts) || body.chain.counts["factory-fix"] < 2`.
- Global, in the gateway: `RUN_EVENT_MAX_DEPTH` (default 12). A run event past
  it is dropped and logged, and the gateway comments on the issue/PR it would
  have replied to. No trigger config can exceed it.

**Durability.** A run survives a rollout of the gateway or the orchestrator.
Each accepted event's run is a Temporal workflow (`eventRun`, docs/adr/0057)
that the gateway's own worker runs, on the engine's Temporal: ack, poll,
result, label removal and the completion event are its steps. A gateway that
dies mid-run costs a retried activity on another replica -- at once on a
rollout (the worker cancels its activities on shutdown), within a minute on a
crash (the poll's heartbeat timeout) -- and no step that finished runs again.
The orchestrator re-attaches to a turn whose process died by the engine id it
recorded.

### The factory recipe

Shipped as `examples/event-triggers/` (`eventsource-runs.yaml`,
`eventtrigger-factory-*.yaml`) and enabled for homelab in
`orchestrator/charts/aktis/values-production.yaml`. All are repo-agnostic.

| Trigger | Matches | Target | Reply |
| --- | --- | --- | --- |
| `github-issue-triage` | `ai-triage` applied to an issue (webhook), when coordination (§10) is not installed | SWE agent | ack, result, `removeLabel`; `onComplete.emitTo: runs` |
| `factory-triage` / `factory-triage-stacked` | a coordinated issue (§10) | SWE agent | ack, result, `removeLabel: ai-triage`; `onComplete.emitTo: runs` |
| `github-pr-triage` | `ai-triage` applied to a PR (webhook) | SWE agent (PR-triage prompt) | ack, result, `removeLabel`; `onComplete.emitTo: runs` |
| `github-pr-review` | `ai-review` applied to any PR, whoever opened it (webhook); param `headRepository` (`body.pull_request.head.repo.full_name`, `""` for a deleted fork) | SWE agent, `readOnly` | ack, `removeLabel` (no result comment: the review is the output, `factory-handoff` reports the outcome, and a blocked run explains itself in a single PR comment instead of a review); `onComplete.emitTo: runs` |
| `factory-review` | `source == "runs" && kind == "run" && body.trigger in ["github-issue-triage", "github-pr-triage", "factory-fix"] && body.status == "succeeded" && (has(body.outcome.pullRequest) \|\| body.trigger in ["github-pr-triage", "factory-fix"]) && (body.trigger != "factory-fix" \|\| (has(body.outcome.madeNewCommits) && body.outcome.madeNewCommits)) && (!("factory-review" in body.chain.counts) \|\| body.chain.counts["factory-review"] < 3)` | SWE agent, `readOnly` | on the PR: ack + session page (no result comment; a blocked run explains itself in a single PR comment instead of a review), `addLabels: [ai-review]`, `removeLabel: ai-review`; `emitTo: runs` |
| `factory-fix` | `body.trigger in ["factory-review", "github-pr-review"] && has(body.outcome.verdict) && body.outcome.verdict == "changes-requested" && (!("factory-fix" in body.chain.counts) \|\| body.chain.counts["factory-fix"] < 2) && (body.trigger != "github-pr-review" \|\| (has(body.params.headRepository) && body.params.headRepository == body.params.repository))` | SWE agent (PR-triage prompt) | on the PR: ack, result, `addLabels: [ai-triage]`, `removeLabel: ai-triage`; `emitTo: runs` |
| `factory-handoff` | a review (`factory-review` / `github-pr-review`) that `factory-fix` doesn't take, or a `factory-fix` run that didn't succeed or pushed no commits | `none` | param `assignee: sender.login`; on the PR: ack with the reason, `assignees: ["{{assignee}}"]` |

So the chain runs to the end however it starts:

- **`ai-triage` on an issue** (or a new issue on an auto-triage repo): triage
  opens a PR → review → …
- **`ai-triage` on a PR**: the PR is picked up and fixed → review → …
- **`ai-review` on a PR**, including one someone else opened: the review's
  verdict routes like `factory-review`'s → …, except on a PR from a fork (see
  below).

… a `changes-requested` verdict sends the PR to a fix run, whose completion
re-enters review; after at most 2 fix rounds (so 3 reviews), or on `ready`, no
verdict, a failed review, or a fix run that failed or pushed nothing, the PR is assigned to the human who applied the
original label. The cap counts `factory-fix` rather than reviews because a
chain's first review may be either `github-pr-review` or `factory-review`, and
every entry point puts exactly one review ahead of each fix.
`factory-review` takes the PR number from `body.outcome.pullRequest.number`,
falling back to `body.params.number` for a run that was already on the PR
(`github-pr-triage`, `factory-fix`; issue triage must report an outcome PR,
since its own number is the issue). The fix and handoff triggers take the
review's own `body.params.number`. All use the PR as `sessionKey`, so fix runs
and re-reviews share one session per PR. The fix loop runs on the PR, not the
issue: the review comments live there, and issue triage would open a second PR.
**Fork PRs.** `factory-fix` holds write credentials and reads content the PR's
author controls (the diff, description and review comments), and the verdict
that starts it comes from a review of that same content. `ai-review` asks only
for a review, so a `changes-requested` verdict from `github-pr-review` starts a
fix only when the PR's branch is in the same repository
(`body.params.headRepository == body.params.repository`). On a fork PR, or a
run that has no `headRepository` param, `factory-handoff` assigns the PR
instead, with the reason that its branch is not in this repository. `ai-triage`
on a fork PR is an explicit request for writes and is not gated. Whatever
starts a fix, one that can't push (e.g. to a fork's branch the App can't write)
reports `madeNewCommits: false`, so `factory-handoff` assigns the PR rather than
re-reviewing unchanged code.

`factory-review` also follows `factory-triage`, `factory-triage-stacked` and
`factory-resync` (§10); like a fix, a resync is re-reviewed only when it pushed
(`madeNewCommits`), and one that failed or pushed nothing is handed off.

## 10. Coordinating concurrent work

[ADR 0055](adr/0055-factory-coordination.md). Factory tickets run
concurrently, so two can change the same code. Two mechanisms keep them apart:
a coordinator that decides how a new issue integrates with the work in flight
**before** it starts, and a sweep that resolves the conflicts that slip through
anyway.

### Held completions (`onComplete.waitUntilClosed`, `onComplete.serialize`)

Two gateway primitives, both on a trigger with `onComplete.emitTo`. Instead of
publishing the completion at once, the run hands it to a `heldRunEvent`
workflow (docs/adr/0057), which publishes it once every hold is released:

- `waitUntilClosed: {repositoryParam, numberParam}`: until the issue or PR
  named by those params is closed (a PR closes when it merges; an issue when
  the PR that `Closes` it merges) or no longer exists. Closing the held run's
  own issue (its `reply.github` target) abandons the wait. Not held, published
  at once and logged: params that aren't a repository and positive number, a
  wait on the run's own issue, a target that is already closed, or a **cycle**
  (the target is itself held waiting, directly or transitively, on this run's
  issue). A wait that really blocked restarts the chain's `depth` at 1 when it
  is published; `counts` are kept, so per-trigger caps still bound the chain.
- `serialize: {group, keyParam}`: at most one completion per
  `<group>:<params[keyParam]>` queue is in flight. The queue is taken when the
  completion is published and held until **the run it started** finishes (its
  completion, and that run's status labels, already out). Each queue is a
  `runQueue` workflow handing out turns oldest first; a holder that never
  releases (its workflow was terminated) is passed over after a timeout derived
  from the run settings to outlast any live holder (about 60 hours with the
  defaults: two 24-hour publish windows, plus a run's turns, link wait and
  label retries). A completion nobody accepted, or accepted but whose run
  could not be started, frees the queue at once. A publish that ended without
  an answer (its worker died, or it timed out) hands the queue to the run it
  may have started, which claims it when it starts; if no run claims it within
  15 minutes, the next completion gets its turn.

Both together hold until both are released: the wait first, then the queue.
A held wait re-reads its issues from GitHub once per
`GATEWAY_HELD_EVENT_STATE_CHECK_INTERVAL_MS` (default 5 minutes). Every
completion is published by exactly one workflow. One whose publish fails
(orchestrator unreachable, an error status, or an error re-verifying the
chain's origin) is retried with backoff for up to 24 hours after the first
failure -- held or not -- then dropped, logged, and a comment on the run's
issue says it needs a human.

### The coordinator

| Trigger | Matches | Target | Reply / onComplete |
| --- | --- | --- | --- |
| `factory-intake` | `ai-triage` applied to an issue (priority 10, so it wins over `github-issue-triage`); in production also a new issue on the auto-triage repos | `none` | `serialize: {group: factory-coordinate, keyParam: repository}` |
| `factory-coordinate` | `body.trigger in ["factory-intake", "factory-park"]` | SWE agent, `readOnly`, `outcome: coordination` | ack, result, `addLabels: [ai-triage]`, `removeLabel: ai-waiting`; `emitTo: runs` |
| `factory-park` | coordinate `succeeded`, `decision == "wait"`, a number other than the issue's own, `counts["factory-park"] < 5` | `none` | ack, `addLabels: [ai-waiting]`, `removeLabel: ai-triage`; `waitUntilClosed` on `waitFor` + the same `serialize` |
| `factory-triage-stacked` | coordinate `succeeded`, `decision == "stack"`, a number other than the issue's own | SWE agent (branch from PR #N, `--base` its branch) | ack, result, `removeLabel: ai-triage`; `emitTo: runs` |
| `factory-triage` | every other coordinate completion: `proceed`, `none`, a failed coordinator, an unroutable wait (its own number, or 5 waits already) | SWE agent | ack, result, `removeLabel: ai-triage`; `emitTo: runs` |

The coordinator records its decision like a review verdict: the runner
installs `aktis-coordinate` (`proceed` | `wait <N>` | `stack <N>`), and on a
run whose `AgentRun.spec.outcome` is `coordination` (the trigger's
`target.outcome`) it appends the coordination prompt section in place of the
verdict one and reports
`outcome.coordination: {decision: "proceed" | "wait" | "stack" | "none", number?}`
instead of a verdict. The mode comes from that structured field alone, never
from the prompt, so a review of a pull request whose description mentions
`aktis-coordinate` still reports its verdict. A missing or invalid
decision is `none`, which proceeds: a confused coordinator costs nothing over
running without one.

Because intake and park both hold their completion in one `factory-coordinate`
queue per repository, a repository's issues are coordinated one at a time, and
each coordinator sees the issues coordinated before it as in flight (they keep
`ai-triage` until their triage run ends). Compare numbers in CEL as strings:
`body.params` values are §4 string forms, so
`string(body.outcome.coordination.number) != body.params.number`.

### The conflict sweep (`kind: conflict`)

A `conflicts` source makes the gateway sweep, every `intervalSeconds`, the open
pull requests `author` opened (GitHub search, one page of 100). For each PR
GitHub reports as conflicting (`mergeable: false`, `mergeable_state: "dirty"`)
it emits one event, **once per (PR, base commit)**: a conflict the resync could
not resolve is retried only after the base moves again.

```
{ source: "pr-conflicts", kind: "conflict", sender: {login: <assignee>, isBot: false},
  repository: "o/r",
  body: { pullRequest: { number, url, title, baseRef, baseSha, headRef, headSha } } }
```

Skipped: drafts; PRs carrying a `skipLabels` label (a factory run is already on
them and syncs the branch itself); PRs whose mergeability GitHub hasn't computed
yet (`null` right after the base moved; the next sweep sees it); and PRs with
no assignee who verifies through the identity resolver and was assigned by
the App itself (the latest `assigned` timeline event for that login has
`author` as its actor; an assignment a person made is never trusted). The
event's sender is that assignee (the factory's handoff assigns the human who
started the chain),
never the bot or whoever merged the other PR; it starts a new chain. Each
source is one `conflictSweep` workflow (docs/adr/0057), started by the gateway
when it sees the source and ending when the source goes away; the (PR, base
commit) marks are its own state, kept for 30 days (a failed scan skips one sweep
and keeps them). `factory-resync` (examples, production) merges the
base into the branch as that person, and its completion goes to review or
handoff like a fix's.

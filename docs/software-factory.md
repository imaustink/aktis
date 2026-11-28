# Setting up the software factory

The software factory turns GitHub labels into a closed loop: an issue labeled
`ai-triage` gets coordinated against other in-flight work, implemented, opened
as a PR, reviewed, fixed, re-reviewed, and handed to a human. A human is only
needed at the start (apply a label) and the end (merge).

This page covers deploying it on an existing Aktis install. It's written so a
coding agent can follow it step by step. The design rationale is in ADRs
[0052](adr/0052-run-events-and-software-factory.md) (run events, the review
loop), [0055](adr/0055-factory-coordination.md) (coordination, conflict sweep)
and [0057](adr/0057-event-runs-on-temporal.md) (durability on Temporal). The
normative contract for every field used below is
[event-triggers.md §9–§10](event-triggers.md#9-run-events-and-the-software-factory).
The manifests themselves are in [`examples/event-triggers/`](../examples/event-triggers/).

> **If you are an AI agent doing this setup:** there's a skill for it,
> [`.claude/skills/configure-software-factory/`](../.claude/skills/configure-software-factory/SKILL.md).
> It runs the steps below in order with checkpoints. Rules that apply either way:
> - Never invent secret values, App IDs or private keys. Ask the human for them,
>   or give them the exact `kubectl create secret` command to run.
> - Apply everything to the **release namespace**: the one integration-gateway
>   and agent-orchestrator run in and watch. Confirm which namespace and
>   kube context that is before writing anything. The user's default context
>   may be a different, live cluster.
> - Render the triggers from the examples with `render-factory.sh`. Don't
>   retype them. The CEL in them is interlocking (see [§6](#6-invariants-when-you-customize)),
>   and a typo silently breaks a hop.

## 1. What it does

```
 human: ai-triage on issue ─► factory-intake ─(queued per repo)─► factory-coordinate (read-only)
                                                                    │ aktis-coordinate …
                         ┌──────────────────────────────────────────┼─────────────────────────┐
                         ▼ proceed / none / failed                  ▼ stack <N>               ▼ wait <N>
                   factory-triage                         factory-triage-stacked        factory-park
                         │ opens PR                                 │ PR on #N's branch     │ held until #N closes,
                         └─────────────────┬────────────────────────┘                      │ then back to coordinate
                                           ▼
 human: ai-triage on PR ─► github-pr-triage ─►  factory-review (read-only) ◄──────────────┐
 human: ai-review on PR ─► github-pr-review ─┐        │ aktis-verdict …                     │
                                             └────────┤                                     │
                                                      ├─ changes-requested (≤ 2 fixes) ─► factory-fix ─┘ (pushed commits)
                                                      └─ ready / none / cap / failed ──► factory-handoff: assign PR to the human
 gateway conflict sweep: factory PR now conflicts ─► factory-resync ─► factory-review …
```

Entry points (all run to the end):

| A human does | Chain |
| --- | --- |
| applies `ai-triage` to an issue (or opens one, if auto-triage is on, [§4.8](#48-optional-auto-triage-new-issues)) | intake → coordinate → triage → review ⇄ fix → handoff |
| applies `ai-triage` to a PR | PR triage (address feedback, fix CI, sync base) → review ⇄ fix → handoff |
| applies `ai-review` to a PR (anyone's) | review → fix (same-repo branches only) ⇄ review → handoff |
| nothing: a factory PR starts conflicting after another merges | resync (merge base in) → review ⇄ fix → handoff |

The end state is always a PR **assigned to the human who started the chain**,
with a comment saying why it stopped: ready, review cap reached, no verdict, a
failed run, or a fork PR.

## 2. How it works

You need this to configure and debug it. Everything here is generic platform
machinery; the factory itself is just EventTrigger CRs.

- **The chain never goes through GitHub.** The gateway drops every webhook a
  bot sent (the self-trigger guard), so a label the bot adds can't start
  anything. Instead, a trigger with `onComplete.emitTo: runs` publishes its
  run's completion as a `kind: "run"` event on the `runs` EventSource
  (`spec.run: {}`). The next trigger matches that event with CEL. Labels the
  factory adds (`ai-review`, `ai-triage`, `ai-waiting`) are status displays
  only.
- **Run event body** (what every `factory-*` match reads):
  ```
  body.trigger   name of the trigger whose run finished
  body.status    succeeded | failed | timed_out
  body.params    that run's params, all as STRINGS (compare numbers with string())
  body.outcome   { pullRequest?: {number,url}, madeNewCommits, verdict?, coordination? }
  body.chain     { id, depth, counts: {<trigger>: n}, origin: {login, repository} }
  sender.login   the human who started the chain, re-verified on every hop
  ```
- **Outcomes come from the agent runner, not the model.** The SWE agents
  (`claude-code-swe-agent`, `opencode-swe-agent`) report `pullRequest` (the
  open PR the turn ended on) and `madeNewCommits` (true only if new commits
  actually reached the remote).
  - On a read-only run, the runner tells the model to run
    `aktis-verdict changes-requested|ready` and records it as
    `outcome.verdict`. A missing or invalid verdict becomes `none`.
  - On a run whose trigger sets `target.outcome: coordination`, the model
    runs `aktis-coordinate proceed|wait <N>|stack <N>`, recorded as
    `outcome.coordination`.
  - So the model's judgment is one enum, and routing on it is CEL. Every
    unclear result fails safe: `none` hands the PR to a human, and a
    coordinator with no decision proceeds.
- **`target.none: {}`** runs nothing; it only runs the `reply` side effects
  (comment, labels, assignees). Handoff, intake and park use it.
- **Held completions** (gateway, on Temporal):
  - `onComplete.serialize: {group, keyParam}` lets only one completion per
    `<group>:<param>` be in flight. The factory uses it to coordinate a
    repository's issues one at a time.
  - `onComplete.waitUntilClosed` holds a completion until an issue or PR
    closes. That's how "wait for #N to merge" works. The wait has a cycle
    guard, and closing the waiting issue cancels it.
- **Conflict sweep:** an EventSource with `spec.conflicts` makes the gateway
  poll the App's open PRs every `intervalSeconds`. It emits one
  `kind: "conflict"` event per (PR, base commit) for each PR GitHub reports
  as `dirty`. The event runs as the PR's assignee, and only if the **App
  itself** made that assignment.
- **Loop bounds:**
  - per trigger, in CEL: at most 2 fixes (so 3 reviews) and at most 5 waits;
  - globally in the gateway: `RUN_EVENT_MAX_DEPTH`, 12 by default.
- **Identity:** every hop acts as the human who applied the label, re-verified
  each hop. If their access is revoked mid-chain, the chain stops. The
  Agent's `allowedRoles` and `identityProviders` apply on every hop.

## 3. Prerequisites

Check each one before installing triggers. The commands are read-only.

| # | Requirement | How to check |
| --- | --- | --- |
| P1 | Aktis is deployed with the **Temporal engine** (the only engine since ADR 0053), and its CRDs are recent enough to have `EventSource.spec.run`/`spec.conflicts` and `EventTrigger.spec.onComplete.serialize`/`waitUntilClosed` and `target.outcome`/`target.none` | `kubectl explain eventtrigger.spec.onComplete.serialize` and `kubectl explain eventsource.spec.conflicts` both succeed |
| P2 | **integration-gateway** enabled, with `integration-gateway.temporal.address` and `.namespace` pointing at the engine's Temporal (the namespace must already exist; the gateway doesn't create it) | gateway pod is Ready; its logs show the Temporal worker started on task queue `integration-gateway` |
| P3 | A public HTTPS route to the gateway's `POST /webhooks/github` | `curl -sS -o /dev/null -w '%{http_code}' -X POST https://<host>/webhooks/github` gives `401` (signature refused), not `404`/`502` |
| P4 | A **GitHub App** installed on the target repositories ([§4.1](#41-github-app)) | App settings page, "Install App" |
| P5 | A SWE **Agent** CR is enabled (`claude-code-swe-agent` or `opencode-swe-agent`, from the community-components chart), using the same GitHub App's credentials | `kubectl get agents -n <ns>` lists it, Ready |
| P6 | Sender-assertion secret set on **both** sides: `GATEWAY_SENDER_ASSERTION_SECRET` (gateway, and temporal-engine `senderAssertion`) equals `AGENT_SENDER_ASSERTION_SECRET` (agent-orchestrator). Without it, agent-orchestrator fails Temporal-mode trigger turns rather than send them unsigned | both Secrets have the key with the same value |
| P7 | Redis for the orchestrator/gateway (the chart's bundled `agent-orchestrator.redis.enabled: true` is fine) | `kubectl get pods -n <ns> \| grep redis` |

## 4. Setup, step by step

### 4.1 GitHub App

Use **one** App for the gateway and the SWE agent. The conflict sweep finds
PRs by `author: app/<slug>` and only trusts assignments made by
`<slug>[bot]`, so the App that opens PRs must be the same App that assigns
them.

- **Repository permissions:**
  - Contents: read & write (the agent pushes branches)
  - Pull requests: read & write (open PRs, review, label, assign)
  - Issues: read & write (comments, labels, assignees)
  - Metadata: read
  - Checks and Actions: read (the fix prompts run `gh pr checks` and
    `gh run view --log-failed`)
  - Workflows: read & write, if the repositories have `.github/workflows/`
    (pushes that touch workflow files, including merge-base resyncs that pull
    in workflow changes from the base branch, are rejected without it)
- **Organization permissions:** Members: read, but only if you grant roles by
  team (`githubTeamRoles`).
- **Subscribe to events:** Issues, Pull request.
- **Webhook URL:** `https://<gateway host>/webhooks/github`. The path segment
  is the EventSource's name, `github`.
- **Webhook secret:** a random string. The same value goes into the gateway
  Secret.
- Install it on every repository the factory should work in. The triggers are
  repo-agnostic: any repository the App is installed on is in scope.
- Write down the **App ID**, the **installation ID**, a **private key** (PEM),
  and the **slug** (from the App's URL: `github.com/apps/<slug>`).

### 4.2 Secrets

The gateway uses the chart's bring-your-own Secret
(`integration-gateway.secrets.existingSecret`). The factory needs these keys
in it (the key names are the chart defaults):

```sh
kubectl create secret generic integration-gateway-secrets -n <ns> \
  --from-literal=GITHUB_WEBHOOK_SECRET=<the App's webhook secret> \
  --from-literal=GITHUB_APP_ID=<app id> \
  --from-literal=GITHUB_APP_INSTALLATION_ID=<installation id> \
  --from-file=GITHUB_APP_PRIVATE_KEY=<path to the .pem> \
  --from-literal=GATEWAY_SENDER_ASSERTION_SECRET=<same value as AGENT_SENDER_ASSERTION_SECRET> \
  ... # plus whatever your gateway already uses (orchestrator token / OIDC client secret, identity-link keys)
```

If the Secret already exists, add the keys with `kubectl edit` or a patch.
Don't recreate it, because that drops the keys that are already there.

When all three `GITHUB_APP_*` keys are set, the gateway mints App
installation tokens and ignores `GITHUB_TOKEN`.

The SWE agent's Secret (`claude-code-swe-secrets` or `opencode-swe-secrets`,
see `orchestrator/charts/community-components/values.yaml`) needs the same
three `GITHUB_APP_*` values, plus model credentials:

- `ANTHROPIC_API_KEY`; or
- for claude-code, per-user Claude linking via `identityLink.providers: [claude]`.

### 4.3 Who may start the factory

The human who applies the label must verify through the gateway's GitHub
identity resolver and hold a role the Agent allows. Both SWE Agents allow
`writer`. In the integration-gateway values, map GitHub permissions to roles
with one of:

```yaml
integration-gateway:
  config:
    # by the sender's collaborator permission on the repo the webhook came from
    githubCollaboratorRoles: '{"admin": ["writer"], "write": ["writer"]}'
    # or, for an org, by team:  '{"my-org/my-team": ["writer"]}'
    githubTeamRoles: ""
```

If the Agent declares `identityProviders` (e.g. `claude` for
claude-code-swe-agent with per-user Claude auth), each person who triggers it
must have linked that account once, on the gateway's Connections page.
Otherwise the run asks them to link instead of starting.

### 4.4 The SWE agent

In the community-components chart values, enable one agent and give it the
App:

```yaml
claudeCodeSweAgent:          # or opencodeSweAgent
  enabled: true
  secretName: claude-code-swe-secrets
  githubAppIdSecretKey: GITHUB_APP_ID
  githubAppPrivateKeySecretKey: GITHUB_APP_PRIVATE_KEY
  githubAppInstallationIdSecretKey: GITHUB_APP_INSTALLATION_ID
  githubAppSlug: <slug>      # commits/PRs are attributed to <slug>[bot]
  anthropicSecretKey: ANTHROPIC_API_KEY   # or "" with identityLink providers [claude]
```

Both agents implement `aktis-verdict` and `aktis-coordinate`. The Agent CR's
name is the `--agent` value for the render step.

### 4.5 Gateway settings worth checking

```yaml
integration-gateway:
  temporal:
    address: temporal-frontend.temporal.svc:7233   # the engine's Temporal
    namespace: default                              # must exist; the engine's namespace
  config:
    # MUST exceed the agent run timeout (agent-orchestrator default 8h), or
    # long runs are reported failed and the chain hands off early. Production
    # uses 8h5m:
    pollTimeoutMs: "29100000"
    # runEventMaxDepth: 12      # global hop cap; per-trigger caps live in CEL
  sessionPage:
    publicBaseUrl: "https://<gateway host>"   # optional: live session link in ack comments
```

### 4.6 Render and install the triggers

Generate the manifests from the examples. This substitutes your Agent and App
slug, and for Helm it escapes `{{placeholders}}` for the chart's `tpl`:

```sh
S=.claude/skills/configure-software-factory/render-factory.sh

# Plain manifests:
$S --agent claude-code-swe-agent --app-slug <slug> > factory.yaml
kubectl apply -n <ns> -f factory.yaml

# Or as aktis-chart values (merge into the existing extraManifests list):
$S --agent claude-code-swe-agent --app-slug <slug> --format helm > factory-values.yaml
```

Options:

| Flag | Effect |
| --- | --- |
| `--no-coordination` | Skip ADR 0055's coordinator: `ai-triage` on an issue goes straight to `github-issue-triage`. Simpler and one fewer agent run per ticket, but concurrent tickets may collide (the conflict sweep still repairs it). |
| `--no-conflict-sweep` | Omit `pr-conflicts` and `factory-resync`. |
| `--no-github-source` | You already have an EventSource named `github`. Its name must be `github`, because every trigger's `sourceRef` uses that name. |
| `--with-issue-review` | Also `ai-review` on an issue (a read-only review, outside the loop). |

Install everything the script emits **together**. A missing trigger doesn't
error; it just leaves that branch of the chain with no listener, so the run
completes and nothing happens next.

If you deploy with GitOps (Argo CD / Helm values in another repo), commit the
`--format helm` output to that repo's values. `orchestrator/charts/aktis/values-production.yaml`
is a worked example of exactly this (`extraManifests`).

### 4.7 Labels

In each repository, create `ai-triage`, `ai-review` and `ai-waiting`
(`gh label create ai-triage -R owner/repo`, …). Humans need `ai-triage` and
`ai-review` to exist to apply them. The bot adds all three as status markers.
If you rename them, see [§6](#6-invariants-when-you-customize).

### 4.8 Optional: auto-triage new issues

Production also starts the factory when a trusted person **opens** an issue,
without needing a label. To do that, replace `factory-intake`'s `match.cel`:

```
headers["x-github-event"] == "issues" &&
((body.action == "labeled" && body.label.name == "ai-triage") ||
 (body.action == "opened" &&
  body.repository.full_name in ["owner/repo-a", "owner/repo-b"] &&
  body.issue.author_association in ["OWNER", "MEMBER", "COLLABORATOR"] &&
  !body.issue.labels.exists(l, l.name == "ai-triage")))
```

The `!labels.exists(...)` clause matters. An issue opened *with* the label
also gets a `labeled` delivery, and without the clause it would be queued
twice. The `author_association` check keeps strangers on public repositories
out.

With `--no-coordination` there is no `factory-intake`; put this CEL on
`github-issue-triage` instead, and change its `label: body.label.name` param
to `label: 'has(body.label) ? body.label.name : "ai-triage"'`. An `opened`
delivery has no `body.label`, so the original param fails to evaluate, the
gateway answers 422, and no run starts. (`factory-intake`'s params never read
`body.label`, so it doesn't need this.)

## 5. Trigger reference

Every trigger lives in `examples/event-triggers/eventtrigger-<name>.yaml`.

| Trigger | Source | Fires on | Target | GitHub side effects | Emits to `runs` |
| --- | --- | --- | --- | --- | --- |
| `factory-intake` | github | `ai-triage` labeled on an issue (priority 10) | `none` | none | yes, serialized per repo (`factory-coordinate:<repo>`) |
| `factory-coordinate` | runs | `factory-intake` or `factory-park` completed | agent, `readOnly`, `outcome: coordination` | ack + result on issue; adds `ai-triage`, removes `ai-waiting` | yes |
| `factory-park` | runs | coordinate succeeded, `wait <N>`, N ≠ own issue, < 5 parks | `none` | ack; adds `ai-waiting`, removes `ai-triage` | yes, held until #N closes, plus the same serialize queue |
| `factory-triage-stacked` | runs | coordinate succeeded, `stack <N>`, N ≠ own issue | agent | ack + result; removes `ai-triage`; `Closes #issue` | yes |
| `factory-triage` | runs | every other coordinate completion (exact complement of park + stacked) | agent | ack + result; removes `ai-triage`; `Closes #issue` | yes |
| `github-issue-triage` | github | `ai-triage` on an issue. Only used with `--no-coordination`; intake outranks it otherwise | agent | ack + result; removes label | yes |
| `github-pr-triage` | github | `ai-triage` labeled on a PR | agent | ack + result; removes label | yes |
| `github-pr-review` | github | `ai-review` labeled on any PR | agent, `readOnly` | ack; removes label (the review is the output) | yes |
| `factory-review` | runs | a triage/pr-triage/fix/resync run succeeded with a PR; fix/resync must have pushed; < 3 reviews | agent, `readOnly` | ack; adds + later removes `ai-review` | yes |
| `factory-fix` | runs | review verdict `changes-requested`, < 2 fixes, and (for `github-pr-review`) a same-repo branch | agent | ack + result; adds + later removes `ai-triage` | yes |
| `factory-handoff` | runs | every review outcome `factory-fix` doesn't take, and every fix/resync that failed or pushed nothing | `none` | ack with the reason; **assigns the PR to `sender.login`** | no (end of chain) |
| `factory-resync` | pr-conflicts | conflict event | agent (merge base in, never force-push) | ack + result; adds + later removes `ai-triage` | yes |

Every agent hop uses `sessionKey: github:<repo>#<number>` on the PR, so
reviews and fixes on a PR share one conversation. The exception is the
coordinator, which uses `factory-coordinate:<repo>#<issue>`.

## 6. Invariants when you customize

Most edits are safe: prompts, ack text, the agent, caps. These ones aren't
obvious:

1. **Trigger names are referenced in other triggers' CEL.** The
   `body.trigger in [...]` lists in `factory-review`, `factory-fix`,
   `factory-handoff` and `factory-coordinate`, and the
   `body.chain.counts["…"]` keys, all name triggers. If you rename a trigger,
   grep for its old name in all of them.
2. **Complements must stay complements.**
   - `factory-fix` and `factory-handoff` must partition review completions.
     Handoff's match is literally `!(<fix's condition>)`.
   - `factory-triage` must be the complement of `factory-park` +
     `factory-triage-stacked` among coordinate completions.
   - If you change a cap or condition on one side, change the mirror. If you
     don't, a PR either gets no owner (no match) or gets both a fix and a
     handoff.
3. **Caps appear twice:** fix ≤ 2 is in both fix and handoff; park ≤ 5 is in
   both park and triage. `factory-review`'s `< 3` is a backstop.
4. **`body.params` values are strings.** Compare with
   `string(body.outcome.coordination.number) != body.params.number`, not as
   ints.
5. **Labels:**
   - `pr-conflicts.spec.conflicts.skipLabels` must list the labels the
     factory puts on PRs it is working: `ai-triage`, `ai-review`,
     `ai-waiting`.
   - The coordinator prompt finds in-flight issues by `--label ai-triage`.
   - Rename all of these together.
6. **`conflicts.author` must be `app/<slug>`** of the App that opens the PRs
   **and** whose installation token the gateway uses. Otherwise the sweep finds
   nothing, or skips every PR with "no App-assigned assignee".
7. **Keep `readOnly: true`** on `factory-review`, `github-pr-review` and
   `factory-coordinate`, and **`outcome: coordination`** only on
   `factory-coordinate`. These are the deterministic write gate and mode
   switch. The prompt can't substitute for them.
8. **The `github` EventSource keeps `dropBots` at its default (true).** Turning
   it off doesn't make the chain work (run events already do that). It only
   lets the bot trigger itself.
9. **Fork safety:** keep the `headRepository == repository` clause in
   `factory-fix`/`factory-handoff`. Without it, `ai-review` on an outside
   contributor's PR would hand a write-credentialed run to content that
   contributor controls.

## 7. Verify

```sh
kubectl get eventsources,eventtriggers -n <ns>
```

Every row should be `Ready=True`. If one isn't, its condition reason says why:

- `SourceNotFound`, `TargetNotFound`: the source or Agent name is wrong.
- `EmitToNotFound`: the `runs` source is missing.
- `InvalidOnComplete`: a `*Param` names a key not in `spec.params`.
- A compile error: bad CEL.

Then run a smoke test on a throwaway repository the App is installed on:

1. Open a small, concrete issue (e.g. "add a `--version` flag") and apply
   `ai-triage` as a person who has the `writer` role.
2. On the issue, within seconds: "Checking this against other in-flight
   work…", then the coordinator's decision as a comment, then "Starting work
   on this now."
3. A PR appears. On it, "Starting an automated review…" and the `ai-review`
   label, then one review.
4. Either "Addressing the automated review's feedback…" and another review, or
   the handoff comment and the PR **assigned to you**.

If a step never comes, use the next section.

## 8. Troubleshooting

| Symptom | Likely cause | Where to look |
| --- | --- | --- |
| Label applied, nothing happens, no ack | Webhook not reaching the gateway, bad signature, or the sender didn't verify (wrong roles) | GitHub App → Advanced → Recent Deliveries (status code); gateway logs for the delivery id (`webhook github event <id>: dropped (…)` / `no trigger could match`) |
| Webhook `202`, but no ack | No Ready trigger matched (`ignored`), or an identity 403 (the agent needs a linked account) | `kubectl get eventtriggers`; agent-orchestrator logs for the trigger name; the issue may have a "link your account" comment |
| Triage opened a PR, no review followed | `onComplete.emitTo` missing on the triage trigger; `factory-review` not installed or not Ready; or the run reported no `outcome.pullRequest` (an agent too old to report outcomes) | gateway logs (`event runs/<id>: no trigger matched (ignored)`, `run event …: publish failed`, `dropped, chain origin … no longer verifies`); `GET /invoke/:id` on the orchestrator shows `outcome` |
| Review posted, then silence | The review emitted, but neither fix nor handoff matched. Their CEL drifted apart ([§6](#6-invariants-when-you-customize).2) | Compare the two `match.cel` against the examples |
| Always handed off with "did not record a verdict" | The model isn't running `aktis-verdict`; the agent image is older than ADR 0052; or the trigger isn't `readOnly` | the review run's logs/session page |
| Fix ran, then handoff "pushed no changes" | Push failed (fork branch, branch protection, missing Contents write) or nothing was changed | fix run's result comment |
| Issue stuck with `ai-waiting` | Expected while #N is open. It resumes when #N closes. Close the issue to cancel. If #N is closed and it's still stuck, check the gateway's Temporal worker | Temporal UI: `heldRunEvent` workflows on task queue `integration-gateway` |
| Conflicting factory PR never resynced | It carries a `skipLabels` label; it's a draft; `conflicts.author` doesn't match the App; or the assignee wasn't assigned by the App | gateway logs `conflicts, but skipped` / `not running as assignee` |
| Chain stops with a "depth" comment | Hit `RUN_EVENT_MAX_DEPTH` | raise `integration-gateway.config.runEventMaxDepth` only if the chain is legitimately long |
| Every run "failed" after ~15 min although the agent finished | `pollTimeoutMs` is below the agent run timeout ([§4.5](#45-gateway-settings-worth-checking)) | gateway values |
| Trigger turns fail with a missing-assertion error | `GATEWAY_SENDER_ASSERTION_SECRET` / `AGENT_SENDER_ASSERTION_SECRET` unset or different (P6) | both Secrets |

## 9. Tunables

| What | Where | Default |
| --- | --- | --- |
| Fix rounds per chain | `factory-fix` + `factory-handoff` CEL: `counts["factory-fix"] < 2` | 2 (so 3 reviews) |
| Waits per issue | `factory-park` + `factory-triage` CEL: `counts["factory-park"] < 5` | 5 |
| Global hop cap | `integration-gateway.config.runEventMaxDepth` | 12 |
| Conflict sweep interval | `pr-conflicts.spec.conflicts.intervalSeconds` (min 60) | 300 |
| How often a held wait re-reads GitHub | gateway env `GATEWAY_HELD_EVENT_STATE_CHECK_INTERVAL_MS` | 5 min |
| Which agent does each step | each trigger's `target.agentRef` (you can mix, e.g. a cheaper model for review) | from `--agent` |
| Review style, fix scope | each trigger's `promptTemplate` | see examples |

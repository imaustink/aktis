# Event trigger examples

Bare `EventSource` / `EventTrigger` manifests showing how to wire different
kinds of events to the platform (ADR 0048, contract in
[docs/event-triggers.md](../../docs/event-triggers.md)). They are not part of
any chart: copy the ones you want, edit them, and apply them to the **release
namespace** (the one integration-gateway and agent-orchestrator watch).

```sh
kubectl apply -n controller-agent -f examples/event-triggers/eventsource-github.yaml
kubectl apply -n controller-agent -k examples/event-triggers   # all of them
```

To ship them with a Helm release instead, paste them into the aktis
chart's `extraManifests`, which is how `orchestrator/charts/aktis/values-production.yaml`
installs production's GitHub triggers. `extraManifests` runs each entry through
`tpl`, so escape every `{{placeholder}}` as ``{{`{{placeholder}}`}}``.

## Sources

| File | Route | Verified by | Secret to create |
|---|---|---|---|
| `eventsource-github.yaml` | `POST /webhooks/github` | `hmacSha256` (`x-hub-signature-256`) + GitHub sender check, bots dropped | the App's webhook secret (`integration-gateway-secrets`, key `GITHUB_WEBHOOK_SECRET`) |
| `eventsource-slack.yaml` | `POST /webhooks/slack` | `slackV0` signing secret; answers `url_verification` | `slack-event-source`, key `signingSecret` |
| `eventsource-ci.yaml` | `POST /webhooks/ci` | `bearer` (`Authorization: Bearer <token>`) | `ci-event-source`, key `token` |
| `eventsource-generic-hmac.yaml` | `POST /webhooks/generic-hmac` | `hmacSha256` (`x-signature-256: sha256=<hex>`) | `generic-hmac-event-source`, key `secret` |
| `eventsource-nightly-sweep.yaml` | cron `0 7 * * *` | the tick's own ServiceAccount (issuer keys) | none |
| `eventsource-runs.yaml` | none: run-completion events, synthesized by the gateway (ADR 0052) | the chain's origin sender, re-verified per hop | none |

## Triggers

| File | Source | Target | What it shows |
|---|---|---|---|
| `eventtrigger-github-issue-triage.yaml` | github | agent | `ai-triage` on an issue: implement + open a PR; GitHub ack/result comments, label removed after; `onComplete.emitTo: runs` starts the factory |
| `eventtrigger-github-issue-review.yaml` | github | agent, `readOnly` | `ai-review` on an issue: review only, no write credentials |
| `eventtrigger-github-pr-triage.yaml` | github | agent | `ai-triage` on a PR: address feedback, push, sync with base; `onComplete.emitTo: runs` sends it to factory review |
| `eventtrigger-github-pr-review.yaml` | github | agent, `readOnly` | `ai-review` on any PR (including one someone else opened): inline code review; `onComplete.emitTo: runs` routes its verdict to factory fix/handoff |
| `eventtrigger-slack-mention-chat.yaml` | slack | orchestrator | an @-mention becomes a normal chat turn, one session per thread |
| `eventtrigger-ci-failure-skill.yaml` | ci | skill, `readOnly` | a failed CI run forces a Skill; optional signed `reply.http` |
| `eventtrigger-generic-hmac-tool.yaml` | generic-hmac | tool | runs one Tool (web-fetch) on a URL from the body |
| `eventtrigger-nightly-stale-sweep.yaml` | nightly-sweep | agent, `readOnly` | a scheduled report; params come from the cron payload |
| `eventtrigger-factory-review.yaml` | runs | agent, `readOnly` | factory: review the PR an issue-triage/PR-triage/fix run ended on (max 3 reviews per chain); `addLabels: [ai-review]` |
| `eventtrigger-factory-fix.yaml` | runs | agent | factory: a `changes-requested` verdict (from `factory-review` or `github-pr-review`) sends the PR to a fix run, which loops back to review (max 2 fix rounds) |
| `eventtrigger-factory-handoff.yaml` | runs | `none` | factory: any other review outcome assigns the PR to the human who started the chain |

Each trigger names the agent, skill or tool it runs (`opencode-swe-agent`,
`cluster-debug-skill`, `web-fetch` from community-components). A trigger whose
source or target does not exist reports `Ready=False` (`SourceNotFound` /
`TargetNotFound`, or `EmitToNotFound` / `EmitToNotRunSource` for an
`onComplete.emitTo` that doesn't name a run source) and dispatches nothing --
`kubectl get eventtriggers` shows it.

The three `factory-*` triggers plus `eventsource-runs.yaml` and the
`onComplete` on the issue-triage, PR-triage and PR-review triggers make up the label-driven software factory of
[ADR 0052](../../docs/adr/0052-run-events-and-software-factory.md); see
[docs/event-triggers.md §9](../../docs/event-triggers.md#9-run-events-and-the-software-factory).

---
name: configure-software-factory
description: Set up, audit or change the Aktis software factory (label-driven issue → PR → review → fix → handoff loop built from EventSources/EventTriggers) on an Aktis deployment. Use when asked to enable/install/configure the software factory, the ai-triage/ai-review triggers, factory-* EventTriggers, run-completion events, factory coordination or the PR conflict sweep, or to debug why a factory chain stopped.
---

# Configure the Aktis software factory

The reference for everything here is `docs/software-factory.md`. Read it
first, all of it. Then work through the phases below in order. Each phase
ends with a check. Don't start the next phase until the check passes, or the
human has explicitly accepted the gap.

## Ground rules

- **Find the target first.** Ask for, or confirm, the kube context and the
  Aktis **release namespace** (where integration-gateway and
  agent-orchestrator run). Use `kubectl --context <ctx> -n <ns>` on every
  command. Never rely on the current context: the user's default can be a
  different, live cluster. Never switch contexts to get past a check.
- **How is Aktis deployed?** Helm values in a GitOps repo, or `kubectl apply`?
  If it's GitOps, edit the values in that repo and let the human merge;
  `kubectl apply` would be reverted by the next sync. If you can't tell, ask.
- **Secrets come from the human.** Never generate or guess App IDs, private
  keys, installation IDs or webhook secrets that already exist elsewhere.
  Print the exact `kubectl create secret` / `kubectl patch` command with
  placeholders and ask the human to run it, or to give you the values. Random
  shared secrets (the webhook secret, the sender-assertion secret) you may
  generate with `openssl rand -hex 32`. Show them to the human, because the
  webhook secret also goes into GitHub.
- **Don't hand-write factory triggers.** Render them with
  `.claude/skills/configure-software-factory/render-factory.sh` (run it from
  the repo root; `--help` lists the flags). The CEL across the triggers is
  interlocking (doc §6).
- Changes that touch something outside this repo need the human's go-ahead
  first: applying to a cluster, editing another repo, creating GitHub labels,
  changing GitHub App settings.

## Phase 1: Gather inputs

Collect these, and ask for anything you can't discover:

| Input | Where it comes from |
| --- | --- |
| kube context + release namespace | human; confirm with `kubectl get deploy -n <ns>` showing integration-gateway and agent-orchestrator |
| deploy method (GitOps repo + values file path, or kubectl) | human |
| GitHub App slug, App ID, installation ID, private key | GitHub App settings (human) |
| gateway public host | existing ingress: `kubectl get ingress -n <ns>` |
| SWE Agent to use | `kubectl get agents -n <ns>`. Prefer whichever is enabled: `claude-code-swe-agent` or `opencode-swe-agent` |
| target repositories | human. The App must be installed on them |
| coordination on/off, conflict sweep on/off, auto-triage repos | human. Default: coordination on, sweep on, no auto-triage |

## Phase 2: Audit prerequisites (read-only)

Run the checks in doc §3 (P1–P7) and report a table: requirement, status,
evidence. Common gaps and what to do about them:

- **CRDs too old** (`kubectl explain eventtrigger.spec.onComplete.serialize`
  fails): the Aktis release predates ADR 0055/0057. The deployment's chart and
  image versions need upgrading first. Stop and tell the human; don't try to
  apply CRDs piecemeal.
- **Gateway has no Temporal config:** set `integration-gateway.temporal.address`
  and `.namespace` to the engine's (copy them from `temporal-engine.temporal`).
- **No sender-assertion secret:** both
  `integration-gateway-secrets.GATEWAY_SENDER_ASSERTION_SECRET` and
  `agent-orchestrator-secrets.AGENT_SENDER_ASSERTION_SECRET` must exist with
  the same value. The temporal-engine gateway reads it too:
  `temporal-engine.gateway.senderAssertion.secretName`.
- **`pollTimeoutMs` below the agent run timeout** (chart default 15 min vs an 8h
  run): set it to `"29100000"`.

Check: every P1–P7 item is green, or the human has signed off on the gap.

## Phase 3: GitHub App and secrets

1. Give the human the App settings checklist from doc §4.1: permissions,
   events (Issues, Pull request), webhook URL
   `https://<host>/webhooks/github`, webhook secret. Wait until they confirm
   it's done.
2. Make sure the gateway Secret and the SWE agent's Secret both hold the same
   App's `GITHUB_APP_ID` / `GITHUB_APP_INSTALLATION_ID` /
   `GITHUB_APP_PRIVATE_KEY` (doc §4.2). Check which keys are present without
   printing values:
   `kubectl get secret <name> -n <ns> -o jsonpath='{.data}' | jq 'keys'`.
3. Make sure the agent chart values have `githubAppSlug: <slug>` and the three
   `githubApp*SecretKey` fields (doc §4.4).
4. Make sure gateway `config.githubCollaboratorRoles` (or `githubTeamRoles`)
   gives the intended people the `writer` role (doc §4.3).

Check: in the App's "Recent Deliveries", a redelivery or ping to the webhook
URL gets `202`, not `401` or `404`.

## Phase 4: Render and install the triggers

```sh
.claude/skills/configure-software-factory/render-factory.sh \
  --agent <agent> --app-slug <slug> [--format helm] [--no-coordination] [--no-conflict-sweep] [--no-github-source]
```

- Pass `--no-github-source` if `kubectl get eventsource github -n <ns>`
  already exists. Then check that its spec matches
  `examples/event-triggers/eventsource-github.yaml`: hmacSha256, a `sender`
  block, and `dropBots` not set to false.
- Kubectl deployments: show the human the rendered file, then
  `kubectl apply -n <ns> -f <file>`.
- Helm/GitOps deployments: use `--format helm` and **append** the entries to
  the existing `extraManifests` list in that values file. Never replace the
  list, and keep the `{{`{{x}}`}}` escaping exactly as rendered.
- For auto-triage of new issues, edit `factory-intake`'s `match.cel` as shown
  in doc §4.8, with the human's repository list. That is the only CEL edit to
  make by hand. If you used `--no-coordination`, edit `github-issue-triage`
  instead, and also change its `label: body.label.name` param to
  `label: 'has(body.label) ? body.label.name : "ai-triage"'`: an `opened`
  delivery has no `body.label`, so the param would fail (422) and no run
  would start.
- If existing triggers have the same names (`github-pr-triage`,
  `github-pr-review`, …), diff the live spec against the render
  (`kubectl get eventtrigger <n> -n <ns> -o yaml`). Tell the human what will
  change before overwriting.

Check: `kubectl get eventsources,eventtriggers -n <ns>`. Every factory object
must be Ready=True. For any that isn't, read
`.status.conditions[?(@.type=="Ready")]` and fix it (doc §7 lists the
reasons).

## Phase 5: Labels and smoke test

1. With approval, create the labels in each target repository:
   `gh label create ai-triage -R <repo>; gh label create ai-review -R <repo>; gh label create ai-waiting -R <repo>`.
   Use `--force` to make it idempotent.
2. Ask the human to run the smoke test in doc §7 on a throwaway repository.
   The label must be applied by a human with `writer`, not by you through a
   bot token: bot-applied labels are dropped by design.
3. Watch while it runs:
   `kubectl logs -n <ns> deploy/<integration-gateway deployment> -f | grep -E 'event|dropped|ignored'`.
   Get the deployment name from `kubectl get deploy -n <ns>`.
   Match what you see against doc §8.

Done when: a test issue ends with a PR assigned to the human who labeled it.

## Changing an existing factory

Re-read doc §6 before any edit. In particular:

- Renaming a trigger means updating every `body.trigger in [...]` list and
  `counts[...]` key that names it.
- Changing a cap or condition in `factory-fix` means mirroring it in
  `factory-handoff`. The same goes for `factory-park`/`factory-triage-stacked`
  ↔ `factory-triage`.
- Before applying, render the old and new versions and diff them, and state
  for each run-event outcome which trigger now catches it. Every outcome must
  be caught by exactly one.

## Reporting back

Finish with a short summary for the human:

- what was installed, and where (namespace, or values file + commit);
- which options were chosen (agent, coordination, sweep, auto-triage repos);
- anything left for them to do (secrets, App settings, merging the GitOps PR,
  the smoke test);
- every check that didn't pass, with its evidence.

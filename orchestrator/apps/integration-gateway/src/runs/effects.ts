import { createHmac, randomUUID } from "node:crypto";
import type { Envelope, JsonValue, RenderedReply } from "@aktis/event-triggers";
import type { ClaudeTokenStore } from "../claude-auth/store.js";
import type { CompiledConflicts, EventSourceLookup, SecretLookup } from "../event-sources.js";
import type { GithubReplyClient } from "../github-client.js";
import type { IdentityResolver, RepoContext } from "../identity.js";
import {
  parseAcceptedTrigger,
  type AcceptedTrigger,
  type IdentityErrorBody,
  type OrchestratorClient,
  type OrchestratorInvokeResult,
  type PollHooks,
} from "../orchestrator-client.js";
import type { SenderClaims } from "../sender-assertion.js";
import type { SessionPageStore } from "../session-page-store.js";
import { startSpan, type TraceSpan } from "../telemetry.js";
import {
  actorLogin,
  chainOf,
  githubReplyTarget,
  positiveInt,
  refKey,
  REPOSITORY,
  type CompletedRun,
  type ConflictCandidate,
  type EmitRunEventResult,
  type EventRunInput,
  type GithubReplyTarget,
  type HeldMemo,
  type HoldPlan,
  type IssueRef,
  type QueueHold,
  type RunChain,
} from "./model.js";

export const EVENT_SIGNATURE_HEADER = "x-event-signature";
const HTTP_REPLY_TIMEOUT_MS = 10_000;
export const DEFAULT_RUN_EVENT_MAX_DEPTH = 12;

/**
 * Default for {@link RunEffectsOptions.resumeWaitMs}: how long a parked turn
 * waits for the user to finish linking their account before giving up (they
 * can always re-trigger later). Matches the link flow's own ~10-minute expiry.
 */
export const DEFAULT_RESUME_WAIT_MS = 10 * 60 * 1000;

/**
 * How a delivery is answered: a status (and optional JSON body) sent once the
 * relay's accept is known, plus any work that continues after the response
 * (an accepted run, an opt-in identity-error comment).
 */
export interface WebhookOutcome {
  status: number;
  body?: unknown;
  after?: () => Promise<void>;
  /** How the orchestrator answered the relay (absent when nothing was relayed). */
  relay?: "ignored" | "rejected" | "failed" | "identity-required" | "accepted";
}

/** Starts the workflows the gateway's runs live in (temporal.ts). */
export interface RunStarter {
  /** Starts one accepted event's run. Idempotent per invocation. */
  startEventRun(input: Omit<EventRunInput, "settings">): Promise<void>;
  /** The holds of every completion now held (docs/adr/0055 §2), for the cycle guard. */
  listHeld(): Promise<HeldMemo[]>;
}

export interface RunEffectsOptions {
  eventSources: EventSourceLookup;
  secrets?: SecretLookup;
  identityResolver: IdentityResolver;
  senderSubjects?: { findSubjectsByAccount?(provider: string, accountId: string): Promise<string[]> };
  orchestratorClient: OrchestratorClient;
  githubReplyClient: GithubReplyClient;
  replyFetch?: typeof fetch;
  sessionPageStore?: SessionPageStore;
  publicBaseUrl?: string;
  claudeAuthStore?: ClaudeTokenStore;
  resumeWaitMs?: number;
  runEventMaxDepth?: number;
  runs: RunStarter;
  log: (line: string) => void;
  report: (error: unknown) => void;
  now: () => Date;
}

/**
 * Every side effect of an event's run (docs/event-triggers.md §7), shared by
 * the HTTP server (the relay, session-page turns) and the run activities
 * (activities.ts). Best effort unless a method says otherwise: failures are
 * reported, never thrown.
 */
export class RunEffects {
  constructor(private readonly o: RunEffectsOptions) {}

  private async best(fn: () => Promise<unknown>): Promise<void> {
    try {
      await fn();
    } catch (error) {
      this.o.report(error);
    }
  }

  async readSecret(name: string, key: string, refresh = false): Promise<string | undefined> {
    if (!this.o.secrets) return undefined;
    try {
      return await this.o.secrets.get(name, key, refresh ? { refresh: true } : undefined);
    } catch (error) {
      this.o.report(error);
      return undefined;
    }
  }

  /** `reply.github`, validated; an unusable one is logged and treated as absent. */
  replyTarget(trigger: AcceptedTrigger): GithubReplyTarget | undefined {
    const { target, problem } = githubReplyTarget(trigger);
    if (problem) this.o.log(problem);
    return target;
  }

  /**
   * Sends the envelope to `/invoke` and handles the three §5 outcomes. Only
   * an accepted event has side effects -- its run, started as a workflow --
   * and only the ones its trigger's `reply` declares.
   */
  async relayEvent(envelope: Envelope, sender: SenderClaims | undefined, span: TraceSpan, holdsQueue?: QueueHold): Promise<WebhookOutcome> {
    const submitted = await this.o.orchestratorClient.submitEvent(envelope, sender, span.traceparent);
    this.o.log(`event ${envelope.source}/${envelope.id}: trace ${span.traceId}`);
    span.set({ "aktis.event.outcome": submitted.kind });
    switch (submitted.kind) {
      case "ignored":
        this.o.log(`event ${envelope.source}/${envelope.id}: no trigger matched (ignored)`);
        return { status: 202, relay: "ignored" };
      case "rejected":
        this.o.log(`event ${envelope.source}/${envelope.id}: trigger matched but failed for this event (422): ${submitted.error}`);
        return { status: 202, relay: "rejected" };
      case "failed":
        this.o.log(`event ${envelope.source}/${envelope.id}: relay failed: ${submitted.error}`);
        span.fail("relay_failed");
        return { status: 202, relay: "failed" };
      case "identity-required":
        this.o.log(`event ${envelope.source}/${envelope.id}: refused, ${submitted.body.error.code} for sender ${sender?.login ?? "?"}`);
        // docs/adr/0050 §4: a refused delivery is a short trace ending in an
        // error span that carries the code.
        span.set({ "aktis.identity_error": submitted.body.error.code });
        span.fail(submitted.body.error.code);
        return { status: 403, body: submitted.body, after: () => this.postIdentityError(submitted.body), relay: "identity-required" };
      case "accepted":
        span.set({ "aktis.trigger": submitted.trigger.name, "aktis.invocation_id": submitted.id });
        return {
          status: 202,
          after: () =>
            this.o.runs.startEventRun({
              invocationId: submitted.id,
              envelope,
              ...(sender ? { sender } : {}),
              trigger: submitted.trigger,
              ...(span.traceparent ? { traceparent: span.traceparent } : {}),
              ...(holdsQueue ? { holdsQueue } : {}),
            }),
          relay: "accepted",
        };
    }
  }

  /**
   * GitHub never reads a webhook's response body, so a trigger may opt in to
   * having the 403's printable message posted where the sender will see it
   * (`reply.github.onIdentityError`, docs/adr/0049).
   */
  async postIdentityError(body: IdentityErrorBody): Promise<void> {
    const trigger = (body.error as { trigger?: unknown }).trigger;
    if (typeof trigger !== "object" || trigger === null) return;
    const accepted = parseAcceptedTrigger(trigger);
    const gh = this.replyTarget(accepted);
    if (!gh || !accepted.reply?.github?.onIdentityError) return;
    await this.o.githubReplyClient.postIssueComment(gh.owner, gh.repo, gh.issueNumber, body.error.message);
  }

  /**
   * What the gateway vouches for about a verified sender (docs/adr/0049 §5):
   * their login, and -- when the source extracts an account id -- the one
   * subject that account is linked under, or why there isn't one. Never
   * throws: a lookup failure is "unavailable", which fails closed.
   */
  async senderClaims(sourceName: string, resolved: { login: string; accountId?: string }): Promise<SenderClaims> {
    const claims: SenderClaims = { login: resolved.login };
    if (!resolved.accountId) return claims;
    claims.accountId = resolved.accountId;
    const lookup = this.o.senderSubjects?.findSubjectsByAccount;
    if (!lookup) return { ...claims, subjectStatus: "unavailable" };
    try {
      const subjects = await lookup.call(this.o.senderSubjects, "github", resolved.accountId);
      if (subjects.length === 1) return { ...claims, subject: subjects[0]!, subjectStatus: "linked" };
      if (subjects.length > 1) {
        this.o.log(`webhook ${sourceName}: sender ${resolved.login} is linked under ${subjects.length} subjects (ambiguous)`);
        return { ...claims, subjectStatus: "ambiguous" };
      }
      return { ...claims, subjectStatus: "unlinked" };
    } catch (error) {
      this.o.report(error);
      return { ...claims, subjectStatus: "unavailable" };
    }
  }

  // --- the run's own steps ---------------------------------------------------

  /** The ack (+ session page link), status labels and assignees, at acceptance (docs/adr/0052 §3). */
  async acknowledge(input: Pick<EventRunInput, "trigger">): Promise<{ ackComment?: { id: number; body: string } }> {
    const { trigger } = input;
    const gh = this.replyTarget(trigger);
    if (!gh) return {};
    let pageUrl: string | undefined;
    if (gh.sessionPage && this.o.sessionPageStore && this.o.publicBaseUrl) {
      const store = this.o.sessionPageStore;
      await this.best(async () => {
        // The issue follow-ups reply to is stored on the entry; it is never
        // derived from the session id (an operator template).
        const entry = await store.getOrCreate(trigger.sessionId, { owner: gh.owner, repo: gh.repo, issueNumber: gh.issueNumber });
        pageUrl = `${this.o.publicBaseUrl!.replace(/\/+$/, "")}/sessions/${entry.token}`;
      });
    }
    let ackComment: { id: number; body: string } | undefined;
    const ack = [gh.ack, pageUrl ? `Follow along or send follow-ups: ${pageUrl}` : undefined].filter(Boolean).join("\n\n");
    if (ack) {
      await this.best(async () => {
        const id = await this.o.githubReplyClient.postIssueComment(gh.owner, gh.repo, gh.issueNumber, ack);
        if (id !== undefined) ackComment = { id, body: ack };
      });
    }
    // Bot-authored, so they never re-enter routing.
    const { addLabels, assignees } = gh;
    if (addLabels) await this.best(() => this.o.githubReplyClient.addIssueLabels(gh.owner, gh.repo, gh.issueNumber, addLabels));
    if (assignees) await this.best(() => this.o.githubReplyClient.addIssueAssignees(gh.owner, gh.repo, gh.issueNumber, assignees));
    return ackComment ? { ackComment } : {};
  }

  async trackTurn(sessionId: string, request: string): Promise<{ turnIndex: number } | undefined> {
    try {
      return await this.o.sessionPageStore?.addTurn(sessionId, request);
    } catch (error) {
      this.o.report(error);
      return undefined;
    }
  }

  async completeTurn(sessionId: string, tracked: { turnIndex: number } | undefined, outcome: OrchestratorInvokeResult): Promise<void> {
    if (!tracked || !this.o.sessionPageStore) return;
    await this.best(() =>
      this.o.sessionPageStore!.completeTurn(
        sessionId,
        tracked.turnIndex,
        outcome.status === "succeeded"
          ? { status: "succeeded", result: outcome.result ?? "" }
          : { status: "failed", error: outcome.error ?? "unknown error" },
      ),
    );
  }

  /**
   * Surfaces the Remote Control link (at most once) on the trigger's issue
   * when the delegated agent reports one: edited INTO the posted ack, keeping
   * one "starting work" comment, or -- with no ack to edit, or a failed edit --
   * posted as its own comment, since the link is the one thing a human needs
   * to take over.
   */
  remoteControlHooks(gh: GithubReplyTarget, ack: { id: number; body: string } | undefined, onPosted?: () => void): PollHooks {
    let posted = false;
    return {
      onRemoteControlUrl: async (url) => {
        if (posted) return;
        posted = true;
        onPosted?.();
        const line = `🤖 Watch live or take over the session here: ${url}`;
        if (ack) {
          try {
            await this.o.githubReplyClient.updateIssueComment(gh.owner, gh.repo, ack.id, `${ack.body}\n\n${line}`);
            return;
          } catch (error) {
            this.o.report(error);
          }
        }
        await this.best(() => this.o.githubReplyClient.postIssueComment(gh.owner, gh.repo, gh.issueNumber, line));
      },
    };
  }

  /** Polls a turn to its end. Never throws for a transport failure (the result says so). */
  pollTurn(invocationId: string, hooks: PollHooks): Promise<OrchestratorInvokeResult> {
    return this.o.orchestratorClient.poll(invocationId, hooks);
  }

  /** Posts `text` where the trigger replies on GitHub, if anywhere. */
  async comment(trigger: AcceptedTrigger, text: string): Promise<void> {
    const gh = this.replyTarget(trigger);
    if (gh) await this.best(() => this.o.githubReplyClient.postIssueComment(gh.owner, gh.repo, gh.issueNumber, text));
  }

  /**
   * Waits for a pending identity link to complete. Only providers whose token
   * store this gateway itself owns can be waited on -- `claude` (setup-token)
   * and `claude-remote` (full login, docs/adr/0027's follow-up). False for
   * anything else, or a link that never landed within the wait window.
   */
  async waitForLink(identityLink: { provider: string; subject: string }): Promise<boolean> {
    const kind = identityLink.provider === "claude" ? "setup-token" : identityLink.provider === "claude-remote" ? "login" : undefined;
    const store = kind ? this.o.claudeAuthStore : undefined;
    if (!store || !kind) return false;
    const record = await store.waitForCompletion(identityLink.subject, this.o.resumeWaitMs ?? DEFAULT_RESUME_WAIT_MS, kind);
    return Boolean(record);
  }

  /** Re-submits an event after a completed identity link; the resumed turn stays in the delivery's trace. */
  async resubmit(envelope: Envelope, sender: SenderClaims | undefined, traceparent: string | undefined): Promise<{ kind: "accepted"; id: string } | { kind: "failed"; error: string }> {
    const hop = startSpan("event resubmit", traceparent, { kind: "client", attributes: { "aktis.event_source": envelope.source } });
    try {
      const again = await this.o.orchestratorClient.submitEvent(envelope, sender, hop.traceparent);
      hop.set({ "aktis.event.outcome": again.kind });
      if (again.kind === "identity-required") hop.fail(again.body.error.code);
      else if (again.kind === "failed") hop.fail("relay_failed");
      if (again.kind === "accepted") return { kind: "accepted", id: again.id };
      const why = again.kind === "ignored" ? "no trigger matched" : again.kind === "identity-required" ? again.body.error.code : again.error;
      return { kind: "failed", error: why };
    } finally {
      hop.end();
    }
  }

  /** The result comment (`reply.github.result`) and the `reply.http` callback. */
  async reportResult(trigger: AcceptedTrigger, outcome: OrchestratorInvokeResult, sendHttp: boolean): Promise<void> {
    const gh = this.replyTarget(trigger);
    if (gh?.result) {
      const text = outcome.status === "succeeded" ? (outcome.result ?? "") : `Something went wrong processing this: ${outcome.error ?? "unknown error"}`;
      await this.best(() => this.o.githubReplyClient.postIssueComment(gh.owner, gh.repo, gh.issueNumber, text));
    }
    const http = trigger.reply?.http;
    if (http && sendHttp) await this.best(() => this.postHttpReply(http, trigger, outcome));
  }

  /** A run that failed before it could report: say so wherever the trigger reports. */
  async reportFailure(trigger: AcceptedTrigger, message: string, sendHttp: boolean): Promise<void> {
    const gh = this.replyTarget(trigger);
    if (gh?.result) {
      await this.best(() => this.o.githubReplyClient.postIssueComment(gh.owner, gh.repo, gh.issueNumber, `Something went wrong processing this: ${message}`));
    }
    const http = trigger.reply?.http;
    if (http && sendHttp) await this.best(() => this.postHttpReply(http, trigger, { status: "failed", error: message }));
  }

  /**
   * The identity-link wait ended without a link: no run started. Only the
   * HTTP callback hears about it (the issue already shows the link prompt).
   */
  async reportLinkAbandoned(trigger: AcceptedTrigger): Promise<void> {
    const http = trigger.reply?.http;
    if (http) await this.best(() => this.postHttpReply(http, trigger, { status: "failed", error: "identity link was not completed; the run did not start" }));
  }

  /** Removes `reply.github.removeLabel`. Throws on failure (the caller retries). */
  async removeLabel(trigger: AcceptedTrigger): Promise<void> {
    const gh = this.replyTarget(trigger);
    if (!gh?.removeLabel) return;
    await this.o.githubReplyClient.removeIssueLabel(gh.owner, gh.repo, gh.issueNumber, gh.removeLabel);
  }

  /**
   * `reply.http`: POST the outcome to the trigger's LITERAL url (it comes only
   * from the EventTrigger CR -- never from event content). With `secretRef`,
   * `x-event-signature: sha256=<hex hmac(secret, body)>` over the exact bytes
   * sent; a signing secret that can't be read means the reply is NOT sent.
   * Redirects are not followed.
   */
  async postHttpReply(
    http: NonNullable<RenderedReply["http"]>,
    trigger: AcceptedTrigger,
    outcome: Pick<OrchestratorInvokeResult, "status" | "result" | "error">,
  ): Promise<void> {
    let url: URL;
    try {
      url = new URL(http.url);
    } catch {
      this.o.log(`trigger ${trigger.name}: reply.http not sent (invalid url)`);
      return;
    }
    if (url.protocol !== "http:" && url.protocol !== "https:") {
      this.o.log(`trigger ${trigger.name}: reply.http not sent (unsupported scheme ${url.protocol})`);
      return;
    }
    const succeeded = outcome.status === "succeeded";
    const body = JSON.stringify({
      trigger: trigger.name,
      sessionId: trigger.sessionId,
      status: succeeded ? "succeeded" : "failed",
      ...(succeeded ? { result: outcome.result ?? "" } : { error: outcome.error ?? "unknown error" }),
    });
    const headers: Record<string, string> = { "content-type": "application/json" };
    if (http.secretRef) {
      const secret = await this.readSecret(http.secretRef.name, http.secretRef.key);
      if (!secret) {
        this.o.log(`trigger ${trigger.name}: reply.http not sent (signing secret ${http.secretRef.name}/${http.secretRef.key} unavailable)`);
        return;
      }
      headers[EVENT_SIGNATURE_HEADER] = `sha256=${createHmac("sha256", secret).update(body, "utf8").digest("hex")}`;
    }
    const doFetch = this.o.replyFetch ?? fetch;
    const res = await doFetch(url, { method: "POST", headers, body, redirect: "manual", signal: AbortSignal.timeout(HTTP_REPLY_TIMEOUT_MS) });
    if (!res.ok) this.o.log(`trigger ${trigger.name}: reply.http answered ${res.status}`);
  }

  // --- completions (docs/adr/0052 §1, docs/adr/0055 §2) ---------------------

  /**
   * Which holds a completion really waits on. A hold whose params are unusable
   * is dropped with a log line -- a broken hold degrades to publishing now,
   * never to a stalled chain. Two guards on `waitUntilClosed`, both of which
   * also publish at once: waiting on the run's own issue (it could never
   * close first), and a cycle (the issue waited on is itself held waiting,
   * directly or through others, on this run's issue).
   */
  async planHold(completed: CompletedRun): Promise<HoldPlan> {
    const { trigger } = completed;
    const oc = trigger.onComplete!;
    const params = trigger.params ?? {};
    const tag = `run event ${trigger.name || "trigger"} -> ${oc.emitTo}`;
    const gh = this.replyTarget(trigger);
    const parkedOn: IssueRef | undefined = gh ? { owner: gh.owner, repo: gh.repo, number: gh.issueNumber } : undefined;

    let waitFor: IssueRef | undefined;
    if (oc.waitUntilClosed) {
      const repoMatch = REPOSITORY.exec(params[oc.waitUntilClosed.repositoryParam] ?? "");
      const number = positiveInt(params[oc.waitUntilClosed.numberParam]);
      const candidate = repoMatch && number ? { owner: repoMatch[1] as string, repo: repoMatch[2] as string, number } : undefined;
      if (!candidate) {
        this.o.log(`${tag}: not waiting (waitUntilClosed params ${oc.waitUntilClosed.repositoryParam}/${oc.waitUntilClosed.numberParam} are not a repository and number)`);
      } else if (parkedOn && refKey(candidate) === refKey(parkedOn)) {
        this.o.log(`${tag}: not waiting (it would wait on its own issue ${refKey(candidate)})`);
      } else if (parkedOn && (await this.waitsOn(candidate, parkedOn))) {
        this.o.log(`${tag}: not waiting (${refKey(candidate)} is already waiting on ${refKey(parkedOn)}; that would be a cycle)`);
      } else {
        let state: "open" | "closed" | "missing" = "open";
        try {
          state = await this.o.githubReplyClient.getIssueState(candidate.owner, candidate.repo, candidate.number);
        } catch (error) {
          // Unknown: hold it, and let the checks find out.
          this.o.report(error);
        }
        if (state === "open") waitFor = candidate;
        else this.o.log(`${tag}: not waiting (${refKey(candidate)} is already ${state})`);
      }
    }

    let queue: string | undefined;
    if (oc.serialize) {
      const key = params[oc.serialize.keyParam];
      if (key) queue = `${oc.serialize.group}:${key}`;
      else this.o.log(`${tag}: not serializing (param ${oc.serialize.keyParam} is empty)`);
    }

    if (waitFor || queue) {
      this.o.log(`${tag}: held${waitFor ? ` until ${refKey(waitFor)} is closed` : ""}${waitFor && queue ? " and" : ""}${queue ? ` until queue ${queue} is free` : ""}`);
    }
    return {
      ...(waitFor ? { waitFor, blocked: true } : {}),
      ...(waitFor && parkedOn ? { parkedOn } : {}),
      ...(queue ? { queue } : {}),
    };
  }

  /** Whether `from` is held waiting -- directly or through other held events -- on `target`. */
  private async waitsOn(from: IssueRef, target: IssueRef): Promise<boolean> {
    const next = new Map<string, string>();
    for (const held of await this.o.runs.listHeld()) {
      if (held.parkedOn && held.waitFor) next.set(held.parkedOn, held.waitFor);
    }
    const goal = refKey(target);
    const seen = new Set<string>();
    let cur: string | undefined = refKey(from);
    while (cur) {
      if (cur === goal) return true;
      if (seen.has(cur)) return false;
      seen.add(cur);
      cur = next.get(cur);
    }
    return false;
  }

  /** A held event's issues, re-read from GitHub. An unreadable state keeps it waiting. */
  async holdState(waitFor: IssueRef, parkedOn: IssueRef | undefined): Promise<"waiting" | "released" | "abandoned"> {
    const gh = this.o.githubReplyClient;
    try {
      if (parkedOn && (await gh.getIssueState(parkedOn.owner, parkedOn.repo, parkedOn.number)) !== "open") return "abandoned";
      return (await gh.getIssueState(waitFor.owner, waitFor.repo, waitFor.number)) === "open" ? "waiting" : "released";
    } catch (error) {
      this.o.report(error);
      return "waiting";
    }
  }

  /** Best-effort notice on the held run's issue that its follow-up will never be published. */
  async commentChainDropped(completed: CompletedRun, parkedOn: IssueRef | undefined): Promise<void> {
    const gh = this.replyTarget(completed.trigger);
    const target = gh ? { owner: gh.owner, repo: gh.repo, number: gh.issueNumber } : parkedOn;
    if (!target) return;
    await this.best(() =>
      this.o.githubReplyClient.postIssueComment(
        target.owner,
        target.repo,
        target.number,
        "Stopped the automated chain here: its next step could not be delivered after retrying for 24 hours. This needs a human to pick it up.",
      ),
    );
  }

  /**
   * Publishes one run's completion to its trigger's `onComplete.emitTo` run
   * source (docs/adr/0052 §1) through the same relay a webhook takes. Three
   * deterministic gates, each of which stops the chain here (logged; the
   * first two also commented where the run replied):
   *
   *  - the global depth cap (`runEventMaxDepth`), which no trigger can raise;
   *  - the origin must re-verify NOW: a human whose access was revoked
   *    mid-chain stops it, and every hop still acts as a verified person;
   *  - the target must be a run source, and the chain must have a human
   *    origin at all (a run event never carries a bot or a made-up sender).
   */
  async emitRunEvent(completed: CompletedRun, opts: { resetDepth?: boolean; holdsQueue?: QueueHold } = {}): Promise<EmitRunEventResult> {
    const { envelope, sender, trigger, finished, traceparent } = completed;
    const emitTo = trigger.onComplete!.emitTo;
    const tag = `run event ${trigger.name || "trigger"} -> ${emitTo}`;
    const target = this.o.eventSources.get(emitTo);
    if (!target || target.kind !== "run") {
      this.o.log(`${tag}: skipped (${target ? `source is kind ${target.kind}, not run` : "no such usable EventSource"})`);
      return "not-accepted";
    }
    const chain = chainOf(envelope, sender);
    if (!chain) {
      this.o.log(`${tag}: skipped (the run's chain metadata is unreadable)`);
      return "not-accepted";
    }
    const origin = chain.origin;
    if (!origin) {
      this.o.log(`${tag}: skipped (the chain has no verified human sender to act as)`);
      return "not-accepted";
    }

    const next: RunChain = {
      id: chain.id,
      // A hold that waited on a human merging something is not a runaway
      // loop: the depth restarts there. Counts are kept, so per-trigger caps
      // still bound the chain.
      depth: opts.resetDepth ? 1 : chain.depth + 1,
      counts: { ...chain.counts, [trigger.name]: (chain.counts[trigger.name] ?? 0) + 1 },
      origin,
    };
    const maxDepth = this.o.runEventMaxDepth ?? DEFAULT_RUN_EVENT_MAX_DEPTH;
    if (next.depth > maxDepth) {
      this.o.log(`${tag}: dropped, chain ${chain.id} would reach depth ${next.depth} (max ${maxDepth})`);
      await this.comment(trigger, `Stopped the automated chain here: it exceeded ${maxDepth} steps.`);
      return "not-accepted";
    }

    const repoMatch = REPOSITORY.exec(origin.repository);
    const repoContext: RepoContext | undefined = repoMatch ? { owner: repoMatch[1] as string, repo: repoMatch[2] as string } : undefined;
    let identity: unknown;
    try {
      identity = await this.o.identityResolver.resolve(origin.login, false, repoContext);
    } catch (error) {
      // A GitHub or identity-link blip is not "no longer verifies": a
      // transport failure, so the publish is retried.
      this.o.report(error);
      this.o.log(`${tag}: chain origin ${origin.login} could not be re-verified right now (resolver error)`);
      return "failed";
    }
    if (!identity) {
      this.o.log(`${tag}: dropped, chain origin ${origin.login} no longer verifies`);
      await this.comment(trigger, `Stopped the automated chain here: ${origin.login} could not be re-verified for the next step.`);
      return "not-accepted";
    }
    const claims = await this.senderClaims(emitTo, { login: origin.login, ...(origin.accountId ? { accountId: origin.accountId } : {}) });

    const event: Envelope = {
      source: emitTo,
      kind: "run",
      id: randomUUID(),
      time: this.o.now().toISOString(),
      headers: {},
      sender: { login: origin.login, isBot: false },
      repository: origin.repository,
      body: {
        trigger: trigger.name,
        status: finished.status,
        params: trigger.params ?? {},
        outcome: (finished.outcome ?? {}) as JsonValue,
        chain: next as unknown as JsonValue,
      },
    };
    // Its own span, in the finished run's trace, so a chain reads as one trace.
    const span = startSpan("run event", traceparent, {
      kind: "client",
      attributes: {
        "aktis.event_source": emitTo,
        "aktis.event_id": event.id,
        "aktis.completed_trigger": trigger.name,
        "aktis.chain_depth": next.depth,
      },
    });
    let relayed: WebhookOutcome;
    try {
      relayed = await this.relayEvent(event, claims, span, opts.holdsQueue);
    } catch (error) {
      span.fail("internal_error");
      throw error;
    } finally {
      span.end();
    }
    if (relayed.relay === "accepted") {
      // The next hop's run is its own workflow. A start that fails even
      // after the starter's own retries is reported, not "failed": the
      // orchestrator already accepted the event, so publishing it again
      // would start a second run.
      await relayed.after?.().catch(this.o.report);
      return "accepted";
    }
    relayed.after?.().catch(this.o.report);
    return relayed.relay === "failed" ? "failed" : "not-accepted";
  }

  // --- conflicts (docs/adr/0055 §1) -----------------------------------------

  /** A `conflicts` source's current spec, or undefined once it is gone. */
  conflictsSource(name: string): CompiledConflicts | undefined {
    const source = this.o.eventSources.get(name);
    return source?.kind === "conflicts" ? source.conflicts : undefined;
  }

  /**
   * The open pull requests `author` opened that now conflict with their
   * base. Deterministic on purpose: which PRs conflict is GitHub's
   * `mergeable_state`. A PR is skipped when it is a draft, carries one of
   * `skipLabels` (a run is already on it), GitHub hasn't computed
   * mergeability yet, or it has no assignee who still verifies. Only an
   * assignment the App itself made counts: anyone with triage access can set
   * assignees, and must not be able to pick whose credentials a write run
   * uses.
   */
  async findConflicts(name: string, conflicts: CompiledConflicts): Promise<ConflictCandidate[]> {
    let prs: Array<{ owner: string; repo: string; number: number }>;
    try {
      prs = await this.o.githubReplyClient.searchOpenPullRequests(conflicts.author);
    } catch (error) {
      this.o.report(error);
      return [];
    }
    const found: ConflictCandidate[] = [];
    for (const ref of prs) {
      try {
        const c = await this.considerConflict(name, conflicts, ref.owner, ref.repo, ref.number);
        if (c) found.push(c);
      } catch (error) {
        this.o.report(error);
      }
    }
    return found;
  }

  private async considerConflict(name: string, conflicts: CompiledConflicts, owner: string, repo: string, number: number): Promise<ConflictCandidate | undefined> {
    const tag = `conflicts ${name}: ${owner}/${repo}#${number}`;
    const gh = this.o.githubReplyClient;
    const pr = await gh.getPullRequest(owner, repo, number);
    if (!pr || pr.state !== "open" || pr.draft) return undefined;
    if (pr.mergeable !== false || pr.mergeableState !== "dirty") return undefined;
    const busy = pr.labels.find((l) => conflicts.skipLabels.includes(l));
    if (busy) {
      this.o.log(`${tag}: conflicts, but skipped (label ${busy} -- a run is already on it)`);
      return undefined;
    }
    const app = actorLogin(conflicts.author).toLowerCase();
    const actors = pr.assignees.length > 0 ? await gh.getAssignmentActors(owner, repo, number) : new Map<string, string>();
    for (const a of pr.assignees) {
      const by = actors.get(a.login.toLowerCase());
      if (by?.toLowerCase() !== app) {
        this.o.log(`${tag}: not running as assignee ${a.login} (assigned by ${by ?? "unknown"}, not ${actorLogin(conflicts.author)})`);
        continue;
      }
      let verified = false;
      try {
        verified = Boolean(await this.o.identityResolver.resolve(a.login, false, { owner, repo }));
      } catch (error) {
        this.o.report(error);
      }
      if (verified) {
        return {
          owner,
          repo,
          pr,
          assignee: { login: a.login, ...(a.id !== undefined ? { accountId: String(a.id) } : {}) },
          markKey: `${name}:${owner}/${repo}#${number}@${pr.baseSha}`,
        };
      }
    }
    this.o.log(`${tag}: conflicts, but skipped (no App-assigned assignee who verifies to run the resync as)`);
    return undefined;
  }

  /**
   * One `conflict` event for a pull request the sweep found conflicting,
   * acting as its verified assignee. A fresh chain: the PR's earlier chain
   * ended when it was handed off. True unless the orchestrator could not be
   * reached (the sweep then retries).
   */
  async emitConflictEvent(source: string, c: ConflictCandidate): Promise<boolean> {
    this.o.log(`conflicts ${source}: ${c.owner}/${c.repo}#${c.pr.number} conflicts with ${c.pr.baseRef}@${c.pr.baseSha.slice(0, 7)}; emitting a conflict event as ${c.assignee.login}`);
    const claims = await this.senderClaims(source, c.assignee);
    const envelope: Envelope = {
      source,
      kind: "conflict",
      id: randomUUID(),
      time: this.o.now().toISOString(),
      headers: {},
      sender: { login: c.assignee.login, isBot: false },
      repository: `${c.owner}/${c.repo}`,
      body: {
        pullRequest: {
          number: c.pr.number,
          url: c.pr.url,
          title: c.pr.title,
          baseRef: c.pr.baseRef,
          baseSha: c.pr.baseSha,
          headRef: c.pr.headRef,
          headSha: c.pr.headSha,
        },
      },
    };
    const span = startSpan("conflict event", undefined, { kind: "client", attributes: { "aktis.event_source": source, "aktis.event_id": envelope.id } });
    let relayed: WebhookOutcome;
    try {
      relayed = await this.relayEvent(envelope, claims, span);
    } catch (error) {
      span.fail("internal_error");
      this.o.report(error);
      return false;
    } finally {
      span.end();
    }
    relayed.after?.().catch(this.o.report);
    return relayed.relay !== "failed";
  }
}

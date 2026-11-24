// The example EventTriggers (examples/event-triggers) compile, and the
// software factory's routing (docs/adr/0052, docs/adr/0055) sends every run
// completion to exactly the step it should. The production copies in
// orchestrator/charts/aktis/values-production.yaml mirror these.

import { readdirSync, readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { parse } from "yaml";
import { compileTrigger, selectTrigger } from "./trigger.js";
import type { Envelope, EventTriggerResource } from "./types.js";
import type { JsonValue } from "./values.js";

const dir = new URL("../../../../examples/event-triggers/", import.meta.url);
const triggers = readdirSync(dir)
  .filter((f) => f.startsWith("eventtrigger-") && f.endsWith(".yaml"))
  .map((f) => compileTrigger(parse(readFileSync(new URL(f, dir), "utf8")) as EventTriggerResource));

function route(envelope: Envelope): { trigger: string; params: Record<string, string> } | string {
  const r = selectTrigger(triggers, envelope, { sessionId: "s" });
  return r.status === "matched" ? { trigger: r.trigger, params: r.stringParams } : r.status;
}
const routed = (envelope: Envelope): string => {
  const r = route(envelope);
  return typeof r === "string" ? r : r.trigger;
};

function runEvent(
  trigger: string,
  outcome: Record<string, JsonValue>,
  opts: { status?: string; counts?: Record<string, number>; params?: Record<string, string> } = {},
): Envelope {
  return {
    source: "runs",
    kind: "run",
    id: "e",
    time: "2026-11-23T00:00:00Z",
    headers: {},
    sender: { login: "alice", isBot: false },
    repository: "acme/widgets",
    body: {
      trigger,
      status: opts.status ?? "succeeded",
      params: opts.params ?? { repository: "acme/widgets", number: "7", title: "Add widgets" },
      outcome,
      chain: { id: "c", depth: 1, counts: opts.counts ?? {} },
    },
  };
}

describe("example EventTriggers", () => {
  it("all compile", () => {
    expect(triggers.length).toBeGreaterThan(10);
    for (const t of triggers) expect(t.error ?? "", t.name).toBe("");
  });
});

describe("factory coordination routing (docs/adr/0055)", () => {
  it("ai-triage on an issue goes to factory-intake, not straight to triage", () => {
    const labeled: Envelope = {
      source: "github",
      kind: "webhook",
      id: "d",
      time: "t",
      headers: { "x-github-event": "issues" },
      sender: { login: "alice", isBot: false },
      repository: "acme/widgets",
      body: { action: "labeled", label: { name: "ai-triage" }, issue: { number: 7, title: "Add widgets", body: "" }, repository: { full_name: "acme/widgets" } },
    };
    expect(route(labeled)).toEqual({ trigger: "factory-intake", params: { repository: "acme/widgets", number: "7", title: "Add widgets" } });
  });

  it.each(["factory-intake", "factory-park"])("a %s completion is coordinated", (from) => {
    expect(routed(runEvent(from, {}))).toBe("factory-coordinate");
  });

  it.each([
    ["proceed", { coordination: { decision: "proceed" } }, {}, "succeeded"],
    ["no decision", { coordination: { decision: "none" } }, {}, "succeeded"],
    ["no outcome at all", {}, {}, "succeeded"],
    ["a failed coordinator", { coordination: { decision: "wait", number: 5 } }, {}, "failed"],
    ["a wait on itself", { coordination: { decision: "wait", number: 7 } }, {}, "succeeded"],
    ["a wait past 5 waits", { coordination: { decision: "wait", number: 5 } }, { "factory-park": 5 }, "succeeded"],
    ["a stack on itself", { coordination: { decision: "stack", number: 7 } }, {}, "succeeded"],
  ])("%s -> factory-triage", (_, outcome, counts, status) => {
    expect(routed(runEvent("factory-coordinate", outcome as Record<string, JsonValue>, { counts, status }))).toBe("factory-triage");
  });

  it("wait <N> -> factory-park, carrying N as waitFor", () => {
    const r = route(runEvent("factory-coordinate", { coordination: { decision: "wait", number: 5 } }, { counts: { "factory-park": 4 } }));
    expect(r).toMatchObject({ trigger: "factory-park", params: { waitFor: "5", number: "7", repository: "acme/widgets" } });
  });

  it("stack <N> -> factory-triage-stacked, carrying N as stackOn", () => {
    const r = route(runEvent("factory-coordinate", { coordination: { decision: "stack", number: 12 } }));
    expect(r).toMatchObject({ trigger: "factory-triage-stacked", params: { stackOn: "12", closesIssue: "7" } });
  });

  it.each(["factory-triage", "factory-triage-stacked"])("a %s run that opened a PR is reviewed", (from) => {
    const r = route(runEvent(from, { pullRequest: { number: 12, url: "u" }, madeNewCommits: true }));
    expect(r).toMatchObject({ trigger: "factory-review", params: { number: "12" } });
  });
});

describe("conflict resync routing (docs/adr/0055 §1)", () => {
  const conflict: Envelope = {
    source: "pr-conflicts",
    kind: "conflict",
    id: "e",
    time: "t",
    headers: {},
    sender: { login: "alice", isBot: false },
    repository: "acme/widgets",
    body: { pullRequest: { number: 12, url: "u", title: "Add widgets", baseRef: "main", baseSha: "b", headRef: "f", headSha: "h" } },
  };

  it("a conflict event goes to factory-resync on that PR", () => {
    expect(route(conflict)).toEqual({
      trigger: "factory-resync",
      params: { repository: "acme/widgets", number: "12", title: "Add widgets", baseRef: "main" },
    });
  });

  const resynced = (outcome: Record<string, JsonValue>, status = "succeeded") =>
    runEvent("factory-resync", outcome, { status, params: { repository: "acme/widgets", number: "12", title: "T", baseRef: "main" } });

  it("a resync that pushed is re-reviewed", () => {
    expect(route(resynced({ madeNewCommits: true }))).toMatchObject({ trigger: "factory-review", params: { number: "12" } });
  });

  it.each([
    ["pushed nothing", { madeNewCommits: false }, "succeeded", "pushed no changes"],
    ["failed", {}, "failed", "did not succeed"],
  ])("a resync that %s is handed off", (_, outcome, status, why) => {
    const r = route(resynced(outcome as Record<string, JsonValue>, status));
    expect(r).toMatchObject({ trigger: "factory-handoff", params: { number: "12", assignee: "alice" } });
    expect(typeof r !== "string" && r.params.reason).toContain(why);
  });
});

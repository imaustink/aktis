import { describe, expect, it } from "vitest";
import { buildCronEnvelope, buildWebhookEnvelope, normalizeHeaders, parseBody, type Envelope } from "./index.js";

const NOW = new Date("2026-10-13T12:00:00.123Z");



describe("normalizeHeaders", () => {
  it("lower-cases, joins arrays and case-variant duplicates, drops credentials", () => {
    expect(
      normalizeHeaders(
        {
          "X-GitHub-Event": "issues",
          "Set-Thing": ["a", "b"],
          "x-dup": "1",
          "X-Dup": "2",
          Authorization: "Bearer s3cret",
          Cookie: "session=1",
          "X-Hub-Signature-256": "sha256=abc",
          "content-length": 12,
          "x-undefined": undefined,
        },
        ["x-hub-signature-256"],
      ),
    ).toEqual({ "x-github-event": "issues", "set-thing": "a, b", "x-dup": "1, 2", "content-length": "12" });
  });

  it("matches dropNames case-insensitively and always drops authorization/cookie", () => {
    const out = normalizeHeaders({ authorization: "x", cookie: "y", "X-Sig": "z", keep: "k" }, ["X-SIG"]);
    expect(out).toEqual({ keep: "k" });
  });

  it("treats a __proto__ header as data", () => {
    const out = normalizeHeaders({ ["__proto__"]: "x" } as Record<string, string>);
    expect(Object.getPrototypeOf(out)).toBe(Object.prototype);
  });
});

describe("parseBody", () => {
  it("parses application/json (with parameters)", () => {
    expect(parseBody(Buffer.from('{"a":1,"b":[true]}'), "Application/JSON; charset=utf-8")).toEqual({ a: 1, b: [true] });
  });
  it("keeps non-object JSON as-is", () => {
    expect(parseBody(Buffer.from("[1,2]"), "application/json")).toEqual([1, 2]);
  });
  it("falls back to text for malformed JSON", () => {
    expect(parseBody(Buffer.from("{nope"), "application/json")).toEqual({ text: "{nope" });
  });
  it("parses form bodies into string fields, first value wins", () => {
    expect(
      parseBody(Buffer.from("command=%2Ftriage&text=acme%2Fwidgets+7&text=second"), "application/x-www-form-urlencoded"),
    ).toEqual({ command: "/triage", text: "acme/widgets 7" });
  });
  it("wraps anything else as text", () => {
    expect(parseBody(Buffer.from("héllo"), "text/plain")).toEqual({ text: "héllo" });
    expect(parseBody(Buffer.from("{\"a\":1}"))).toEqual({ text: "{\"a\":1}" });
  });
  it("respects Uint8Array views", () => {
    const backing = Buffer.from("xx{\"a\":2}yy");
    expect(parseBody(backing.subarray(2, 9), "application/json")).toEqual({ a: 2 });
  });
});

describe("build*Envelope", () => {
  it("builds a webhook envelope", () => {
    expect(
      buildWebhookEnvelope({
        source: "github",
        id: "d-1",
        headers: { "x-github-event": "issues" },
        body: { a: 1 },
        sender: { login: "octocat", isBot: false },
        repository: "acme/widgets",
        now: NOW,
      }),
    ).toEqual({
      source: "github",
      kind: "webhook",
      id: "d-1",
      time: "2026-10-13T12:00:00Z",
      headers: { "x-github-event": "issues" },
      body: { a: 1 },
      sender: { login: "octocat", isBot: false },
      repository: "acme/widgets",
    });
  });
  it("defaults id to a uuid, sender to {} and repository to \"\"", () => {
    const e = buildWebhookEnvelope({ source: "s", headers: {}, body: {}, now: NOW });
    expect(e.id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
    expect(e.sender).toEqual({});
    expect(e.repository).toBe("");
  });
  it("builds a cron envelope", () => {
    expect(buildCronEnvelope({ source: "nightly-sweep", payload: { repo: "acme/widgets" }, now: NOW })).toEqual({
      source: "nightly-sweep",
      kind: "cron",
      id: expect.stringMatching(/^nightly-sweep-1791892800-[0-9a-f]{8}$/),
      time: "2026-10-13T12:00:00Z",
      headers: {},
      body: { repo: "acme/widgets" },
      sender: {},
      repository: "",
    });
    expect(buildCronEnvelope({ source: "s", now: NOW }).body).toEqual({});
  });
  it("gives two ticks in the same second different ids", () => {
    const a = buildCronEnvelope({ source: "s", now: NOW });
    const b = buildCronEnvelope({ source: "s", now: NOW });
    expect(a.id).not.toBe(b.id);
  });
});

import { describe, expect, it, vi } from "vitest";
import { GithubReplyClient, REPLY_MARKER } from "./github-client.js";

const baseConfig = {
  githubToken: "pat_123",
  githubAppId: "",
  githubAppPrivateKey: "",
  githubAppInstallationId: "",
  githubApiUrl: "https://api.github.com",
};

describe("GithubReplyClient.postIssueComment", () => {

  it("posts a marker-prefixed comment using the resolved token", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({ ok: true });
    const client = new GithubReplyClient({ ...baseConfig, fetchImpl });

    await client.postIssueComment("acme", "widgets", 42, "What branch should this target?");

    expect(fetchImpl).toHaveBeenCalledWith(
      "https://api.github.com/repos/acme/widgets/issues/42/comments",
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({ authorization: "Bearer pat_123" }),
        body: JSON.stringify({ body: `${REPLY_MARKER}\nWhat branch should this target?` }),
      }),
    );
  });

  it("throws with response detail on a non-2xx response", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({ ok: false, status: 403, text: async () => "forbidden" });
    const client = new GithubReplyClient({ ...baseConfig, fetchImpl });

    await expect(client.postIssueComment("acme", "widgets", 42, "hi")).rejects.toThrow(/403.*forbidden/s);
  });

  it("returns the new comment's id, or undefined when the response has none", async () => {
    const withId = new GithubReplyClient({ ...baseConfig, fetchImpl: vi.fn().mockResolvedValue({ ok: true, json: async () => ({ id: 987 }) }) });
    expect(await withId.postIssueComment("acme", "widgets", 42, "hi")).toBe(987);
    const noBody = new GithubReplyClient({ ...baseConfig, fetchImpl: vi.fn().mockResolvedValue({ ok: true }) });
    expect(await noBody.postIssueComment("acme", "widgets", 42, "hi")).toBeUndefined();
  });
});

describe("GithubReplyClient.updateIssueComment", () => {
  it("PATCHes the comment with a marker-prefixed body", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({ ok: true });
    const client = new GithubReplyClient({ ...baseConfig, fetchImpl });

    await client.updateIssueComment("acme", "widgets", 987, "Starting.\n\nWatch live: x");

    expect(fetchImpl).toHaveBeenCalledWith(
      "https://api.github.com/repos/acme/widgets/issues/comments/987",
      expect.objectContaining({
        method: "PATCH",
        headers: expect.objectContaining({ authorization: "Bearer pat_123" }),
        body: JSON.stringify({ body: `${REPLY_MARKER}\nStarting.\n\nWatch live: x` }),
      }),
    );
  });

  it("throws with response detail on a non-2xx response", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({ ok: false, status: 404, text: async () => "Not Found" });
    const client = new GithubReplyClient({ ...baseConfig, fetchImpl });

    await expect(client.updateIssueComment("acme", "widgets", 987, "hi")).rejects.toThrow(/987.*404.*Not Found/s);
  });
});

describe("GithubReplyClient.removeIssueLabel", () => {
  it("DELETEs the single label, URL-encoding its name", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({ ok: true });
    const client = new GithubReplyClient({ ...baseConfig, fetchImpl });

    await client.removeIssueLabel("acme", "widgets", 42, "ai triage/pr");

    expect(fetchImpl).toHaveBeenCalledWith(
      "https://api.github.com/repos/acme/widgets/issues/42/labels/ai%20triage%2Fpr",
      expect.objectContaining({
        method: "DELETE",
        headers: expect.objectContaining({ authorization: "Bearer pat_123" }),
      }),
    );
  });

  // The label already being gone is the outcome this call wants, so a 404 is
  // success -- a human removing it mid-run must not fail the turn.
  it("treats a 404 as success", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({ ok: false, status: 404, text: async () => "not found" });
    const client = new GithubReplyClient({ ...baseConfig, fetchImpl });

    await expect(client.removeIssueLabel("acme", "widgets", 42, "ai-triage")).resolves.toBeUndefined();
  });

  it("throws with response detail on any other non-2xx response, after exhausting its retries", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({ ok: false, status: 403, text: async () => "forbidden" });
    const sleep = vi.fn().mockResolvedValue(undefined);
    const client = new GithubReplyClient({ ...baseConfig, fetchImpl, sleep });

    await expect(client.removeIssueLabel("acme", "widgets", 42, "ai-triage")).rejects.toThrow(/403.*forbidden/s);
    // Three attempts (the default), not one -- the headline reliability fix.
    expect(fetchImpl).toHaveBeenCalledTimes(3);
  });

  // The core determinism fix: a transient non-404 failure must not strand the
  // label. The DELETE is retried with backoff and succeeds on a later attempt.
  it("retries a transient non-404 failure and succeeds, so the label is not stranded", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce({ ok: false, status: 502, text: async () => "bad gateway" })
      .mockResolvedValueOnce({ ok: false, status: 500, text: async () => "server error" })
      .mockResolvedValueOnce({ ok: true });
    const sleep = vi.fn().mockResolvedValue(undefined);
    const client = new GithubReplyClient({ ...baseConfig, fetchImpl, sleep });

    await expect(client.removeIssueLabel("acme", "widgets", 42, "ai-triage")).resolves.toBeUndefined();
    expect(fetchImpl).toHaveBeenCalledTimes(3);
    // Backoff was applied between attempts (twice, not after the final success).
    expect(sleep).toHaveBeenCalledTimes(2);
  });

  // A rejected fetch (connection reset mid-rollout) is retryable too, not just
  // a non-ok response.
  it("retries a rejected fetch (dropped connection) and succeeds", async () => {
    const fetchImpl = vi
      .fn()
      .mockRejectedValueOnce(new Error("UND_ERR_SOCKET"))
      .mockResolvedValueOnce({ ok: true });
    const sleep = vi.fn().mockResolvedValue(undefined);
    const client = new GithubReplyClient({ ...baseConfig, fetchImpl, sleep });

    await expect(client.removeIssueLabel("acme", "widgets", 42, "ai-triage")).resolves.toBeUndefined();
    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });

  // A 404 is success on the FIRST try and must never be retried -- the label is
  // already gone, so retrying would waste calls for no reason.
  it("does not retry a 404 (already-gone label is immediate success)", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({ ok: false, status: 404, text: async () => "not found" });
    const sleep = vi.fn().mockResolvedValue(undefined);
    const client = new GithubReplyClient({ ...baseConfig, fetchImpl, sleep });

    await expect(client.removeIssueLabel("acme", "widgets", 42, "ai-triage")).resolves.toBeUndefined();
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(sleep).not.toHaveBeenCalled();
  });
});

describe("GithubReplyClient.addIssueLabels / addIssueAssignees (docs/adr/0052 §3)", () => {
  it.each([
    ["addIssueLabels", "labels", ["ai-review", "factory"]],
    ["addIssueAssignees", "assignees", ["alice"]],
  ] as const)("%s POSTs { %s } to the issue's endpoint", async (method, field, values) => {
    const fetchImpl = vi.fn().mockResolvedValue({ ok: true });
    const client = new GithubReplyClient({ ...baseConfig, fetchImpl });

    await client[method]("acme", "widgets", 12, [...values]);

    expect(fetchImpl).toHaveBeenCalledWith(
      `https://api.github.com/repos/acme/widgets/issues/12/${field}`,
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({ authorization: "Bearer pat_123" }),
        body: JSON.stringify({ [field]: values }),
      }),
    );
  });

  it("retries a transient failure (5xx, dropped connection) like removeIssueLabel, then succeeds", async () => {
    const fetchImpl = vi
      .fn()
      .mockResolvedValueOnce({ ok: false, status: 502, text: async () => "bad gateway" })
      .mockRejectedValueOnce(new Error("ECONNRESET"))
      .mockResolvedValueOnce({ ok: true });
    const sleep = vi.fn().mockResolvedValue(undefined);
    const client = new GithubReplyClient({ ...baseConfig, fetchImpl, sleep });

    await expect(client.addIssueLabels("acme", "widgets", 12, ["ai-review"])).resolves.toBeUndefined();
    expect(fetchImpl).toHaveBeenCalledTimes(3);
  });

  it("throws after exhausting retries", async () => {
    const fetchImpl = vi.fn().mockResolvedValue({ ok: false, status: 503, text: async () => "unavailable" });
    const client = new GithubReplyClient({ ...baseConfig, fetchImpl, sleep: vi.fn().mockResolvedValue(undefined) });

    await expect(client.addIssueAssignees("acme", "widgets", 12, ["alice"])).rejects.toThrow(/503.*unavailable/s);
    expect(fetchImpl).toHaveBeenCalledTimes(3);
  });

  it.each([404, 422])("does not retry a %d (a deterministic refusal), and throws", async (status) => {
    const fetchImpl = vi.fn().mockResolvedValue({ ok: false, status, text: async () => "nope" });
    const sleep = vi.fn().mockResolvedValue(undefined);
    const client = new GithubReplyClient({ ...baseConfig, fetchImpl, sleep });

    await expect(client.addIssueLabels("acme", "widgets", 12, ["x"])).rejects.toThrow(new RegExp(`${status}`));
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(sleep).not.toHaveBeenCalled();
  });

  it("sends nothing for an empty list", async () => {
    const fetchImpl = vi.fn();
    await new GithubReplyClient({ ...baseConfig, fetchImpl }).addIssueLabels("acme", "widgets", 12, []);
    expect(fetchImpl).not.toHaveBeenCalled();
  });
});

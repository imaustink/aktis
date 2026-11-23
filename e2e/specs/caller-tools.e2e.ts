import { readFile } from "node:fs/promises";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { requireLocalClusterContext } from "../support/guard.js";
import { fetchThrough, kubectl, kubectlApplyStdin, waitFor, type PortForward } from "../support/k8s.js";
import { chatToolTurn, type ChatMessage, type ChatToolDefinition } from "../support/chat.js";
import { invokeStatus, invokeTurn } from "../support/invoke.js";
import { COLLECTIONS, allPointCounts, listCollections, openQdrantGrpc, payloadNames, withQdrant } from "../support/qdrant.js";
import { runGoTests } from "../support/go-test.js";

// Module scope, before any fixture: a suite pointed at the wrong cluster must
// fail on import, not after it has started creating objects.
requireLocalClusterContext();

/**
 * Consumer-supplied tools (docs/adr/0035), end to end.
 *
 * agent-orchestrator's unit tests (Go, internal/facade) cover the decisions; they cannot cover
 * either half of what actually breaks this feature in a cluster:
 *
 * - **Qdrant accepts the queries.** Those tests mock the Qdrant client outright,
 *   so they prove which method the code MEANT to call and nothing about whether
 *   the filter DSL is valid. `has_id`, a payload-only `setPayload`, and a
 *   delete-by-filter `range` are three hand-written filter shapes that a mock
 *   will happily accept and a real Qdrant will reject with a 400 — and the
 *   feature's whole latency story rests on them.
 * - **The catalog stays clean.** The central claim is that a caller's ephemeral
 *   definitions go into their OWN collection and never perturb tool/skill/agent
 *   retrieval. That is a statement about which collection holds which points,
 *   and it is unobservable anywhere but a real Qdrant.
 *
 * Plus the wire contract itself, over the real chat facade and the real planner:
 * a client has to receive `finish_reason: "tool_calls"` with usable arguments,
 * and be able to resume by resending the result.
 */

const CHAT_USER = "e2e-caller-tools-user";
const DEVICE_SERIAL = "SN-4417";
/** What the client "reads off the device" — distinctive so the final answer can be attributed. */
const BATTERY_READING = "37";

/**
 * Makes this run's tool definitions distinct from every previous run's.
 *
 * Load-bearing, and it cost a red build to learn why. The caller-tool index is
 * keyed by a CONTENT HASH and persists in the cluster, so a fixed set of
 * definitions is indexed by the first run that ever sends them and is a pure cache
 * hit for every run afterwards. An "indexing happened" assertion against fixed
 * definitions therefore passes exactly once in the lifetime of a Qdrant volume and
 * fails forever after -- which is the cache working correctly, reported as a
 * product failure.
 *
 * Varying the DESCRIPTION rather than the name is deliberate: the description is
 * part of the hash, so this yields a genuinely new definition each run, while the
 * name stays `get_device_battery` -- which the skill markdown names as
 * `caller:get_device_battery` and every assertion below matches on.
 */
const RUN_TOKEN = `run-${process.pid}-${Date.now()}`;

/** The caller's own tool, as a consumer would declare it. */
const batteryTool: ChatToolDefinition = {
  type: "function",
  function: {
    name: "get_device_battery",
    description: `Read the current battery percentage of an e2e-widget fleet device by serial number. (${RUN_TOKEN})`,
    parameters: {
      type: "object",
      properties: { serial: { type: "string", description: "The device serial, e.g. SN-4417" } },
      required: ["serial"],
    },
  },
};

/**
 * Filler tools, used only to cross the top-K threshold.
 *
 * Deliberately unrelated to the request so ranking has a real job to do: the
 * assertion that matters is that the turn still works with more tools than K,
 * which is the path that embeds and runs a filtered vector search.
 */
const fillerTools: ChatToolDefinition[] = ["convert_currency", "translate_text", "resize_image", "roll_dice"].map(
  (name) => ({
    type: "function",
    function: {
      name,
      description: `Unrelated utility: ${name.replace("_", " ")}. (${RUN_TOKEN})`,
      parameters: { type: "object", properties: { input: { type: "string" } } },
    },
  }),
);

/** The whole array, as sent by the tests that cross the top-K threshold. */
const manyTools = [...fillerTools, batteryTool];

function userTurn(content: string): ChatMessage[] {
  return [{ role: "user", content }];
}

/** A fresh session id per test: session state keys off it, and sharing one lets turns bleed together. */
function session(label: string): string {
  return `e2e-caller-tools-${label}-${process.pid}`;
}

describe("caller-supplied tools: the store's queries against a real Qdrant", () => {
  // The store is Go (internal/callertools, docs/adr/0055), so its queries are
  // exercised by TestLiveQdrant*, run here against the cluster's Qdrant over a
  // gRPC port-forward. They work in their OWN throwaway collection, never the
  // deployment's `caller_tools`, and check exactly what a mock cannot: that
  // Qdrant accepts the hash keyword filter (the isolation boundary -- a gamma
  // indexed but not supplied must never come back), the payload-only
  // lastSeenAt touch (re-indexing adds no points and embeds NOTHING, against a
  // counting embedder), `limit`, and the delete-by-range prune, including the
  // millisecond timestamps the TypeScript store left behind.
  it("indexes, caches, filters, limits and prunes against a real Qdrant", async () => {
    const forward = await openQdrantGrpc();
    try {
      runGoTests({
        module: "orchestrator/engines/temporal",
        pkg: "./internal/callertools",
        pattern: "^TestLiveQdrant",
        mustPass: ["TestLiveQdrantIndexSearchAndCache", "TestLiveQdrantPruneDropsOnlyStaleDefinitionsIncludingLegacyMillis"],
        env: { CALLERTOOLS_QDRANT_ADDR: forward.address },
      });
    } finally {
      forward.close();
    }
  }, 300_000);
});

describe("caller-supplied tools: the round trip through the chat facade", () => {
  let baseline: Awaited<ReturnType<typeof allPointCounts>>;

  beforeAll(async () => {
    const manifestPath = new URL("../manifests/caller-tool-skills.yaml", import.meta.url).pathname;
    await kubectlApplyStdin(await readFile(manifestPath, "utf8"));

    // The Skill catalog hot-reloads over a k8s watch (ADR 0020), so the CRs take
    // effect without a restart -- but not instantly, and a turn that runs before
    // the skill is indexed silently takes the no-skill fallback path and asserts
    // nothing this spec means to assert. Wait for the skills to actually be
    // retrievable, which is a fact about Qdrant, not about the CR existing.
    await waitFor(
      "the e2e caller-tool Skill CRs to be indexed into the skills collection",
      async () => {
        const names = await withQdrant((url) => payloadNames(url, COLLECTIONS.skills));
        return names.includes("e2e-caller-tool-telemetry") && names.includes("e2e-caller-tool-refused")
          ? true
          : undefined;
      },
      { timeoutMs: 120_000, intervalMs: 3_000 },
    );

    baseline = await allPointCounts();
  });

  afterAll(async () => {
    // Left in place these would keep winning retrieval for any turn mentioning a
    // device serial, in every later spec.
    await kubectl(["delete", "skill", "e2e-caller-tool-telemetry", "--ignore-not-found"]).catch(() => undefined);
    await kubectl(["delete", "skill", "e2e-caller-tool-refused", "--ignore-not-found"]).catch(() => undefined);
  });

  it("creates the caller-tool collection separately from the catalog's", async () => {
    // Startup wiring: a deployment that never called `ensureCollection` would
    // 404 on the first caller-tool turn instead of failing at boot.
    const collections = await withQdrant(listCollections);
    expect(collections).toContain(COLLECTIONS.callerTools);
    expect(collections).toContain(COLLECTIONS.tools);
  });

  it("asks the client to run the tool, with arguments it can actually use", async () => {
    const turn = await chatToolTurn(
      CHAT_USER,
      userTurn(`What is the battery level of e2e-widget fleet device ${DEVICE_SERIAL}?`),
      { tools: [batteryTool], sessionId: session("roundtrip") },
    );

    // `finish_reason` is the load-bearing field: a client that sees "stop" here
    // renders an empty assistant message and executes nothing.
    expect(turn.finishReason).toBe("tool_calls");
    expect(turn.toolCalls).toHaveLength(1);
    expect(turn.toolCalls[0]!.name).toBe("get_device_battery");
    // The name must be the CALLER's, never the `caller:`-namespaced internal id --
    // the client matches this string back to one of its own functions.
    expect(turn.toolCalls[0]!.name).not.toContain("caller:");
    expect(turn.toolCalls[0]!.id).toBeTruthy();

    // Arguments must be a JSON object conforming to the schema the caller sent,
    // not the plain string every catalog tool takes.
    const args = JSON.parse(turn.toolCalls[0]!.arguments) as { serial?: string };
    expect(args.serial).toContain(DEVICE_SERIAL);
  });

  it("leaves the tool/skill/agent catalogs untouched by a caller-tool turn", async () => {
    // The central claim of docs/adr/0035: catalog recall cannot be affected by a
    // caller's tools. Deliberately NOT also asserting that `caller_tools` GAINED
    // points -- the turn above sent one tool, which is under the top-K threshold,
    // so the index is skipped entirely and the collection is correctly untouched
    // too. The "more tools than top-K" test below is where indexing actually
    // happens, and that is where the growth assertion belongs.
    const after = await allPointCounts();

    expect(after.tools).toBe(baseline.tools);
    expect(after.skills).toBe(baseline.skills);
    expect(after.agents).toBe(baseline.agents);
    // And the caller's tool never appears in the catalog under any name.
    const catalogNames = await withQdrant((url) => payloadNames(url, COLLECTIONS.tools));
    expect(catalogNames).not.toContain("get_device_battery");
    expect(catalogNames.some((n) => n.startsWith("caller:"))).toBe(false);
  });

  it("finishes the turn using a result the client executed and resent", async () => {
    // The second half of the feature, and the half with no server-side state to
    // fall back on: the `assistant.tool_calls` + `role: "tool"` pair on the wire
    // is the ONLY place this result exists. Before docs/adr/0035 the history fold
    // dropped `role: "tool"` messages outright, so the planner would have
    // re-issued the same call forever.
    const sessionId = session("resume");
    const first = await chatToolTurn(
      CHAT_USER,
      userTurn(`What is the battery level of e2e-widget fleet device ${DEVICE_SERIAL}?`),
      { tools: [batteryTool], sessionId },
    );
    expect(first.finishReason).toBe("tool_calls");
    const call = first.toolCalls[0]!;

    const resumed = await chatToolTurn(
      CHAT_USER,
      [
        { role: "user", content: `What is the battery level of e2e-widget fleet device ${DEVICE_SERIAL}?` },
        {
          role: "assistant",
          content: null,
          tool_calls: [{ id: call.id, type: "function", function: { name: call.name, arguments: call.arguments } }],
        },
        { role: "tool", tool_call_id: call.id, content: `${BATTERY_READING}% remaining` },
      ],
      // Same session id: the conversation's active skill has to survive the round
      // trip, or the resumed turn re-runs full retrieval on a message the human
      // never wrote.
      { tools: [batteryTool], sessionId },
    );

    expect(resumed.finishReason).toBe("stop");
    expect(resumed.toolCalls).toHaveLength(0);
    expect(resumed.text).toContain(BATTERY_READING);
  });

  it("works with more tools than the top-K budget, indexing them just in time", async () => {
    // Crosses the threshold where the just-in-time index is actually consulted:
    // this turn embeds, upserts and runs the has_id-filtered search for real,
    // through the DEPLOYED orchestrator against the DEPLOYED Qdrant. Below the
    // threshold (every test above) that whole path is skipped, so this is the
    // only place the live wiring of it is exercised.
    const before = (await allPointCounts()).callerTools ?? 0;

    const turn = await chatToolTurn(
      CHAT_USER,
      userTurn(`What is the battery level of e2e-widget fleet device ${DEVICE_SERIAL}?`),
      { tools: manyTools, sessionId: session("topk") },
    );

    expect(turn.finishReason).toBe("tool_calls");
    // Ranking had to put the battery tool ahead of four unrelated utilities to
    // leave it in the top 3 the planner ever saw.
    expect(turn.toolCalls[0]!.name).toBe("get_device_battery");

    const after = await allPointCounts();
    // EVERY supplied definition gets indexed, not just the ones that survived
    // ranking -- that is what makes the next turn a cache hit. Exactly `manyTools`
    // many, because this run's descriptions carry RUN_TOKEN and so are new.
    expect((after.callerTools ?? 0) - before).toBe(manyTools.length);
    const indexed = await withQdrant((url) => payloadNames(url, COLLECTIONS.callerTools));
    expect(indexed).toContain("get_device_battery");
    expect(indexed).toContain("convert_currency");
    // And still nothing in the catalog.
    expect(after.tools).toBe(baseline.tools);
  });

  it("re-sending the same tool array adds no points, and an EDITED tool adds exactly one", async () => {
    // The content-hash cache through the DEPLOYED path (docs/adr/0035 §2). The
    // store-level block above proves Qdrant collides the ids; this proves the
    // ORCHESTRATOR keys on content -- not on a per-session or per-request id,
    // which would grow the collection once per turn forever and re-embed on every
    // message.
    const before = (await allPointCounts()).callerTools ?? 0;

    await chatToolTurn(CHAT_USER, userTurn(`Battery level of e2e-widget fleet device ${DEVICE_SERIAL}?`), {
      tools: manyTools,
      sessionId: session("cache"),
    });

    expect((await allPointCounts()).callerTools ?? 0).toBe(before);

    // The other half of "keyed by content": an edited tool that KEEPS ITS NAME is
    // a different definition and must not resolve to the stale embedding of the
    // old one. Only the description changes, so a name-keyed cache would add
    // nothing here and go on serving the previous vector.
    const edited: ChatToolDefinition[] = [
      ...fillerTools,
      {
        ...batteryTool,
        function: { ...batteryTool.function, description: `${batteryTool.function.description} Now in millivolts.` },
      },
    ];
    await chatToolTurn(CHAT_USER, userTurn(`Battery level of e2e-widget fleet device ${DEVICE_SERIAL}?`), {
      tools: edited,
      sessionId: session("cache-edited"),
    });

    expect((await allPointCounts()).callerTools ?? 0).toBe(before + 1);
  });

  it("honors tool_choice: none by never asking the client to run anything", async () => {
    const turn = await chatToolTurn(
      CHAT_USER,
      userTurn(`What is the battery level of e2e-widget fleet device ${DEVICE_SERIAL}?`),
      { tools: [batteryTool], toolChoice: "none", sessionId: session("choice-none") },
    );

    expect(turn.toolCalls).toHaveLength(0);
    expect(turn.finishReason).toBe("stop");
  });

  it("withholds caller tools from a skill that refuses them", async () => {
    // Asserts the REFUSAL BRANCH the skill's markdown defines, not merely the
    // absence of a tool call -- absence is also what a model choosing not to call
    // looks like, and that would pass whether the gate worked or not.
    const turn = await chatToolTurn(
      CHAT_USER,
      userTurn(`Read the sealed diagnostic counters for e2e-sealed rack chassis ${DEVICE_SERIAL}.`),
      { tools: [batteryTool], sessionId: session("gate") },
    );

    expect(turn.toolCalls).toHaveLength(0);
    expect(turn.text).toContain("SEALED_RACK_NO_TOOL");
  });

  it("never emits a tool call for an Open WebUI housekeeping completion", async () => {
    // Open WebUI sends title/tag generation to the SAME endpoint with the SAME
    // body, tool array included. A tool call here would have the client execute a
    // real function as a side effect of rendering a chat title -- which is why
    // the housekeeping short-circuit runs before caller tools are even parsed
    // (docs/adr/0035 §5). Ordering that only this can check: a unit test can
    // assert the graph wasn't invoked, not that a live Open WebUI turn is safe.
    const turn = await chatToolTurn(
      CHAT_USER,
      userTurn(
        `### Task:\nGenerate a concise chat title.\n\n### Chat History:\nUSER: What is the battery level of device ${DEVICE_SERIAL}?`,
      ),
      { tools: [batteryTool], sessionId: session("housekeeping") },
    );

    expect(turn.toolCalls).toHaveLength(0);
    expect(turn.finishReason).toBe("stop");
  });

  it("offers caller tools from the PROGRAMMATIC entry point too, surfacing them on the polled record", async () => {
    // The suite's standing rule: a behaviour that differs per entry point gets
    // covered from each, because every keying bug here has been an asymmetry
    // between two of them. Caller tools are such a behaviour -- both entry points
    // may OFFER, only the chat facade can RESUME -- and `/invoke` has its own
    // translation of the result (`pendingToolCalls` on the record, not
    // `tool_calls` on a message), which nothing else exercises.
    const record = await invokeTurn(`What is the battery level of e2e-widget fleet device ${DEVICE_SERIAL}?`, {
      tools: [batteryTool],
      sessionId: session("invoke"),
    });

    expect(record.status).toBe("succeeded");
    expect(record.pendingToolCalls?.map((c) => c.name)).toEqual(["get_device_battery"]);
    // A record carrying pending calls has no answer in it: the answer depends on
    // the caller running the function.
    expect(record.result).toBeUndefined();
    const args = JSON.parse(record.pendingToolCalls![0]!.arguments) as { serial?: string };
    expect(args.serial).toContain(DEVICE_SERIAL);
  });

  it("rejects a malformed tools array on the programmatic entry point as well", async () => {
    // Validation lives in one place and both facades must reach it -- an entry
    // point that skipped it would silently drop a caller's tools, the exact
    // failure docs/adr/0035 exists to prevent.
    expect(
      await invokeStatus({ request: "hello", tools: [{ type: "function", function: { name: "not valid!" } }] }),
    ).toBe(400);
  });

  it("rejects a malformed tools array instead of silently ignoring it", async () => {
    // Silently dropping a caller's tools is the behaviour docs/adr/0035 exists to
    // fix: a client that offers tools and gets prose back cannot tell "not
    // chosen" from "never seen". Asserted through the real HTTP surface, since
    // the OpenAI error envelope is part of the contract clients special-case.
    await expect(
      chatToolTurn(CHAT_USER, userTurn("hello"), {
        tools: [{ type: "function", function: { name: "not a valid name!" } }],
        sessionId: session("invalid"),
      }),
    ).rejects.toThrow(/400/);
  });
});

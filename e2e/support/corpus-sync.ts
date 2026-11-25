import { expect } from "vitest";
import { fetchThrough, kubectl, withPortForward } from "./k8s.js";

const BROKER_SERVICE = "agent-controller-connection-broker";
const BROKER_PORT = 8080;
const LOCAL_PORT = 18113;
const SYNC_TOKEN_SECRET = "e2e-connection-broker-sync-tokens";

/** The broker's SYNC_TOKEN_<CORPUS> convention: upper-cased, dashes and dots as underscores. */
function syncTokenKey(corpus: string): string {
  return `SYNC_TOKEN_${corpus.toUpperCase().replace(/[-.]/g, "_")}`;
}

/**
 * Runs a corpus's full sync pass now and waits for it, rather than waiting
 * for its schedule.
 *
 * Production's own trigger: POST /corpora/:name/sync as that corpus's sync
 * worker, which the broker answers with a full pass of the corpus's Temporal
 * workflow (docs/adr/0058) that started after the request -- so when this
 * returns, a pass has examined the source as it is now and its chunks are in
 * Qdrant. It replaces creating a Job from the per-Corpus CronJob, which no
 * longer exists.
 */
export async function syncCorpusNow(corpus: string): Promise<{ indexed: number; removed: number; full: boolean }> {
  const encoded = (
    await kubectl(["get", "secret", SYNC_TOKEN_SECRET, "-o", `jsonpath={.data.${syncTokenKey(corpus)}}`])
  ).trim();
  expect(encoded, `${SYNC_TOKEN_SECRET} has no ${syncTokenKey(corpus)}`).not.toBe("");
  const token = Buffer.from(encoded, "base64").toString("utf8");

  return withPortForward(BROKER_SERVICE, BROKER_PORT, LOCAL_PORT, async (_baseUrl, forward) => {
    // A 404 is the broker's watch not having seen the Corpus (or the
    // collection the controller just published) yet: retried for a while,
    // where the old CronJob path quietly accepted "not synced" as done.
    const deadline = Date.now() + 60_000;
    for (;;) {
      const res = await fetchThrough(forward, `/corpora/${encodeURIComponent(corpus)}/sync`, {
        method: "POST",
        headers: { authorization: `Bearer ${token}` },
        // A pass embeds every new chunk; the CronJob path allowed it 300s too.
        timeoutMs: 300_000,
      });
      const body = await res.text();
      if (res.status === 404 && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 1_000));
        continue;
      }
      expect(res.status, `sync ${corpus}: ${body}`).toBe(200);
      return JSON.parse(body) as { indexed: number; removed: number; full: boolean };
    }
  });
}

import { kubectl } from "./k8s.js";

/**
 * What is still in Redis after docs/adr/0034: conversation sessions and session
 * pages. Cache-shaped state whose loss costs a conversation, which is the only
 * thing that Redis -- persistence disabled, on an emptyDir -- can honestly hold.
 *
 * Credentials moved to `support/credential-store.ts`. They were here until that
 * Redis restarted and deleted every link in the cluster.
 */

async function redisCli(args: string[]): Promise<string> {
  // `app.kubernetes.io/name`, not `app` -- the chart uses the standard
  // recommended labels, and the obvious-looking `app=` selector silently
  // matches nothing (kubectl exits 0 with empty output, so it reads as "no
  // Redis" rather than "wrong selector").
  const pod = (
    await kubectl(["get", "pods", "-l", "app.kubernetes.io/name=agent-orchestrator-redis", "-o", "name"])
  )
    .trim()
    .split("\n")
    .filter(Boolean)[0];
  if (!pod) {
    throw new Error("e2e: no agent-orchestrator-redis pod found (label app.kubernetes.io/name=agent-orchestrator-redis)");
  }
  return (await kubectl(["exec", pod.replace("pod/", ""), "--", "redis-cli", ...args])).trim();
}

/** Every key matching `pattern`. Key NAMES only -- this module never reads a value. */
export async function redisKeys(pattern: string): Promise<string[]> {
  const out = await redisCli(["--scan", "--pattern", pattern]);
  return out ? out.split("\n").map((k) => k.trim()).filter(Boolean).sort() : [];
}

/** Deletes keys matching `pattern`, so a test starts from a known state. */
export async function deleteRedisKeys(pattern: string): Promise<number> {
  const keys = await redisKeys(pattern);
  for (const key of keys) await redisCli(["DEL", key]);
  return keys.length;
}

/**
 * Deletes the chat facade's session records (`sess:<sessionId>`, the
 * orchestrator's RedisSessionStore) whose session id matches one of
 * `sessionIdPatterns` (Redis glob syntax).
 *
 * Never `sess:*`: spec files run CONCURRENTLY against one Redis
 * (vitest.config.ts), so that would drop other files' parked turns and
 * session-page records mid-test. Each file passes only patterns for the
 * session ids it creates itself -- its own chat session prefix, its own
 * issues' `<trigger prefix>:<owner>/<repo>#<n>` keys.
 */
export async function deleteSessionRecords(...sessionIdPatterns: string[]): Promise<number> {
  let deleted = 0;
  for (const pattern of sessionIdPatterns) {
    if (!pattern || /^[*?]/.test(pattern)) {
      throw new Error(`e2e: session pattern ${JSON.stringify(pattern)} is not scoped to one spec file`);
    }
    deleted += await deleteRedisKeys(`sess:${pattern}`);
  }
  return deleted;
}

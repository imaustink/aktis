import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll } from "vitest";
import { requireLocalClusterContext } from "./guard.js";

/**
 * Named locks that keep spec files which CANNOT share the cluster from running
 * at the same time, while every other file runs concurrently.
 *
 * Spec files run in parallel (vitest.config.ts). Most need nothing from each
 * other: each claims its own runs (k8s.ts claimRuns), reads fake-github from its
 * own position (fixtures.ts), and deletes only its own credentials
 * (credential-store.ts). A few hold state that cannot be partitioned that way --
 * one identity two files both need in opposite states, or an assertion over ALL
 * traffic -- and those take a lock instead. Vitest has no notion of "these files
 * run one at a time, alongside everything else", so the lock is ours: an
 * atomically-created directory, shared by every worker process on the machine.
 *
 * Acquired in a root `beforeAll`, so a waiting file holds a worker but does no
 * work; vitest.config.ts sizes the pool with that in mind.
 */

/**
 * github:e2e-user and the e2e-user GitHub login: identity-keying needs that
 * subject EMPTY for its negative controls while repo-read-gate seeds it, and
 * both assert over every `/login/...` request fake-github saw.
 */
export const E2E_USER_LOCK = "e2e-user-identity";

/**
 * The `e2e-alpha` KnowledgeBase and its `alpha-docs` Corpus: knowledge-base-crd
 * creates, breaks and deletes them, and knowledge-base-retrieval syncs and
 * queries them -- a delete landing mid-sync left its sync Job hanging.
 */
export const ALPHA_KNOWLEDGE_BASE_LOCK = "e2e-alpha-knowledge-base";

/**
 * The whole cluster: held by every file in the suite's second, exclusive phase
 * (vitest.config.ts), which restarts shared services or reconfigures the
 * shared stub agent. That phase starts only after the concurrent one has
 * finished; the lock then runs its files one at a time.
 */
export const EXCLUSIVE_LOCK = "exclusive";

/** Long enough to wait out every other holder in a full run, with margin. */
const LOCK_WAIT_MS = 45 * 60 * 1000;
const POLL_MS = 1_000;

function lockDir(name: string): string {
  // Per cluster context: two suites against different clusters share nothing.
  const context = requireLocalClusterContext().replace(/[^A-Za-z0-9_.-]/g, "_");
  return join(tmpdir(), "aktis-e2e-locks", context, name);
}

function isAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    // EPERM: it exists but belongs to someone else -- alive.
    return (err as NodeJS.ErrnoException).code === "EPERM";
  }
}

async function ownerOf(dir: string): Promise<number | undefined> {
  const raw = await readFile(join(dir, "owner"), "utf8").catch(() => undefined);
  const pid = Number(raw);
  return raw && Number.isInteger(pid) ? pid : undefined;
}

async function acquire(name: string): Promise<void> {
  const dir = lockDir(name);
  await mkdir(join(dir, ".."), { recursive: true });
  const startedAt = Date.now();
  let reported = false;
  for (;;) {
    try {
      await mkdir(dir); // atomic: exactly one creator wins
      await writeFile(join(dir, "owner"), String(process.pid));
      if (reported) console.log(`  [lock] ${name} acquired after ${Math.round((Date.now() - startedAt) / 1000)}s`);
      return;
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "EEXIST") throw err;
    }
    // A holder that died (a crashed worker, an aborted run) never releases.
    // Its pid says so; an owner file not yet written means a holder that is
    // mid-acquire, which is alive by definition.
    const owner = await ownerOf(dir);
    if (owner !== undefined && owner !== process.pid && !isAlive(owner)) {
      await rm(dir, { recursive: true, force: true });
      continue;
    }
    if (!reported) {
      console.log(`  [lock] waiting for ${name} (held by pid ${owner ?? "?"})`);
      reported = true;
    }
    await new Promise((r) => setTimeout(r, POLL_MS));
  }
}

async function release(name: string): Promise<void> {
  const dir = lockDir(name);
  if ((await ownerOf(dir)) === process.pid) await rm(dir, { recursive: true, force: true });
}

/**
 * Holds lock `name` for the whole of the calling spec file. Call at module
 * scope, after `requireLocalClusterContext()`.
 */
export function holdLock(name: string): void {
  beforeAll(() => acquire(name), LOCK_WAIT_MS);
  afterAll(() => release(name));
}

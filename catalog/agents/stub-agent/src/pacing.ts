/**
 * How long a stub turn takes, and whether it narrates while it does.
 *
 * The stub replies instantly by default, which makes it useless for the one
 * class of behaviour that needs a turn still in flight when something else
 * happens to the cluster: an orchestrator rollout, a NATS bounce, or a silence
 * long enough to trip the idle window. These knobs open that window.
 *
 * Kept out of index.ts so the parsing is unit-testable without a NATS
 * connection, matching reply.ts.
 */
export interface StubPacing {
  /**
   * Emit a progress message every `narrateEveryMs` for this long before
   * replying. Narration RESETS the orchestrator's idle window, so a turn paced
   * this way must survive regardless of how far it exceeds that window.
   */
  narrateForMs: number;
  /** Cadence of the narration above. */
  narrateEveryMs: number;
  /**
   * Say NOTHING for this long before replying (after any narration phase).
   * Exceeding the orchestrator's idle window here must fail the turn — that is
   * the behaviour the window exists for, and it silently did not work at all
   * before (the bound was a nats.js first-message timeout, cancelled by the
   * stub's own first progress message).
   */
  silentForMs: number;
}

/** Upper bound on any single knob, so a typo cannot hang a suite for hours. */
const MAX_MS = 10 * 60 * 1000;

function ms(raw: string | undefined, fallback: number): number {
  const n = Number(raw);
  if (!raw || !Number.isFinite(n) || n < 0) return fallback;
  return Math.min(n, MAX_MS);
}

/**
 * A per-run pacing directive, written into the turn itself (an issue body, a
 * chat request): `[stub-pacing narrateForMs=120000 narrateEveryMs=5000
 * silentForMs=0]`. Omitted keys take the same defaults as the env vars.
 */
const DIRECTIVE = /\[stub-pacing((?:\s+[A-Za-z]+=\d+)*)\s*\]/;

/**
 * The pacing for one run: a directive in `turnText` if it carries one, else the
 * `STUB_*` env vars (from the stub-agent CR).
 *
 * The directive exists because the env vars are deployment-wide. Patching the
 * CR paces EVERY stub run that starts while the patch stands, so a spec that
 * did it could not share the cluster with any other spec launching the stub --
 * which, with e2e spec files running concurrently, is most of them. A directive
 * paces only the run whose turn carries it. When present it REPLACES the env
 * pacing rather than merging with it, so a run's behaviour never depends on
 * what some other spec last patched into the CR.
 */
export function readPacing(env: NodeJS.ProcessEnv, turnText = ""): StubPacing {
  const directive = DIRECTIVE.exec(turnText);
  if (directive) {
    const knobs = Object.fromEntries(
      (directive[1] ?? "")
        .trim()
        .split(/\s+/)
        .filter(Boolean)
        .map((kv) => kv.split("=") as [string, string]),
    );
    return {
      narrateForMs: ms(knobs.narrateForMs, 0),
      narrateEveryMs: ms(knobs.narrateEveryMs, 2000) || 2000,
      silentForMs: ms(knobs.silentForMs, 0),
    };
  }
  return {
    narrateForMs: ms(env.STUB_NARRATE_FOR_MS, 0),
    narrateEveryMs: ms(env.STUB_NARRATE_EVERY_MS, 2000) || 2000,
    silentForMs: ms(env.STUB_SILENT_FOR_MS, 0),
  };
}

/** True when the stub should behave exactly as it did before these knobs existed. */
export function isImmediate(p: StubPacing): boolean {
  return p.narrateForMs === 0 && p.silentForMs === 0;
}

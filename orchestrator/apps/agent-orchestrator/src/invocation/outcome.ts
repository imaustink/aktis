// A run's structured outcome (docs/adr/0052 §2) on its way to `GET /invoke/:id`.

/**
 * Upper bound on an outcome's compact JSON. The Go Temporal engine enforces
 * the same number, so a run's completion event carries the same outcome
 * whichever engine ran it. The bound exists because the outcome rides inside
 * the invocation record, the gateway's run-completion envelope and every CEL
 * evaluation over it: it is a small machine-readable summary (a PR number, a
 * verdict), not a channel for the agent to ship arbitrary data.
 */
export const MAX_OUTCOME_BYTES = 8192;

export type OutcomeCheck =
  | { ok: true; outcome: Record<string, unknown> }
  | { ok: false; reason: string };

/**
 * Accepts `raw` only as a plain JSON object whose compact serialization is at
 * most MAX_OUTCOME_BYTES (UTF-8). Anything else (an array, a scalar, an
 * oversized object, something JSON.stringify rejects) is refused: the
 * caller drops it rather than truncating, because a half outcome could still
 * satisfy a CEL predicate it should not.
 *
 * The returned object is a JSON round-trip of `raw`, so the record holds
 * exactly what the size check measured.
 */
export function checkOutcome(raw: unknown): OutcomeCheck {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) {
    return { ok: false, reason: `outcome must be a JSON object, got ${raw === null ? "null" : Array.isArray(raw) ? "array" : typeof raw}` };
  }
  let json: string;
  try {
    json = JSON.stringify(raw);
  } catch (err) {
    return { ok: false, reason: `outcome is not serializable: ${err instanceof Error ? err.message : String(err)}` };
  }
  const bytes = Buffer.byteLength(json, "utf8");
  if (bytes > MAX_OUTCOME_BYTES) {
    return { ok: false, reason: `outcome is ${bytes} bytes of JSON, over the ${MAX_OUTCOME_BYTES}-byte limit` };
  }
  const parsed: unknown = JSON.parse(json);
  // An object with a toJSON (a Date, say) serializes to a non-object.
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    return { ok: false, reason: "outcome does not serialize to a JSON object" };
  }
  return { ok: true, outcome: parsed as Record<string, unknown> };
}

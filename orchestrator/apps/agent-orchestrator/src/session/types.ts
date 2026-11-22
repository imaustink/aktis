/**
 * What this process remembers about a conversation between turns. The Temporal
 * engine holds a conversation's own state (skill continuity, continuation
 * tokens, pending links and approvals) in its workflow (docs/adr/0036, 0053),
 * so all that is left here is the sticky read-only flag the facade applies
 * before a turn reaches the engine.
 *
 * Records written before docs/adr/0053 carry more fields (an active skill,
 * agent-run anchors, continuation tokens, approval pauses); they are simply
 * ignored, and expire with their TTL.
 */
export interface SessionRecord {
  /**
   * The conversation ran a read-only turn (an EventTrigger with
   * `target.readOnly`, docs/event-triggers.md §1), so every later turn in it is
   * read-only too -- a session-page follow-up, a continuation.
   *
   * Honored regardless of who wrote the record (restricting a turn is never a
   * leak) and written eagerly when the turn is accepted, before any run
   * exists. Only a writable trigger turn clears it (server.ts
   * `setSessionReadOnly`). Losing the record (TTL, Redis outage) still drops it
   * -- the session store is soft-failing by contract -- which is why the
   * trigger re-asserts it on every event it matches rather than relying on
   * this alone.
   */
  readOnly?: boolean;
  /** Last touch time (ms since epoch); used for sliding TTL expiry. */
  updatedAt: number;
}

/**
 * Port for conversation-session persistence. The default adapter is
 * in-memory ({@link InMemorySessionStore}) which assumes a single
 * orchestrator replica; the Redis adapter shares it across replicas
 * (docs/adr/0016).
 */
export interface SessionStore {
  get(sessionId: string): Promise<SessionRecord | undefined>;
  set(sessionId: string, record: Omit<SessionRecord, "updatedAt">): Promise<void>;
}

// JSON value model for the event envelope (docs/event-triggers.md §2-§3).
//
// CEL itself is evaluated by the orchestrator (Go, cel-go): this package only
// describes and builds the envelope that evaluation runs over.

export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

/** Defines an own enumerable property even for keys like "__proto__". */
export function setOwn<T>(obj: Record<string, T>, key: string, value: T): void {
  Object.defineProperty(obj, key, { value, enumerable: true, writable: true, configurable: true });
}

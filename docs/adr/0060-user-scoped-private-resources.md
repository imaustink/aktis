# 0060. `allowedPrincipals` privately scopes catalog resources and Corpora on top of RBAC

Date: 2026-11-27

## Status

Proposed. Extends [0004](0004-rbac-scoped-dynamic-tool-discovery.md)
(RBAC-scoped retrieval), [0011](0011-skill-access-derived-from-tools.md)
(skill access derived from tools/agents) and
[0039](0039-knowledgebase-crd-composed-corpora.md) §4 (knowledge-base member
visibility). Uses the per-user identity signals from
[0049](0049-configurable-agent-identity-mode.md) §5.

Resolves issue #149 ("User Scoped Skills, Agents and Tools"). Replaces the
design in PR #193, which targeted the TypeScript orchestrator removed by
[0053](0053-remove-langgraph-engine.md)/[0056](0056-agent-orchestrator-in-go.md)
and keyed on a principal that is not known when retrieval runs (see
"Which identity is matched").

## Context

Retrieval is gated by RBAC alone. Every Tool, LocalTool, MCPTool, Agent and
Corpus carries a required `allowedRoles`; the vector store returns a record
only when it intersects the caller's roles; a Skill carries no roles and
derives its audience from what it references (0011). A role names a class of
caller, not a person.

Issue #149 asks for the missing axis: a resource only specific people can
discover and use, however many people share the role that would otherwise
expose it. RBAC can only express that by minting a one-person role and
attaching it everywhere.

## Decision

Add an optional **`allowedPrincipals: []string`** to the `Tool`, `LocalTool`,
`MCPTool`, `Agent`, `Skill` and `Corpus` specs, and to each `MCPServer`
exposure entry (mcp-broker copies it onto the MCPTool it writes, like
`allowedRoles`). core-controller only stores the field; the engine enforces it.

### Semantics

- **Empty or unset means public.** `allowedRoles` alone decides, exactly as
  before, so every existing CR is unchanged.
- **Non-empty means private.** A caller sees the resource only when **both**
  hold: one of their roles is in `allowedRoles`, **and** one of their
  principals is in `allowedPrincipals`. Private scoping only narrows what RBAC
  admits; it never grants. A resource meant to belong to one person pairs a
  broad `allowedRoles` with a one-person `allowedPrincipals`.
- Fail closed throughout. A caller with no principals sees no private
  resource.

The rule lives once, in `internal/access` (`Viewer.Admits(Audience)`), and
every visibility decision calls it: the Qdrant query filter's in-code twin
(`GetByIDs`), knowledge-base member gating (`visibleMembers`,
`VisibleConnections`) and the per-member checks in corpus read, query and
lookup.

### Which identity is matched

A principal is a caller **subject** as the engine resolves it, for example
`openwebui:<user-id>`. A caller's principals are:

1. `Caller.Subject`, **only when `Caller.PerUser` is true**. `PerUser` comes
   from a resolver that structurally knows the subject is one human, carried
   to the gateway in the signed caller-identity assertion. A shared subject
   (the gateway's own service subject, a bot, a trigger) is never a
   principal, even if a CR happens to list that string; otherwise everyone
   behind that subject would see the resource.
2. `Caller.SenderSubject`: the subject a webhook sender's account is linked
   under, set by the gateway **only** from a verified sender assertion whose
   status is `linked` (0049 §5). This lets a private resource reach its owner
   on a turn they triggered from GitHub as well as from chat, using the same
   identifier. An unsigned envelope sender, or an `ambiguous`/`unlinked` one,
   contributes nothing.

PR #193 used `principal ?? subject`. That no longer works: `Caller.Principal`
is the credential key, empty when retrieval runs and only established (and
possibly upgraded) by the authorization pre-flight after a delegate is
chosen, and falling back to a non-per-user `subject` would match a shared
identity.

Provider principals such as `github:<login>` are not matched in v1. They
would need the principal resolved before retrieval, and a raw `SenderLogin`
is not trustworthy without an assertion secret.

### Skills derive a principal scope the way they derive roles

A Skill may carry its own `allowedPrincipals`. This is owner intent, not a
capability grant, so it is the one access field a Skill states directly. Its
**effective** scope (`DeriveSkillAccess`) is the intersection of:

- its own `allowedPrincipals`, if any, and
- the `allowedPrincipals` of every referenced Tool/Agent that is **itself**
  private. Public refs add no constraint.

Intersecting stops a skill from ever widening a private tool's audience: the
planner may call any referenced tool, so the caller must be allowed on all of
them. A disjoint result is "private, nobody listed" and admits no one. That
is why the store record carries an explicit `private` flag rather than
inferring it from a non-empty list. With no private input the skill is public
on this axis. A skill with no refs stays `Unrestricted` on roles (0011), but
its own `allowedPrincipals` still apply.

### Knowledge bases use the union, like roles

A Corpus is the unit of access inside a knowledge base (0039 §4, 0043), so
`allowedPrincipals` goes on the Corpus beside `allowedRoles`, not on the
KnowledgeBase. The derived KB skill and its generated search/read/query tools
are visible to anyone who can see **at least one** member, the same union
0039 uses for roles: one public member keeps them public; if every member is
private, their principal lists are unioned. That union only controls
discovery. Every search, read, query and lookup re-checks each member with
the full rule, and a private member the caller is not listed on is withheld
and counted in the partial-visibility disclosure exactly like a role-scoped
one.

Withholding a private member's content is not enough, because the derived
skill's markdown and the read/query tools' descriptions are one shared text
for everyone the face admits. A private member is therefore **named** there
(id, label, provider, description) only when the face is itself private and
every principal it admits is on that member's own list. Otherwise it is
replaced by a count ("plus N private sources"), so a public knowledge base
never reveals that, say, "Alice's 1:1 notes" exists. A caller the member does
admit still reaches it: search results cite its `<corpus>`, which the read and
query tools accept, and execution gates every member per caller.

Chunks in a Corpus collection carry roles only. Private scoping is enforced
at the member, before any chunk query, on every path that reaches a corpus.

### Store encoding

Each catalog point's payload gains `private` (bool) and `principals`
(keywords). The filter is

```
must:
  - should: [unrestricted == true, roles ∋ any(viewer.roles)]
  - should: [must_not(private == true), principals ∋ any(viewer.principals)]
```

with the second clause's keyword match omitted for a viewer with no
principals. `GetByIDs` applies the same rule in code to the payload it reads
back. `GetByIDsUnfiltered`, used only for an Agent's operator-declared
`toolRefs` (0028), stays unfiltered: that question is about deployed
configuration, not the caller, for principals as for roles.

## Consequences

- An owner makes a CR private by listing principals; nothing else changes.
  Removing a person is a one-line edit, picked up through the catalog watch
  and the debounced skill/KB re-derivation.
- **Rolling upgrade is safe.** A point written before this change has no
  `private` field. `must_not(private == true)` passes an absent field, so it
  reads as public, which is what it was. The catalog is re-listed and
  re-indexed at startup anyway.
- **Not a credential boundary.** `allowedPrincipals` controls discovery and
  selection. It is orthogonal to `identityProviders` and identity mode (0049),
  which decide whose credential a launch runs with.
- **Agent `toolRefs` are not caller-filtered.** An operator who puts a
  private tool in a public agent's `toolRefs` has given that agent's callers
  the tool, the same as with roles today. Making the agent itself private is
  the way to keep it to its owner.
- Not addressed: group or wildcard principals, provider principals
  (`github:<login>`), automatically adding a CR's creator, chunk-level
  principal stamping, and any UI. Groups can be layered on later through the
  same `Viewer.Principals` list without changing the filter.

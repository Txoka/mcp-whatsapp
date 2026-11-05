# Fork changes

Based on upstream commit `078ac8c5e3f8a6f32c6a5902e4859f86c853330f`.

- Normal message reads use compact local-time output, with exact transport identifiers available in detailed mode.
- Overlapping context windows are deduplicated, ordered and scoped to the selected chat. Same-second messages have a deterministic ID tie-breaker; this is not a claim about their true subsecond send order.
- Message-context lookups accept a chat reference and reject ambiguous unscoped message IDs. Opaque message cursors avoid offset shifts when new messages arrive.
- Bounded automatic history loading handles cached anchors, asynchronous delivery, cooldowns and LID fallback. Incomplete history is reported rather than described as an empty conversation.
- History synchronization extracts participant metadata from both supported locations and does not overwrite known authors with a group identity or missing author. Chat naming prefers exact peer contacts.
- Unread queries distinguish supplied counters, known cached unread identities and manual unread markers. Missing counters remain unknown. A complete initial unread baseline is not guaranteed.
- Event-driven waits support incoming messages and read-state changes, with optional chat filters and resumable cursors. Default wait is one hour; `-1` waits until an event or cancellation. Client/transport deadlines still apply. Outgoing messages from other linked devices are cached but do not currently wake incoming-message waits.
- Explicit WhatsApp mention metadata is retained and resolved into readable names and mentions-me flags. Original message text is preserved. Historical rows lacking metadata are not guessed from literal @numbers.
- Group discovery returns compact summaries. Participant pages expose phone, saved contact name, synced profile name, role and exact reference separately; missing names remain null.
- The upstream local outbound/usync throttle package and bypass-header plumbing are removed. WhatsApp's own responses and limits remain outside this implementation's control.
- Compose and initialization/probe helpers keep runtime data outside version control and build contexts.

## Validation

The complete Go test suite passed after the code changes. Store/MCP race tests passed for concurrent event waiting and retrieval. Live checks used cached reads, group metadata and bounded waits; no chat messages or read receipts were sent as tests. This validation does not establish complete synchronization of every WhatsApp state or every possible history variant.

## Explicit deletion scopes and automatic history

Message listing retains automatic selected-chat history backfill. The manual
`request_sync` tool is removed from the agent catalog.

`delete_message` requires `scope: everyone` or `scope: me`, with no default.
Both scopes require approval. Everyone uses WhatsApp revocation; me sends a
version-3 regular-high account-state deletion mutation and removes the cached
message after acceptance. It requires the exact cached message timestamp and
ownership. Incoming linked-device DeleteForMe events apply the same cleanup.
Durable chat/message tombstones prevent history replay from restoring deleted
messages. Cached attachment files are retained (`deleteMedia=false`).

Validation uses isolated tests; no real messages are deleted as tests. Successful
submission does not prove that every linked client has applied the deletion.

## Transparent synchronized reads

The agent-facing `list_messages` schema has no `auto_sync` switch. Cache-only
inspection remains possible through local diagnostics, not an agent argument.
Cache-backed reads wait for connection catch-up and active history imports.
Disconnected or failed synchronization does not silently return cached data.

A selected-chat query that needs more history waits for its correlated ON_DEMAND
response to be imported. Partial chunks do not complete a request. Reads continue
through completed batches, without fixed request cooldowns.
Recent reads use a 3-second sync window; older searches/pages use the configured
longer budget. Caller cancellation still applies.
Timeout, import failure, missing correlation, or a response that cannot advance
history without an explicit boundary returns cached rows with a warning.
Concurrent reads of a chat share newly loaded data rather than fetching in parallel.

Global searches and unread queries cover synchronized local state; they do not
claim to scan every message on the phone. Explicit WhatsApp end/inaccessible-history
markers are distinguished from timeout. All cached read results state that coverage
and remote edits/deletions may be incomplete. No protocol mechanism proves a fully
current authoritative phone snapshot.

## Concurrent account-state updates

Delete-for-me updates serialize the full account-state operation, not just the
library fetch phase. Each update fetches the latest collection state first.
A mismatching LTHash triggers a full rebuild of that local collection from the
server. No device logout or server-side account reset is requested. A failure
after sending is reported as an unknown outcome; the mutation is never retried
by our wrapper. Recovery fetches can deliver deletion events already accepted by
WhatsApp. Users should not blindly retry uncertain deletions.

## Transactional architecture

See [architecture](ARCHITECTURE.md) for component boundaries and exact guarantees.
Normalized input is captured durably and projected atomically. Message revisions,
tombstones, read markers and poll votes use explicit ordering rules; unversioned
conflicts are unavailable rather than presented as known text. Related reads use
one committed snapshot. Outgoing operations and uncertain outcomes survive restart.

History requests retain confirmed transfer membership/spans and real continuation
keys. Fixed one-second and thirty-second pacing and the missing-metadata completion
fallback are removed. Operator resource policy is separate from WhatsApp limits;
server backoff is handled by the updated pinned SDK. Unread-chat results support
page/next_page, and resource-limit errors replace silent clamping.

- Display detailed messages and JSON chat/context timestamps in the configured
  IANA timezone; retain UTC storage and DST-aware offsets.
- Preserve caption provenance, repair legacy unknown captions when available, and
  return caption metadata alongside downloaded media.
- Persist outgoing local projections before submission and replay acknowledged
  operations without resending them after cancellation or restart.

- Return compact persistent message-page/update-watch bookmarks, bound to their
  filters and purpose; reject raw cursor formats and remove numbered message pages.
- Detect no-progress transfers even when the cached page is full. PN/LID fallback
  applies to direct chats only; groups do not repeat the same request under an
  identity mode that cannot apply to groups.

- Remove the unused manual history-request API and speculative anchor fallback;
  automatic history requests require an exact stored anchor and correlation ID.

- Compare message ordering, time bounds and cursor positions as timestamp instants,
  with expression indexes and a mixed-offset regression test.

- Recent message reads use a configurable 3s sync window and at most one targeted
  refresh for a full cache page. Uncertain/partial results carry warnings, not a
  coverage veto. They never scan weeks of old history to revalidate live messages.
- Final responses re-read a coherent committed snapshot, without repeating the
  long read gate. Targeted refreshes cannot reset older-history frontiers.
- Accumulate history membership/bounds across every chunk, rather than keeping
  only the final chunk. This prevents false gaps and forward-moving frontiers.

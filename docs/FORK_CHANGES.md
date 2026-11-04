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

# Architecture and consistency contract

The service is one Go process with SQLite and a pinned whatsmeow adapter. It does
not use a second service, message broker, or model-managed synchronization.

## Components

- `internal/client/message_events.go`: decode message creation, edit and revoke
  variants, retain identity namespaces, and validate original message keys.
- `internal/store/inbox.go`: capture normalized input before projection. A restart
  can replay pending local work without repeating a network write. Applied input
  payloads are cleared; their digest and commit version remain.
- `internal/store/projector.go`: apply a message/history batch atomically, including
  mentions, revisions, tombstones, poll metadata, unread state and notification
  references. Protocol revisions dominate unversioned creation replay. Conflicting
  unversioned bodies are explicitly unknown until a versioned event resolves them.
- `internal/store/snapshot.go`: pin related message, context, unread and formatting
  reads to a committed SQLite snapshot. SDK contacts are a separate source; this
  is not a transaction over the remote phone or its address book.
- `internal/client/read_sync.go` and `internal/mcp/history_reader.go`: track
  correlated history responses, confirmed transfer membership/spans, explicit
  boundaries, cancellation and bounded shared work. Missing chunk metadata does
  not prove completion. A full SQL page alone does not establish history coverage.
- `internal/client/operations.go`, `tracked_send.go`, `account_state.go`: journal
  exact message operations, preserve unknown outcomes, and serialize versioned
  account-state writes. Only typed pre-submission failures permit resubmission.
- `internal/mcp/write_boundary.go`: record mutating tool invocations before their
  handlers run. Generic failed invocations are conservatively unresolved; this
  does not mean every such record represents a remote side effect. Specific child
  message records can establish stronger outcomes. No automatic write replay occurs.

## Invariants

1. Message, read-state and poll projection changes commit atomically. Readers do
   not observe half of a history import or mixed message/context revisions.
2. Read-marker changes and message changes advance the projection version.
3. A tombstone prevents history replay from recreating a deleted message, unread
   badge entry, poll tally or queued message notification. Cleanup does not reduce
   the notification cursor sequence.
4. Original message time is separate from edit time. An older or unversioned body
   cannot replace an explicit newer edit. Conflicting votes at the same revision
   are not assigned a guessed order.
5. History request submission, confirmed completion, exhaustion, inaccessibility,
   timeout and interruption are separate states. Returned message keys must belong
   to confirmed transfers before a selected history page is considered established.
6. A successful network submission is not a guarantee of delivery to every device.
   Unknown writes are retained across restart and are not blindly repeated.
7. Approval is enforced by the configured MCP client. Annotations describe effects;
   they are not an authorization mechanism. A bearer credential grants direct API
   capabilities independently of the client's approval dialog.

## Scheduling and resource policy

There are no fixed one-second/global or thirty-second/per-chat history delays.
Requests advance when eligible work completes. Shared reads reuse loaded data.
The SDK's explicit server-backoff handling remains enabled.

| Operator setting | Default | Meaning |
|---|---:|---|
| `WHATSAPP_HISTORY_CONCURRENCY` | 1 | Maximum simultaneous history requests; compatibility/resource setting, not a claimed WhatsApp quota |
| `WHATSAPP_HISTORY_QUEUE` | 64 | Active and queued history work budget |
| `WHATSAPP_HISTORY_BATCH_SIZE` | 50 | SDK-recommended peer request size; not a documented hard maximum |
| `WHATSAPP_READ_TIMEOUT` | 5m | Total synchronization budget, bounded by earlier client cancellation |
| `WHATSAPP_MAX_PAGE_SIZE` | 100 | Response row budget; use pagination |
| `WHATSAPP_MAX_RESPONSE_BYTES` | 262144 | Cached-query response budget; excessive output returns an explicit resource error |

Compose forwards these settings from the operator's environment or private `.env`.
They are not exposed as synchronization arguments to the model. Invalid settings
fail initialization. Explicit output/context limits produce errors instead of
silently changing requested limits. Pairing endpoint protections are separate.
The update waiter retains its own one-hour default and cancellable indefinite mode.

## SDK and recovery

whatsmeow is pinned to `v0.0.0-20261102152715-54038813c47f` (2026-11-02).
It implements app-state backoff from explicit upstream 429/backoff responses.
The compatibility check tracks all relied-on SDK methods; build/test uses a clean
source image excluding private runtime data and historical snapshots.

The serve command acquires the store lock before marking interrupted requests.
Interrupted write submissions become unknown. Pending projection input is replayed
locally; network writes are never replayed during startup. A reconnect alone does
not clear a failed projection. Recovery must confirm that its captured batch applied.
Selected-chat recovery does not need to replay unrelated captured failures.

`python3 ops/inspect_state.py` reports local state counts without printing message
bodies, tokens, session keys or raw input payloads. Counts are database records,
not numbers of people or messages. Root invocation and child operation records
must not be added together as if each represented a separate remote action.

## Protocol limits and boundaries

A snapshot describes committed, observed local state. It is not a proof of a
complete, current phone replica. WhatsApp can withhold older history and does not
provide universal freshness or coverage guarantees. Global search and context
lookups operate over the synchronized index. Expiration and every possible remote
message/state variant are not claimed to be fully replicated. Unsupported state
must not be inferred from absence.

Legacy bare sender identifiers are retained for compatibility and enriched only
on matching decoded identity evidence. New events preserve full PN/LID namespaces.
Historical data is not retroactively certified by a schema migration.

Generic tool invocation outcomes and SDK callbacks can remain unresolved after
an ambiguous failure. Exact edit/revoke/delete-for-me events can reconcile their
message-operation records. There is no claim of exactly-once delivery or automatic
semantic reconciliation of every group/privacy operation.

Cross-device write propagation requires end-to-end observation; synthetic tests
and a server acknowledgement alone do not prove it. Live verification uses reads
and bounded history requests. No real chat message, reaction, read receipt or
message deletion is used as a test.

## Display time and attachment captions

Storage and pagination keys remain UTC instants. The operator sets an IANA display
zone through `WHATSAPP_TIMEZONE` in Compose (default `Europe/Madrid`, forwarded as
`TZ`). Compact and detailed message output use the same zone. JSON chat/context
instants include the appropriate numeric UTC offset. Daylight saving time is
calculated for the message date, never added as a fixed hour. Per-query timezone
can override display only.

Attachment captions are message metadata, distinct from text recognized inside an
image. Previously cached blank image/video/document bodies have unknown provenance;
the migration marks them unknown rather than claiming they have no caption. A
new complete decoded media message establishes either its caption or explicit
absence. Download results include `caption_status` and, when known, `caption`.
For legacy unknowns, download attempts a bounded correlated history fetch from a
real newer message key. No newer key, inaccessible history, or failed transfer
leaves the caption explicitly unknown. Image inspection cannot reconstruct a
missing WhatsApp caption.

## Durable outgoing projection

Before message submission, the outbox saves the transport ID and normalized local
projection. Server acknowledgement records the server timestamp. Local replay can
then finish after cancellation or restart without resending the network operation.
Uncertain submissions are never automatically retried. Accepted edit/revoke/local
delete operations use the same projection boundary without inventing creation
timestamps. Captured outgoing payloads are cleared after successful application.

## No-progress handling

An already full cached page never counts as progress by itself. After a confirmed
transfer, the reader checks coverage and the next real anchor. An unchanged
frontier without sufficient coverage produces an explicit incomplete-history error
(after the bounded PN/LID compatibility alternative), rather than repeatedly
requesting the same batch until timeout. Live empty transfers do not certify that
retained cached messages still exist on the primary device.

## Continuation bookmarks

The agent-facing message-page and update-watch cursors are short opaque bookmarks
(`p_…` and `u_…`). The private `cursor_bookmarks` table stores their actual keyset
or event-stream positions. References survive service restarts and are reused for
identical positions and filters. They contain no phone number, message ID or query
text. They are references, not authentication credentials; the MCP endpoint still
requires authentication and the client still controls write approval.

Page bookmarks are bound to the message filters; update bookmarks are bound to
chat and event kind. Wrong-purpose, unknown and changed-filter bookmarks fail
explicitly. Changing display format, display timezone or page size does not change
the underlying message query. Old encoded page cursors and epoch/sequence update
cursors remain accepted. Message `page` remains a compatibility option; use the
returned bookmark and omit `page` for new calls. Never synthesize or increment a
bookmark. Offset-based chat-list compatibility APIs are unchanged.

Paging walks older messages from a real message key, so new head messages do not
shift the next page. Update bookmarks resume an event stream and avoid missing
arrivals between waits. These are different operations; numbered pages cannot
replace an update-stream position. A bookmark does not freeze the remote phone
or guarantee that WhatsApp can supply older history. Page data can reflect later
edits or deletions. Bookmarks have no automatic expiry, are private local database
state, and are removed with that database. Each distinct continuation/filter pair
uses one row; repeated empty waits at the same position reuse their reference.

# Durable internal message delivery

Implemented 2026-10-28 as the next X12 collaboration increment. This protocol is
for explicit message flows in a deployed collection; public message-name
correlation remains a separate API.

## Commit boundaries

1. A routed send executes its successful output mappings and end listeners on a
   candidate snapshot. That snapshot contains both the next sender token and
   every outgoing flow's `MessageDelivery` record in
   `event_state.message_outbox`. One required instance save commits the complete
   send occurrence before any receiver work is exposed. The execution revision
   on the token increases even when sequence flows revisit the same send node.
2. Dispatch selects only the route's pinned target definition/version and element
   with the exact nonempty instance business key. An unavailable or ambiguous
   receiver leaves the delivery pending. Once selected, a receiver identity is
   saved with the sender outbox before receiver mutation; retries never silently
   bind to a different catch or start instance.
3. The receiver candidate snapshot contains the payload effects, successful catch
   end listeners/mappings, the next token and a `MessageInbox` receipt. Cleanup
   records name the exact old jobs to remove. One required instance save commits
   these together, before continuation execution or sender acknowledgment.
4. Job cleanup is retried by identity. The permanent `MessageReceipt` is then
   atomically created in the persistence adapter. Reusing an ID with different
   content fails closed. Once this permanent bridge exists, the temporary inbox
   entry can be pruned so long-running instances do not retain every payload.
5. The sender outbox entry is removed with a required snapshot save. Receiver
   continuation uses the existing event reconciliation machinery. On restart,
   the committed token resumes, or its already-published wait state is retained.

```mermaid
sequenceDiagram
    participant Sender
    participant Store as WorkflowPersistence
    participant Dispatch
    participant Receiver
    Sender->>Store: save instance: continuation + all outbox records
    Dispatch->>Store: read committed outbox (active + archived senders)
    Dispatch->>Store: save exact receiver binding in sender snapshot
    Dispatch->>Receiver: revalidate pinned catch/start occurrence
    Receiver->>Store: save effects + inbox + continuation + cleanup identities
    Receiver->>Store: delete old jobs; persist any boundary rearm
    Receiver->>Store: create-or-return permanent message receipt
    Dispatch->>Store: remove acknowledged sender outbox entry
    Dispatch->>Receiver: reconcile committed continuation
```

## Receiver behavior

- **Message starts:** the delivery UUID reserves the receiver instance identity.
  Defaults and the explicit payload are applied, while the business key comes
  from delivery metadata. Start and end callbacks are committed with the initial
  continuation. Retries consult active instances, archives and permanent receipts.
- **Ordinary catches/receive tasks:** the bound instance, catch and token IDs must
  still match the pinned definition, element, message name and business key.
  Suspended, completed, removed or stale receivers remain unavailable. Payload
  mappings and catch end listeners run as part of the receiver candidate commit.
- **Event gateways:** the exact alternative must still belong to the current
  winner-free wait group. The committed receive removes sibling subscriptions
  by their job IDs; a competing winner cannot consume the same token again.
- **Interrupting boundaries:** the original token is replaced by a new branch;
  associated jobs are durably named for cleanup and child cancellation.
- **Non-interrupting boundaries:** the activity token and task remain. A new
  boundary catch is recorded with an occurrence marker and rearmed after commit.
  Replaying an old cleanup cannot resurrect a catch consumed by a later message.

Payloads are JSON value snapshots. File references remain references to existing
objects, not independent blob copies; shared object lifetime is separate from
the message receive transaction.

Each explicit flow has its own delivery ID. Fan-out is committed as a complete
sender outbox; receiver delivery is independent/eventual, not an atomic transaction
across all pools. There is no global FIFO guarantee. No message-name broadcast or
latest-version fallback is used. A committed outbox keeps its historical route
when the sender migrates; deleting a target definition does not redirect messages.

## Recovery and lifecycle

The dispatcher runs after execution batches, on server startup after definitions,
instances and wait states are restored, and from the server timer scheduler.
Concurrent/reentrant dispatchers coalesce. A bounded pass attempts at most 256
records and rotates its starting position so an unavailable prefix cannot starve
later ready deliveries.

An unacknowledged snapshot save retains its candidate identities in memory and
blocks continuation exposure until a required save succeeds. A restart instead
uses the persisted snapshot. The receiver's committed inbox bridges a crash
before permanent receipt creation; the permanent receipt bridges a crash before
sender acknowledgment, including after the receiver is archived or deleted.
Recovery may resume a receiver continuation from its already committed snapshot
before finalizing the permanent receipt; the persisted inbox remains its bridge.
Snapshots are checked against the instance payload limit. Malformed or unsupported
outbox records fail closed; current records use protocol version 1.

Deleting a sender with pending deliveries is rejected to prevent outbox loss.
Deleting a receiver first confirms any unacknowledged candidate snapshot and
retains its committed inbox receipts permanently.
Delivery UUIDs and receipts have no TTL. Explicit cancellation/unbinding controls
are not yet exposed through REST or the desktop UI.

## Storage and APIs

| State | Storage |
|---|---|
| Pending sender deliveries and receiver binding | Sender instance snapshot, including completed-instance archives |
| Receiver effects and temporary receipt bridge | Receiver instance snapshot |
| Permanent deduplication receipt | NATS `message_receipts` KV; equivalent in-memory adapter map |
| Route metadata | Original `definition_collections` commit, reconstructed into pinned routes |

Backends must implement `get_message_receipt` and atomic
`reserve_message_receipt`; the default port implementation fails closed. The
NATS adapter exposes the receipt and collection buckets through storage monitoring.

Engine APIs are `pending_internal_messages()` for committed pending records and
`process_internal_messages()` for explicit retry/reconciliation. Automatic send
handling is limited to nodes with explicit internal collection routes; unrouted
legacy sends retain their existing behavior. Preflight remains read-only through
`plan_internal_messages`.

## Current limits and remaining X12 work

The engine assumes one writer per store. Pending discovery scans active and
archived snapshots for correctness; a durable pending index is a future scaling
optimization. The instance and broker payload limits apply. Permanent receipts
need an explicit retention/migration policy before introducing pruning or
multi-writer coordination.

A bound catch cancelled before acceptance remains pending rather than being
redirected to another occurrence. Delivery queue administration, dedicated
history/SSE payloads, desktop collection workflows and modeler roundtrips remain
separate X12 work. The implementation does not promise exactly-once execution of
external workers or effects outside the engine's committed receive transaction.

See [the X12 plan](x12-collaboration-plan.md) for verification results and the
[architecture document](architecture.md) for component and persistence diagrams.

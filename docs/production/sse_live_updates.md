# Durable Live Updates

Favn's live-update correctness comes from PostgreSQL, not process-local PubSub.
Every run mutation commits its authoritative snapshot/event and outbox record in
one transaction. A post-commit sequencer assigns publication IDs in durable commit
order. Consumers page from those persisted publications with bounded cursors.

PostgreSQL `NOTIFY` and Phoenix PubSub only wake consumers early. Lost or duplicate
wakeups are safe because consumers always refresh from their last durable cursor.

## Scope and authorization

Private endpoints:

- `GET /api/orchestrator/v1/streams/runs`
- `GET /api/orchestrator/v1/streams/runs/:run_id`

Both require an authenticated viewer workspace context. Global-to-a-workspace run
streams include only that workspace. Run-specific streams verify that the run
belongs to the same workspace before subscribing. PubSub topics also include the
workspace identifier; a run ID is never used as an authorization boundary.

Platform-wide monitoring uses a separate explicitly authorized bounded read path,
not a customer stream.

## Replay

- Run cursor: `run:<run_id>:<sequence>`.
- Workspace publication cursor: `global:<publication_id>`.
- `Last-Event-ID` resumes after the supplied cursor.
- Each fetch is capped at 200 events.
- Malformed cursors return `400 validation_failed`.
- A cursor ahead of durable state or outside retained replay history returns
  `410 cursor_expired`.

Identity values and timestamps are not publication order. `global_sequence` in the
wire DTO is the durable publication ID.

## Frames and disconnects

Streams emit a retry hint, redacted run-event frames, `stream.ready` after replay,
and heartbeat comments. DTOs contain stable identifiers, status, timestamps,
cursor, summary, and bounded safe details; they never expose internal structs,
database URLs, tokens, cookies, SQL errors, or secrets.

Subscriptions exist only for the connection lifetime. Disconnects cancel timers
and remove PubSub subscriptions. A reconnect resumes from PostgreSQL, so node
failover does not require sticky sessions for correctness.

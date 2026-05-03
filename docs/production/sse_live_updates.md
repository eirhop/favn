# SSE Live Updates

Favn's production live-update contract is single-node first. The orchestrator is
the source of truth for run lifecycle events, SQLite persists replayable run
events, and in-memory PubSub is used only for same-node live fanout after the
persisted write succeeds.

## Endpoints

Private orchestrator endpoints:

- `GET /api/orchestrator/v1/streams/runs`
- `GET /api/orchestrator/v1/streams/runs/:run_id`

Browser-facing endpoints:

- `GET /api/web/v1/streams/runs`
- `GET /api/web/v1/streams/runs/:run_id`

Browsers should connect to the SvelteKit web endpoints. `favn_web` validates the
browser session cookie server-side and calls the private orchestrator endpoint
with its service identity plus the actor session token. The orchestrator service
token is never sent to the browser.

## Cursors and replay

Run-scoped cursors use:

```text
run:<run_id>:<sequence>
```

Global cursors use:

```text
global:<global_sequence>
```

`Last-Event-ID` is supported for both streams. Replay is capped at 200 persisted
events per stream connection. A missing run-scoped cursor replays the selected
run from the beginning when it fits inside that cap. A missing global cursor
replays the latest persisted run events up to the cap in ascending persisted
order. A valid cursor replays only events after the cursor and then continues
live when the full replay fits inside the cap.

Malformed cursors return a safe `400 validation_failed` response. Well-formed
cursors that are not known/replayable return `410 cursor_expired`. If more than
200 persisted events would need replay after a cursor, the stream also returns
`410 cursor_expired` instead of entering live mode with a replay gap.

Global ordering is based on a persisted monotonic `global_sequence` assigned
when the run event is stored. It is not wall-clock ordering.

## Stream frames

Streams send:

- `retry: 3000` at stream start.
- Redacted run-event frames with stable `id:` values matching the cursor format.
- `event: stream.ready` after replay has completed.
- `: heartbeat` comments every 15 seconds while connected.

Run-event payloads are normalized DTOs containing run identity, event type,
status, timestamp, run sequence, global sequence where available, cursor,
summary, and safe details. Streams must not expose raw internal structs, service
tokens, session tokens, cookies, stack traces, database paths, raw SQL errors,
or secret material.

## Disconnects

The orchestrator subscribes to the relevant PubSub topic only for the lifetime
of the stream. When the client disconnects or chunking fails, the stream loop
stops, heartbeat timers are cancelled, and PubSub subscriptions are removed.

## Limits

This contract is for one orchestrator node with SQLite control-plane
persistence. Distributed PubSub/replay and Postgres production guarantees are
future production-mode work.

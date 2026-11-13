# Parent Wake Transport v1

Parent Wake delivers one bounded wake event from an external process to one explicitly registered
`AgentSession`, over the daemon broker socket of one project scope. The event carries pointers only;
the target session fetches canonical state itself. It is off by default: no session registers and no
broker state is touched unless the opt-in environment below is set.

## Target (the session being woken)

Launch the session with both variables set; it registers when it starts:

| Variable | Meaning |
| --- | --- |
| `OMP_PARENT_WAKE_TARGET_SESSION_ID` | Session id to register under. Launch with `--resume <id>` so it equals the session's own id; a wake for any other id is rejected by the session. |
| `OMP_PARENT_WAKE_RUNTIME_SCOPE` | Runtime/candidate label bound to that id. A wake naming a different label fails `wrong-target`. |
| `OMP_PARENT_WAKE_BROKER_RUNTIME_DIR` | Optional absolute private runtime directory. Joins (and, if absent, starts) a broker owned by that directory instead of the shared project broker. |

Either of the first two unset means no registration. The registration is torn down (`target-stale`) when
the session adopts a different session id (fork, resume, move).

## Submitter

```sh
cat event.json | omp parent-wake --project-dir <project> [--runtime-dir <private-dir>] [--timeout <ms>]
```

`event.json` (stdin, one object):

```json
{
  "event_id": "stable-across-retries",
  "event_kind": "candidate-ready",
  "canonical_pointers": [{ "kind": "artifact", "ref": "local://report.md" }],
  "created_at": 1700000000000,
  "target": { "scope": "<project>", "parent_session_id": "<id>", "candidate_runtime_id": "<label>" },
  "producer_session_id": "optional, audit only"
}
```

Stdout is exactly one JSON line: `event_id`, `target`, `dispatch_status` (`pending|dispatched|failed`),
`ack_status` (`pending|acked|timeout`), `timestamps`, and `failure_code` when not acknowledged. The exit code
is `0` only for `ack_status: "acked"`. Transport failures (bad stdin, unreachable broker) print to stderr and
never produce a result line.

## Guarantees

- **Bounded:** ids match `[A-Za-z0-9][A-Za-z0-9._-]{0,127}`; at most 8 pointers (`kind` ≤ 64, `ref` ≤ 512 chars).
- **Strict target:** the event must match the registered session id, runtime label, and canonical project
  scope. Failures are durable and fail closed: `target-missing`, `target-dead` (registered, no live connection),
  `target-stale` (explicitly unregistered), `wrong-target`, `timeout`. Malformed events are rejected as request errors.
- **Replay-safe:** `event_id` is the dedupe key. A resubmission returns the persisted result and never wakes
  twice; an `event_id` already used for a different target is rejected.
- **Honest ACK:** the broker records `acked` only after the target session has injected the wake into its own
  queue. A session that rejects it (invalid, other session id, disposed) never acknowledges.
- **Persist before dispatch:** events and registrations are stored under `<runtime-dir>/parent-wake/` and survive
  a broker restart; a still-unacknowledged dispatch is redelivered when the target re-registers.

## Isolation

`--runtime-dir` / `OMP_PARENT_WAKE_BROKER_RUNTIME_DIR` must be absolute and outside the profile's
`run/daemons` root, so the isolated broker can neither join a live shared broker nor sit among the scopes the
dead-scope sweep reclaims. Submitter and target must name the same directory.

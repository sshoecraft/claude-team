# claude-team wire protocol

## Discovery packet (UDP multicast)

- **Group:** `239.66.83.2:7500`
- **TTL:** 1
- **Encoding:** UTF-8 JSON

```json
{
  "magic": "CLTM",
  "version": 1,
  "cluster_id": "<16 hex chars>",
  "node_id": "<hostname>-<pid>-<hex4>",
  "hostname": "<full hostname>",
  "pid": 12345,
  "nats_url": "nats://...",
  "sent_at": 1745366400.5
}
```

Receivers filter by `cluster_id` (must match their own) and ignore self-
echoes (matching `node_id`). Announce interval: 2 s. Peer timeout: 6 s.

## NATS resources (per cluster)

| Kind | Name |
|---|---|
| KV bucket | `claude-team-<cluster_id>-locks` |
| JetStream stream | `claude-team-<cluster_id>-events` |
| Object store | `claude-team-<cluster_id>-files` |

Subject prefix: `ct.<cluster_id>`. The event stream subscribes to
`ct.<cluster_id>.>`.

## KV lock entries

Key: `lock.<sha256(path)[:16]>`. Value (JSON):

```json
{
  "path": "src/foo.py",
  "holders": [
    {
      "node": "<node_id>",
      "mode": "EX" | "SHARED",
      "epoch": 1,
      "acquired_at": 1745366400.5,
      "pid": 1234,
      "hostname": "host"
    }
  ]
}
```

Concurrency: all updates go through KV `update(key, value, last=rev)`
(optimistic CAS). Initial claim uses `create(key, value)` which fails if
the key already exists.

## Event stream subjects

All events share an envelope:

```json
{
  "type": "<event type>",
  "path": "<canonical relative path>",
  "sender": "<node_id>",
  ...type-specific fields
}
```

| Subject | Type | Payload fields |
|---|---|---|
| `ct.<id>.checkpoint` | `checkpoint` | `{cluster_id, checkpoint_number, checkpoint_id, created_at, created_by, message, object_store_bucket}` |
| `ct.<id>.claim.<path_hash>` | `claim` | `{mode}` |
| `ct.<id>.release.<path_hash>` | `release` | `{}` |
| `ct.<id>.diff.<path_hash>` | `diff` | `{pre_sha256, post_sha256, format, payload}` |
| `ct.<id>.create.<path_hash>` | `create` | `{sha256, inline, content}` |
| `ct.<id>.delete.<path_hash>` | `delete` | `{pre_sha256}` |
| `ct.<id>.rename.<path_hash>` | `rename` | `{old, new, sha256}` |

- `path_hash` = `sha256(path)[:16]`.
- `format` = `"unified"` | `"replaced"` | `"binary_inval"`.
- Files > 1 MB (configurable via `max_diff_size`) or binary files use
  `binary_inval`; full contents are stored in the object store keyed by
  `post_sha256` for receivers to fetch.

## Checkpoint files (on disk)

Both live under `.claude-team/` at the project root.

`checkpoint.json`:

```json
{
  "cluster_id": "0c56b33af1b2b100",
  "checkpoint_number": 1,
  "checkpoint_id": "<sha256 of sorted manifest>",
  "created_at": 1745366400.5,
  "created_by": "<node_id>",
  "message": "optional"
}
```

`manifest.json`: `{relative_path: sha256_hex}` for all tracked files.

Users commit these files to their VCS so checkpoints propagate to peers.
Claude-team does not touch `.git/` or know about branches.

## Local IPC (hook ↔ server)

Socket path: `~/.claude-team/runtime/<sha256(project_root)[:16]>.sock`.

Line-delimited JSON: one request object, newline, response object,
newline. Commands: `claim`, `release`, `status`, `peers`.

Request:

```json
{"command": "claim", "args": {"path": "src/foo.py", "mode": "exclusive", "timeout_ms": 15000}}
```

Response:

```json
{"ok": true, "data": {...}, "error": null}
```

# claude-team operator guide

## Platform

Linux and macOS only. Windows is not supported. Run claude-team in WSL
on Windows if you must.

## Running NATS

Claude-team does not ship or auto-launch nats-server. Pick one:

- **Local dev:** `brew install nats-server && nats-server -js`. Config
  defaults to `nats://localhost:4222`, which claude-team also defaults to.
- **Team server:** run nats-server on a LAN-accessible host:
  ```
  nats-server -js -a 0.0.0.0 -p 4222
  ```
  Every developer sets `CLAUDE_TEAM_NATS_URL=nats://<host>:4222`.
- **Docker:** `docker run -p 4222:4222 -p 8222:8222 nats:latest -js`.

JetStream must be enabled (`-js` flag). Claude-team auto-creates its
stream, KV bucket, and object store on first use.

## Configuration

Precedence (highest first): explicit arg → env var → project
`.claude-team/config.json` → user `~/.claude-team/config.json` →
defaults.

```json
{
  "nats_url": "nats://localhost:4222",
  "discovery_port": 7500,
  "claim_timeout_ms": 120000,
  "max_diff_size": 1048576,
  "shared_filesystem": false,
  "log_level": "INFO",
  "cluster_name_override": null
}
```

Environment variables (all `CLAUDE_TEAM_*`):

| Var | Field |
|---|---|
| `CLAUDE_TEAM_NATS_URL` | nats_url |
| `CLAUDE_TEAM_DISCOVERY_PORT` | discovery_port |
| `CLAUDE_TEAM_CLAIM_TIMEOUT_MS` | claim_timeout_ms |
| `CLAUDE_TEAM_MAX_DIFF_SIZE` | max_diff_size |
| `CLAUDE_TEAM_SHARED` | shared_filesystem (bool) |
| `CLAUDE_TEAM_CLUSTER` | cluster_name_override |
| `CLAUDE_TEAM_LOG_LEVEL` | log_level |

## Installing the plugin

From a local clone:

```
claude plugin install ./plugin
```

This wires:
- MCP server `claude-team` (stdio, `claude-team-mcp`)
- PreToolUse hook on `Edit|Write|NotebookEdit|MultiEdit` → `claude-team hook pre`
- PostToolUse hook on the same matchers → `claude-team hook post`

`claude-team-mcp` and `claude-team` must be on `PATH`. Install the
Python package with `pip install -e .` from the project root, or add
`<project>/.venv/bin` to your PATH.

## Distributing checkpoints

Claude-team writes `.claude-team/checkpoint.json` and `.claude-team/manifest.json`
on checkpoint creation. Commit these files in your repository so peers
receive them through normal `git pull`. Add anything claude-team-specific
you want excluded to `.claude-team/ignore` (claude-team's own ignore
list; unrelated to `.gitignore`).

## Divergence handling

If a peer's local state doesn't match the cluster's checkpoint, startup
refuses with a clear message. Options:

- **Fetch newer checkpoint from somewhere** (usually `git pull`) and retry.
- **Start `claude-team-mcp --new-cluster`** — founds a new cluster from
  the local state, discarding the existing cluster's overlay.
- **Start `claude-team-mcp --bump-cluster`** — publishes local checkpoint
  as the new cluster checkpoint (when your local is newer).
- **Start `claude-team-mcp --accept-cluster`** — destructively overwrite
  local files with the cluster's state on replay.

## Shell-driven edits (sed, redirects, etc.)

Claude Code's `Bash` tool and any commands it runs (`sed -i`, `>`,
`>>`, `mv`, `rm`, editor-outside-Claude, `git pull`, etc.) **bypass**
the DLM — no claim is taken, no mutual exclusion is enforced for
Bash-driven writes. To still keep peers in sync, the MCP server runs a
filesystem watcher that hashes changed files and publishes overlay
events. Inotify/FSEvents are event-driven; idle cost is negligible.

If you have a very large project, tune `.claude-team/ignore` so
high-churn build dirs (`node_modules/`, `target/`, `dist/`, `build/`)
are excluded. Linux inotify has a per-user watch limit (default 8192)
— `sysctl fs.inotify.max_user_watches=524288` raises it.

## Troubleshooting

- `peers` returns empty after startup → check multicast: `tcpdump -i any
  udp port 7500`. LAN may block multicast; run with
  `CLAUDE_TEAM_DISCOVERY_PORT=<port>` to try a different port or use a
  directly-accessible NATS URL so peers can coordinate even if UDP
  discovery fails.
- `claim` times out even on a quiet cluster → inspect the KV bucket
  with `nats kv ls claude-team-<cluster_id>-locks` to find stale entries,
  or restart nats-server to clear state.
- Hook blocks Claude Code too long → the default hook timeout is 5s
  (most uncontended claims resolve in milliseconds). If a peer is
  legitimately mid-edit the hook exits with a structured "held by X"
  message and Claude surfaces options to the user. Reduce further via
  the CLI if 5s still feels long.
- Watcher not picking up changes → on Linux, check `sysctl
  fs.inotify.max_user_watches`; on macOS, FSEvents coalesces under
  heavy load (rare in normal dev). Running `claude-team verify` rehashes
  and reports drift.

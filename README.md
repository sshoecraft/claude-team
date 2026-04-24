# claude-team

Coordination layer for multiple [Claude Code](https://claude.ai/code)
instances editing the same project.

When two or more developers (or two Claude sessions on one machine) work
on the same codebase at the same time, claude-team keeps them from
stepping on each other: one holds an exclusive lock on a file, others
block or get told who's editing it. Completed edits propagate to every
peer's filesystem through an event log, so all checkouts stay in sync in
real time. Edits outside Claude (shell commands, manual editors, `git
pull`) are replicated too via a filesystem watcher.

Exposed to Claude Code as an MCP server, with plug-in hooks that
auto-claim on every Edit/Write tool call so the coordination is
deterministic — not dependent on Claude remembering to call the tool.

## Architecture

```
Developer laptop                   Alice's laptop            Bob's laptop
┌─────────────────────────────┐
│ Claude Code                 │
│   ↓ MCP stdio               │
│ claude-team-mcp             │ ←──── NATS TCP ────→ [NATS server] ←────→
│   ↑ Unix socket             │
│ claude-team CLI ◄── hook    │
└─────────────────────────────┘
         ↑ UDP multicast :7500 discovery
```

Three layers, each doing one thing:

1. **Discovery** — UDP multicast on `239.66.83.2:7500` finds peers in
   the same cluster (loopback + LAN).
2. **NATS JetStream** (user-run) — KV bucket holds the lock table (CAS
   semantics), an event stream carries the overlay (claim / release /
   diff / create / delete / rename), and an object store holds file
   contents for late joiners and `resync`.
3. **MCP server** — one per Claude Code session. Exposes coordination
   tools to Claude and hosts a local Unix socket so the hook CLI can
   claim/release before and after tool calls.

## Requirements

- **Linux or macOS.** Windows is not supported — run in WSL if you must.
- Python 3.11+
- A reachable `nats-server` with JetStream enabled (run your own; the
  package does not ship or auto-launch one).
- Claude Code itself, for the MCP integration.

## Quick start

```bash
# 1. Get NATS running somewhere (local for dev).
brew install nats-server   # or grab a release binary
nats-server -js &

# 2. Install the package.
git clone https://github.com/sshoecraft/claude-team.git
cd claude-team
python3 -m venv .venv && source .venv/bin/activate
pip install -e .

# 3. Point claude-team at your NATS server.
export CLAUDE_TEAM_NATS_URL=nats://localhost:4222

# 4. Install the Claude Code plugin (wires up the MCP server + hooks).
claude plugin install ./plugin
```

Open Claude Code in a project. On first run, claude-team founds a new
cluster from your current filesystem state and writes
`.claude-team/checkpoint.json` + `.claude-team/manifest.json`. Commit
those files to your repo so teammates on other machines receive the
cluster definition through their normal `git pull`.

## Configuration

Precedence (highest first): explicit arg → env var →
`.claude-team/config.json` in the project → `~/.claude-team/config.json`
→ built-in defaults.

| Env var | Default | Description |
|---|---|---|
| `CLAUDE_TEAM_NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `CLAUDE_TEAM_DISCOVERY_PORT` | `7500` | UDP multicast port |
| `CLAUDE_TEAM_CLAIM_TIMEOUT_MS` | `30000` | Default claim wait |
| `CLAUDE_TEAM_MAX_DIFF_SIZE` | `1048576` | Max inline diff payload (1 MB) |
| `CLAUDE_TEAM_SHARED` | `false` | Shared-filesystem mode (don't apply peer diffs locally) |
| `CLAUDE_TEAM_CLUSTER` | auto | Override cluster name |

## MCP tools

Exposed to Claude via the MCP server:

| Tool | Purpose |
|---|---|
| `peers` | Active peers in this cluster |
| `claim` | Lock one or more paths (blocking with timeout) |
| `release` | Drop claims |
| `status` | Self state + cluster checkpoint |
| `recent_changes` | Overlay events filtered by path |
| `checkpoint` | Take a new checkpoint, rotate the stream |
| `force_claim` | Preempt a held lock; logged with audit trail |
| `verify` | Rehash local files, report drift |
| `resync` | Overwrite a local file with cluster's authoritative content |

Same operations are available from the shell via `claude-team claim`,
`claude-team status`, `claude-team force-claim`, etc.

## How coordination happens

The plugin's PreToolUse hook fires before every `Edit / Write /
NotebookEdit / MultiEdit` tool call. It runs `claude-team hook pre`,
which takes an EXCLUSIVE lock on the file Claude is about to edit. If
another peer holds it, the call blocks up to 5 seconds (covering "the
other peer is just finishing"); if still held, the hook exits with a
structured message telling Claude who holds it — Claude surfaces that
to the user, who picks *wait longer* / *force* / *skip*.

PostToolUse fires after the tool completes. It publishes the diff
against the pre-edit snapshot and releases the lock. Peers subscribed
to the event stream receive the diff, verify the pre-hash matches their
local file, and apply the unified diff.

Shell-driven edits (`sed -i`, `>`, `mv`, editor outside Claude, `git
pull`) are not locked — we can't tell from a shell command what paths
will change — but they are *replicated* via a continuous filesystem
watcher running inside the MCP server. Hashes are deduped against
Claude's own edits so nothing double-publishes.

## Checkpoints

Claude-team never reads `.git/`. But the `.claude-team/checkpoint.json`
and `.claude-team/manifest.json` files are committed to your repo by
convention, which is how checkpoints travel between machines — git
pull, rsync, whatever. Claude-team is unaware of the transport.

`checkpoint` MCP tool captures the current filesystem as a new
checkpoint, bumps the number, writes both files, and rotates the
overlay event stream so future events start from the new baseline.

## Limitations

- **Bash-driven edits don't have mutual exclusion.** Only replication.
  If two peers `sed -i` the same file at the same time, last writer
  wins. Coordinated edits must go through Claude's Edit/Write tools.
- **Checkpoint matching is byte-exact.** Line-ending differences
  (CRLF vs LF) between platforms produce mismatches. Configure git
  consistently across the team.
- **Case-sensitivity.** Mixed case-sensitive/case-insensitive
  filesystems can see phantom divergence. Edge case.
- **No peer-to-peer secrets for NATS.** Use NATS TLS + auth if your
  NATS server is reachable from untrusted networks.

## Docs

- [docs/architecture.md](docs/architecture.md) — module-by-module design
- [docs/protocol.md](docs/protocol.md) — wire formats, subject layout,
  KV schema
- [docs/operator.md](docs/operator.md) — running NATS, configuration,
  troubleshooting

## Development

```bash
# Unit tests (no external deps).
pytest tests/

# Integration tests (require a live NATS).
CLAUDE_TEAM_NATS_URL=nats://localhost:4222 pytest tests/test_integration_nats.py
```

## License

MIT — see [LICENSE](LICENSE).

# Isaac Remote-CLI Wire Protocol

The contract between **isaac-cli-server** (the `/cli` WebSocket endpoint) and
**isaac-cli-proxy** (the `isaac remote …` client). One pipe, two ends — keep
this file in lockstep across both repos.

The shape generalizes the ACP `/acp` route + `acp --remote` proxy: a WebSocket,
authenticated at the HTTP upgrade, carrying framed IO — but **command-agnostic**.
Commands execute **in-process on a server worker thread** with framed IO. The
server spawns nothing: the wire is the only boundary.

## Transport

- **WebSocket** at `GET /cli` on the isaac server.
- **Auth at the HTTP upgrade**, before the socket is accepted — bearer token,
  reusing the server's existing auth (the same check `/acp` uses). An
  unauthenticated upgrade is rejected with `401` and no socket.
- After upgrade, every frame is a **JSON text message**, one JSON object per
  frame. Binary payloads (stdin/stdout/stderr bytes) are **base64-encoded** in
  the `data` field — keeps the framing uniform and binary-safe. (Optimization to
  binary frames is possible later; M1 stays JSON+base64.)

## Lifecycle

```
client                                   server
  │  ── upgrade (Authorization: Bearer …) ──▶  auth; accept or 401
  │  ── {"type":"start","argv":[…],"stdout-tty":true} ─▶  dispatch <argv…> on a server thread
  │  ◀── {"type":"start-ack","stream-id":"…"}  resumable stream id
  │  ◀── {"type":"stdout","data":"…"} ──────  (0..N, streamed as produced)
  │  ◀── {"type":"stderr","data":"…"} ──────  (0..N, streamed, separate)
  │  ── {"type":"stdin","data":"…"} ───────▶  (0..N, interactive)
  │  ── {"type":"stdin-close"} ────────────▶  EOF to the command's stdin
  │  ── socket drops ───────────────────────▶  server keeps the command for grace window
  │  ── {"type":"attach","stream-id":"…"} ─▶  replay buffered frames; resume live stream
  │  ◀── {"type":"exit","code":N} ──────────  terminal; server closes socket
```

## Messages

### Client → Server

- **start** (first frame on a fresh socket, required):
  `{"type":"start","argv":["prompt","-m","hi"],"stdout-tty":true}`
  - `argv` — the command + args to run (NOT including `isaac`).
  - `stdout-tty` — optional boolean hint from the proxy that its local stdout is
    a terminal. When true, the server may force terminal-oriented formatting
    (for example ANSI color) for that command even though the server-side
    transport is a pipe.
  - An **empty `argv`** requests usage: the server replies with usage text on
    `stdout` and `exit 0`.
- **attach** (first frame on a replacement socket):
  `{"type":"attach","stream-id":"abc123"}`
  - `stream-id` — identifier previously issued by `start-ack`.
  - Reattaches to a still-live command inside the grace window.
  - Server replays buffered frames produced while detached, then resumes live streaming.
- **stdin**: `{"type":"stdin","data":"<base64>"}` — bytes for the command's stdin.
- **stdin-close**: `{"type":"stdin-close"}` — closes the command's stdin (EOF).

### Server → Client

- **start-ack**: `{"type":"start-ack","stream-id":"abc123"}`
  - Sent once per started command.
  - `stream-id` is stable across reconnects for that command's lifetime.
- **stdout**: `{"type":"stdout","data":"<base64>"}`
- **stderr**: `{"type":"stderr","data":"<base64>"}` (kept distinct from stdout)
- **exit** (terminal): `{"type":"exit","code":N}` — the command's exit code.
  The server then closes the socket. The proxy exits its own process with `N`.
- **error**: `{"type":"error","message":"…"}` — a protocol/auth/dispatch failure
  with no command exit code (e.g. malformed handshake, unknown `stream-id`).
  Terminal; server closes.

## Execution model (server)

A server-side detail, not part of the wire contract — the frames above are the
same either way.

Every command runs **embedded**: the handshake `argv` is dispatched against the
server's live command registry on a server thread, with `stdin`/`stdout`/`stderr`
bound to this socket's frames. The server spawns no subprocess, so a command's
sessions are created and written through the server's own store under its
persist lock — one writer, no second process racing the files it is mid-turn on.

- The client never names a binary. It supplies `argv` only, and `argv` selects a
  command from the registry — never a program on disk.
- `System/exit`, stdio, env, cwd, tty, and shutdown hooks are mediated by the CLI
  host (`isaac.cli.host`), so an exiting or throwing command is contained and the
  server keeps serving.
- Commands marked `:local-only` in their module manifest (`server`, `service`,
  `modules install|upgrade`, `remote`) are **refused over the pipe** with
  `run this on the host` and `exit 2`. SSH plus the cold `isaac` binary is the
  escape hatch — there is no client-selectable "run as a process" flag.
- A `--root` that is not the server's own root is refused with `exit 2`.
- Each start is logged `:cli/command-started` with `hosted true`; each finish is
  logged `:cli/command-finished` with `code` and `duration-ms`.
- A command exceeding `:cli-server :timeout-ms` is cancelled and reported as
  `exit 124`.

## Reconnect and grace window

- The socket stays **full-duplex open** until the command exits — long-lived
  commands (acp, chat) stream both directions for the whole session.
- When the socket drops after `start`, the server **does not immediately cancel**
  the command. It starts a **grace window** timer.
- While detached, the server buffers `stdout`, `stderr`, and terminal `exit`
  frames for that `stream-id`.
- If the client reattaches before grace expiry, the server replays buffered
  frames exactly once, then resumes live delivery on the new socket.
- If grace expires first, the server cancels the command — running its shutdown
  hooks — and drops the buffer.
- A server restart drops every live stream. A reattach for a `stream-id` the
  server no longer knows gets `{"type":"error"}` (`unknown stream-id`) and the
  proxy exits; the client reconnects with a fresh `start`.
- Proxy UX: status text is written to **stderr only** — e.g.
  `isaac remote: connection lost, reconnecting...` and
  `isaac remote: reattached`.

## Errors & exit codes

- Auth failure → `401` at upgrade (no frames).
- Bad/missing `start` or bad `attach` → `{"type":"error"}` then close.
- Command runs and exits → `{"type":"exit","code":N}`; `N` is the real code.
- The proxy's process exit code MUST equal the server's reported `code`.

## Milestones this protocol serves

- **M1** — handshake + `stdout` streaming + `exit` (batch round-trip).
- **M2** — `stdin` + `stderr` separation + nonzero exits.
- **M3** — interactive full-duplex hold-open + reconnect/resume + auth hardening.

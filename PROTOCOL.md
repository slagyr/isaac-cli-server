# Isaac Remote-CLI Wire Protocol

The contract between **isaac-cli-server** (the `/cli` WebSocket endpoint) and
**isaac-cli-proxy** (the `isaac remote …` client). One pipe, two ends — keep
this file in lockstep across both repos.

The shape generalizes the ACP `/acp` route + `acp --remote` proxy: a WebSocket,
authenticated at the HTTP upgrade, carrying framed IO — but **command-agnostic**.
The server runs the real isaac launcher as a subprocess with the client's argv
and pipes process IO back.

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
  │  ── {"type":"start","argv":[…]} ───────▶  spawn isaac <argv…> (subprocess)
  │  ◀── {"type":"start-ack","stream-id":"…"}  resumable stream id
  │  ◀── {"type":"stdout","data":"…"} ──────  (0..N, streamed as produced)
  │  ◀── {"type":"stderr","data":"…"} ──────  (0..N, streamed, separate)
  │  ── {"type":"stdin","data":"…"} ───────▶  (0..N, interactive)
  │  ── {"type":"stdin-close"} ────────────▶  EOF to the command's stdin
  │  ── socket drops ───────────────────────▶  server keeps subprocess for grace window
  │  ── {"type":"attach","stream-id":"…"} ─▶  replay buffered frames; resume live stream
  │  ◀── {"type":"exit","code":N} ──────────  terminal; server closes socket
```

## Messages

### Client → Server

- **start** (first frame on a fresh socket, required):
  `{"type":"start","argv":["prompt","-m","hi"]}`
  - `argv` — the command + args to run (NOT including `isaac`).
  - An **empty `argv`** requests usage: the server replies with usage text on
    `stdout` and `exit 0`.
- **attach** (first frame on a replacement socket):
  `{"type":"attach","stream-id":"abc123"}`
  - `stream-id` — identifier previously issued by `start-ack`.
  - Reattaches to a still-live subprocess inside the grace window.
  - Server replays buffered frames produced while detached, then resumes live streaming.
- **stdin**: `{"type":"stdin","data":"<base64>"}` — bytes for the command's stdin.
- **stdin-close**: `{"type":"stdin-close"}` — closes the command's stdin (EOF).

### Server → Client

- **start-ack**: `{"type":"start-ack","stream-id":"abc123"}`
  - Sent once per started subprocess.
  - `stream-id` is stable across reconnects for that subprocess lifetime.
- **stdout**: `{"type":"stdout","data":"<base64>"}`
- **stderr**: `{"type":"stderr","data":"<base64>"}` (kept distinct from stdout)
- **exit** (terminal): `{"type":"exit","code":N}` — the command's exit code.
  The server then closes the socket. The proxy exits its own process with `N`.
- **error**: `{"type":"error","message":"…"}` — a protocol/auth/spawn failure
  with no process exit code (e.g. malformed handshake, unknown `stream-id`).
  Terminal; server closes.

## Execution model (server)

Always **subprocess per connection** — clean isolation for long-lived and
streaming commands, real stdio pipes, and containment for `System/exit` or
other process-level failures. The handshake `argv` is run as `isaac <argv…>`.
The client never chooses the binary; it only supplies args to the implied
`isaac` launcher.

## Reconnect and grace window

- The socket stays **full-duplex open** until the command exits — long-lived
  commands (acp, chat) stream both directions for the whole session.
- When the socket drops after `start`, the server **does not immediately kill**
  the subprocess. It starts a **grace window** timer.
- While detached, the server buffers `stdout`, `stderr`, and terminal `exit`
  frames for that `stream-id`.
- If the client reattaches before grace expiry, the server replays buffered
  frames exactly once, then resumes live delivery on the new socket.
- If grace expires first, the server destroys the subprocess and drops the buffer.
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

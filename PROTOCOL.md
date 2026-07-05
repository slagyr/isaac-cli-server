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
  │  ◀── {"type":"stdout","data":"…"} ──────  (0..N, streamed as produced)
  │  ◀── {"type":"stderr","data":"…"} ──────  (0..N, streamed, separate)
  │  ── {"type":"stdin","data":"…"} ───────▶  (0..N, interactive)
  │  ── {"type":"stdin-close"} ────────────▶  EOF to the command's stdin
  │  ◀── {"type":"exit","code":N} ──────────  terminal; server closes socket
```

## Messages

### Client → Server

- **start** (first frame, required):
  `{"type":"start","argv":["prompt","-m","hi"]}`
  - `argv` — the command + args to run (NOT including `isaac`).
  - An **empty `argv`** requests usage: the server replies with usage text on
    `stdout` and `exit 0`.
- **stdin**: `{"type":"stdin","data":"<base64>"}` — bytes for the command's stdin.
- **stdin-close**: `{"type":"stdin-close"}` — closes the command's stdin (EOF).

### Server → Client

- **stdout**: `{"type":"stdout","data":"<base64>"}`
- **stderr**: `{"type":"stderr","data":"<base64>"}` (kept distinct from stdout)
- **exit** (terminal): `{"type":"exit","code":N}` — the command's exit code.
  The server then closes the socket. The proxy exits its own process with `N`.
- **error**: `{"type":"error","message":"…"}` — a protocol/auth/spawn failure
  with no process exit code (e.g. malformed handshake, command failed to start).
  Terminal; server closes.

## Execution model (server)

Always **subprocess per connection** — clean isolation for long-lived and
streaming commands, real stdio pipes, and containment for `System/exit` or
other process-level failures. The handshake `argv` is run as `isaac <argv…>`.
The client never chooses the binary; it only supplies args to the implied
`isaac` launcher.

## Interactive & reconnect

- The socket stays **full-duplex open** until the command exits — long-lived
  commands (acp, chat) stream both directions for the whole session.
- **Reconnect** (proxy side) is a resilience concern, NOT part of M1. A dropped
  socket mid-command cannot be naively replayed (re-running re-executes). M3
  defines resumable-session semantics (carry over the ACP proxy's reconnect /
  stdin-serialization work). Until then, a drop ends the invocation and the
  server destroys the subprocess.

## Errors & exit codes

- Auth failure → `401` at upgrade (no frames).
- Bad/missing `start` → `{"type":"error"}` then close.
- Command runs and exits → `{"type":"exit","code":N}`; `N` is the real code.
- The proxy's process exit code MUST equal the server's reported `code`.

## Milestones this protocol serves

- **M1** — handshake + `stdout` streaming + `exit` (batch round-trip).
- **M2** — `stdin` + `stderr` separation + nonzero exits.
- **M3** — interactive full-duplex hold-open + reconnect/resume + auth hardening.

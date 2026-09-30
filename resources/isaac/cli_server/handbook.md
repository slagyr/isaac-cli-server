# isaac.cli-server — remote CLI execution over `/cli`

You are a crew running inside Isaac. This chapter covers what
**isaac-cli-server** owns: the `/cli` WebSocket door that lets a remote
client run an `isaac` command against *this* server's own process — the
same command registry, config, and session store a local invocation would
use — instead of spawning anything. Foundation's introduction chapter
(`handbook__read` topic `isaac.foundation`) covers config mechanics and the
vocabulary table; read it first if you haven't. isaac-http's chapter
(topic `isaac.http`) covers the auth gate every request, including this
one, passes through before a handler runs, and how a principal's scopes
are minted — read its Inbound auth section before this chapter's Scopes
section, below. This chapter's own topic id is `isaac.cli-server`.

isaac-cli-server does not spawn a subprocess and does not know what a
command *does* — it dispatches the client's argv into the same embedded
CLI host a local `isaac` invocation runs on, and pipes whatever that
command reads from stdin and writes to stdout/stderr back over the
socket. The client side of this pipe lives in **isaac-cli-proxy** (topic
`isaac.cli-proxy`), the module behind the `remote` command; foundation's
own remote-routing appendix (`isaac.foundation#appendix-the-cli-and-remote-routing`)
covers when an *ordinary* `isaac` invocation is transparently redirected
here instead of running locally. Neither is re-documented in this chapter.

## The `/cli` endpoint

**What it is.** `GET /cli` upgrades to a WebSocket, authenticated at the
HTTP upgrade by isaac-http before this module's handler ever runs. Once
open, the client sends a `start` frame with `argv` (the command and its
arguments, not including `isaac` itself); the server runs that argv on a
server worker thread and streams `stdout`/`stderr` frames back, followed
by a terminal `exit` frame carrying the command's real exit code. A
command's session and store writes go through the server's own process,
under its own persist lock — there is no second process racing the files
a hosted command is mid-turn on. The wire contract (frame shapes,
reconnect, milestones) is a separate reference; this chapter covers the
operational surface only.

**How to change it.** The route itself is code (a manifest contribution),
not config — there is no `handbook__configure` path to add, remove, or
move it. The one runtime knob this module owns is the wall-clock timeout
applied to every hosted command:

```
config set cli-server.timeout-ms 30000
config unset cli-server.timeout-ms
```

`config:cli-server.timeout-ms` defaults to no timeout (omit it, or unset
it, for a hosted command that may run indefinitely). A command that
exceeds it is cancelled and reported as `exit 124`.

**How to verify.** `isaac modules show isaac.cli-server` lists this
module's route contribution. Every hosted command logs
`:cli/command-started` (info) at the start, with `argv`, `stream-id`,
`hosted true`, and the principal's name if the request carried one, and
`:cli/command-finished` (info) at the end, with `argv`, `stream-id`,
`duration-ms`, and either `code` or a `reason` (`abandoned-stream`,
`grace-window-expired`) when it finished without a code — read these on
the `cli` log stream (`isaac logs cli`).

### Troubleshooting

- **A hosted command never seems to return.** Check
  `config:cli-server.timeout-ms` — an unset timeout means no timeout, so
  a genuinely hung command hangs the stream until the client disconnects
  (the grace window below then takes over).
- **The socket upgrade itself is refused with `401`/`403` before any
  frame arrives.** That's isaac-http's auth gate, not this module —
  see `isaac.http#inbound-auth-principals`.
- **`GET /cli` 404s.** Confirm isaac-cli-server is actually installed
  (`isaac modules show isaac.cli-server`); a 404 means no route matched
  at all, the same rule as any other door on this server.

## Scopes: opening the socket vs. running a command

**What it is.** Two separate checks gate a hosted command, at two
different layers. First, isaac-http's own auth gate: the route declares
`:scope :cli` (no namespace), so — per isaac-http's namespace-relaxation
rule — any principal holding `:*` or *any* `cli/…` scope can open the
socket at all; see `isaac.http#inbound-auth-principals` for that rule in
general. Second, once inside, isaac-cli-server enforces its own
**per-command** scope before a command runs: the principal must hold
`:*`, the exact scope `:cli` (every command), or the exact scope
`cli/<command>` naming the specific command being started. `<command>` is
the registry name of the argv's first word once any `--root <path>` flag
is stripped — `cli/http` for any `isaac http …` invocation, `cli/logs`
for `isaac logs`; a command's subcommands are not separate scopes, and
holding one command's scope grants nothing for a different command
(`cli/http` does not run `logs`). A refusal happens over the wire, after
`start-ack`: stderr names the missing scope (`requires cli/<command>`),
the stream exits `77`, and the refusal is logged `:warn :cli/refused-scope`
with the principal's name and the attempted argv — the command never
starts. Asking for usage (empty argv, or a bare `--help`/`-h`) always
succeeds with exit `0` regardless of which narrow scope a principal
holds; it isn't treated as running a command. A request with no
principal at all (an unauthenticated/open server, or a test harness) is
never scope-filtered by this module — that mirrors isaac-http's own
"server runs open" condition.

**How to change it.** Minting and rotating principals, and the scopes
they hold, is entirely isaac-http's surface — see
`isaac.http#inbound-auth-principals` for the full flow. The scope *names*
this module recognizes are `cli`, `cli/<command>` (per registry command
name), and the wildcard `:*`:

```
isaac http auth mint laptop --scopes cli
isaac http auth mint laptop --scopes cli/logs
```

The first grants every hosted command; the second, only `isaac logs …`.
Everything minted lands under `config:http.auth.principals`, inspectable
the same way as any other config.

**How to verify.** `isaac http auth list` prints each principal's held
scopes. `isaac logs cli` shows the principal name on every
`:cli/command-started` entry and on every `:warn :cli/refused-scope`
refusal.

### Troubleshooting

- **A principal gets `requires cli/<command>` on stderr and exit `77`.**
  Their held scopes don't include `:*`, `:cli`, or that exact
  `cli/<command>` — holding a *different* `cli/<other>` scope, even one
  that was enough to open the socket, does not carry over. Mint or
  rotate the principal with the scope the command actually needs.
- **A principal can open the socket but every command it sends is
  refused.** They likely hold a narrow `cli/<command>` scope for a
  command other than the one in the argv they're sending — check the
  argv's first word against the scope name.
- **`cli/read` doesn't work as a scope.** It was retired — read-vs-write
  is not part of this module's scope model (see the `:read-only`
  manifest hint under Stale module basis, below, which is unrelated to
  auth). Name the actual command instead.

## Local-only commands and `--root`

**What it is.** A command whose manifest sets `:local-only true` — for
example `server`, `service`, `modules`, and the `remote` command itself —
can never run over `/cli`, at any scope: it fails immediately with
`<command> is local-only; run this on the host` on stderr and exit code
`2`. This keeps a way to recover the host available even when the remote
door itself is the thing that's broken. SSH to the host, or the cold
`isaac` binary there, is the only way to run one of these. Separately, a
`--root` flag naming anything other than the server's own root is refused
the same way — `--root must match the server root <path>`, exit `2` — so
a remote client can't point the hosting server at a different install.

**How to change it.** Not from here. `:local-only` is a manifest
declaration owned by whichever module registers the command; there is no
`handbook__configure` path to waive it for a specific principal or
scope — a scope can't override it.

**How to verify.** Send the command and read the exit code: `2` with
either message identifies this refusal, distinct from the scope refusal
above (`77`) and the stale-basis refusal below (`75`).

### Troubleshooting

- **A principal scoped for the command still gets "run this on the
  host."** The command is local-only; no scope grants it over the pipe,
  by design.
- **A `--root` other than the server's own fails outright rather than
  quietly running against the wrong install.** That's the intended
  isolation, not a bug.

## Grace window: a dropped socket doesn't kill the command

**What it is.** When the client's socket drops while a command is still
running, the server does not cancel it immediately. It keeps the command
alive for a short grace window (2 seconds by default), buffering its
`stdout`/`stderr`/terminal `exit` frames rather than delivering them. If
the client reconnects and sends `attach` with the previously-issued
`stream-id` before the window elapses, the server replays every buffered
frame exactly once and then resumes live delivery on the new socket. If
the window elapses first, the command is cancelled — its shutdown hooks
run — and the buffer is dropped; a later `attach` for that `stream-id`
gets a terminal `{"type":"error"}` frame, and the client has to `start` a
fresh command. A server restart drops every live stream the same way,
immediately, for all of them at once.

**How to change it.** Not exposed as config today — the grace period is
fixed in code, not a `config:` path `[verify: worth a config key if
operators need a longer window for flaky reconnects — flagging for
Micah rather than assuming]`.

**How to verify.** Disconnect mid-command and reattach within the
window; the reattached client should receive the buffered output.
`isaac logs cli` records `:cli/command-finished` with
`:reason :grace-window-expired` when the window lapses before a
reattach, or `:reason :abandoned-stream` when the command finished
naturally while nobody was attached to see it.

### Troubleshooting

- **A reattach gets `{"type":"error","message":"unknown stream-id: …"}"`.**
  The grace window already elapsed (or the server restarted) — start a
  fresh command; there's nothing left to resume.
- **A long-running interactive command drops on a flaky connection.**
  Reattach quickly. The window is short by design; once it fires, the
  command is gone for good.

## Stale module basis: mutating commands wait for a restart

**What it is.** The server compares the module basis it loaded at boot
against the basis on disk (foundation version plus installed module
versions). Ordinary config edits never trip this — those hot-reload, the
same as anywhere else in Isaac. Only a classpath-level change (a
foundation upgrade, an installed module's version bump) needs a process
restart to actually take effect, and until that restart happens, any
hosted command **not** marked `:read-only` in its manifest is refused:
stderr `server restart pending — restart the server, or run with
--local`, exit `75`, logged `:warn :cli/refused-stale-basis`. A command
whose manifest marks it `:read-only` — either `true` for the whole
command, or a set of specific subcommand names — keeps working during
the wait, on the premise that a read is safe to run against a slightly
stale classpath. This check runs independently of, and before, the scope
check above; a scope refusal and a stale-basis refusal never both apply
to the same attempt.

**How to change it.** Not a config knob — restart the `isaac server`
process to clear it, or run the command locally on the host (`--local`)
instead of over `/cli` in the meantime.

**How to verify.** `isaac logs cli` shows `:cli/refused-stale-basis`
entries while the condition is active; the same command succeeds again
once the server has restarted onto the current basis.

### Troubleshooting

- **Every command over `/cli` fails with "restart pending" right after a
  module upgrade.** Expected — restart `isaac server`, or use `--local`
  on the host for anything urgent in the meantime.
- **A read-only command keeps working but a mutating one doesn't, during
  the same window.** Intentional — only the manifest's `:read-only` hint
  (or its listed read-only subcommands) is exempt from the restart-pending
  refusal.

Feature: /cli WebSocket endpoint
  Handler-level tests for isaac.cli-server.ws/handler. Auth is enforced by
  isaac-http before the handler runs; not asserted here.

  Scenario: a batch command streams stdout and exits zero
    Given the cli-server handler with spawn command "echo isaac"
    When a /cli client sends start with argv ["--version"]
    Then the handler sends frames:
      | type   | data            | code |
      | stdout | #".*isaac.*"   |      |
      | exit   |                 | 0    |

  Scenario: empty argv streams usage and exits zero
    Given the cli-server handler with spawn command "echo Usage: isaac"
    When a /cli client sends start with argv []
    Then the handler sends frames:
      | type   | data            | code |
      | stdout | #".*Usage.*"   |      |
      | exit   |                 | 0    |

  Scenario: CLI validation errors frame on stdout with nonzero exit
    Given the cli-server handler with spawn command "echo Unknown option: --bogus; exit 1"
    When a /cli client sends start with argv "logs,--bogus"
    Then the handler sends frames:
      | type   | data                    | code |
      | stdout | #".*Unknown option.*"  |      |
      | exit   |                         | 1    |

  Scenario: unknown commands exit nonzero with usage on stdout
    Given the cli-server handler with spawn command "echo Unknown command: not-a-command; exit 1"
    When a /cli client sends start with argv ["not-a-command"]
    Then the handler sends frames:
      | type   | data                     | code |
      | stdout | #".*Unknown command.*"  |      |
      | exit   |                          | 1    |

  Scenario: stdin frames are accepted after a batch command starts
    Given the cli-server handler with spawn command "echo isaac"
    When a /cli client sends start with argv ["--version"]
    And the /cli client sends stdin "ignored"
    And the /cli client sends stdin-close
    Then the handler sends frames:
      | type   | data            | code |
      | stdout | #".*isaac.*"   |      |
      | exit   |                 | 0    |

  Scenario: an interactive subprocess streams stdin to stdout before exit (isaac-895i)
    Commands run as subprocesses with piped stdio; frames stream as produced.
    cat only echoes what it is fed, so the stdout frame arriving before
    stdin-close proves streaming duplex — impossible under buffer-until-exit.
    Given the cli-server handler with spawn command "cat"
    When a /cli client sends start with argv ["ignored-by-stub"]
    And the /cli client sends stdin "hello pipe"
    Then the handler sends frames:
      | type   | data              |
      | stdout | #".*hello pipe.*" |
    When the /cli client sends stdin-close
    Then the handler sends frames:
      | type | code |
      | exit | 0    |

  Scenario: a subprocess that kills itself is contained — the server keeps serving (isaac-895i)
    Given the cli-server handler with spawn command "sh -c 'exit 3'"
    When a /cli client sends start with argv []
    Then the handler sends frames:
      | type | code |
      | exit | 3    |
    Given the cli-server handler with spawn command "sh -c 'echo alive'"
    When a /cli client sends start with argv []
    Then the handler sends frames:
      | type   | data           | code |
      | stdout | #".*alive.*"  |      |
      | exit   |                | 0    |

  Scenario: the spawned command is always the isaac launcher with the client argv (isaac-895i)
    argv never selects the binary — `isaac` is implied and the arguments are
    applied to isaac main. There is no way to run an arbitrary program.
    Given the cli-server handler with a recording spawn stub
    When a /cli client sends start with argv ["sessions","list"]
    Then the recorded spawn command is the isaac launcher with args ["sessions","list"]

  Scenario: a dropped socket keeps the subprocess alive for the grace window, then destroys it (isaac-4tn1)
    Supersedes the unconditional kill-on-disconnect above once reconnect lands:
    disconnect enters the grace window; expiry destroys. Grace timing uses the
    injectable clock, not wall-clock sleeps.
    Given the cli-server handler with spawn command "sleep 60" and grace window 200 ms
    When a /cli client sends start with argv []
    And the /cli client disconnects
    Then the spawned subprocess is still running
    When the grace window elapses
    Then the spawned subprocess is no longer running

  Scenario: a remote command execution is logged with argv, timing, and exit code (isaac-iouj)
    No subprocess runs: the recording stub captures the spawn and simulates
    completion, so the scenario proves the LOGGING contract, not sh. The /cli
    endpoint executes arbitrary isaac commands for remote clients; grep :cli/
    in the server log must reconstruct what it was asked to do, when, and how
    each command exited (isaac-jnkp precedent).
    Given the cli-server handler with a recording spawn stub that exits with code 7
    When a /cli client sends start with argv ["sessions","list"]
    Then the cli log has entries matching:
      | level | event                | argv                | stream-id |
      | :info | :cli/command-started | ["sessions" "list"] | #*        |
    And the cli log has entries matching:
      | level | event                 | code | duration-ms | stream-id |
      | :info | :cli/command-finished | 7    | #*          | #*        |

  # --- isaac-qvhy: hosted commands run on a server thread, not a subprocess ---
  # The fixture commands are registered in the live CLI registry by the Given
  # (fx-echo, fx-print, fx-exit, fx-throw, fx-block, fx-local, fx-legacy); all
  # are marked :hosted except fx-legacy, and fx-local is :local-only.

  Scenario: a hosted command streams stdin to stdout before it exits (isaac-qvhy)
    Given the cli-server handler with the fixture commands registered
    When a /cli client sends start with argv ["fx-echo"]
    And the /cli client sends stdin "hello pipe"
    Then the handler sends frames:
      | type   | data              | code |
      | stdout | #".*hello pipe.*" |      |
    When the /cli client sends stdin-close
    Then the handler sends frames:
      | type | data | code |
      | exit |      | 0    |
    And no subprocess was spawned

  Scenario: a hosted command that calls exit is contained — the server keeps serving (isaac-qvhy)
    Given the cli-server handler with the fixture commands registered
    When a /cli client sends start with argv ["fx-exit","3"]
    Then the handler sends frames:
      | type | data | code |
      | exit |      | 3    |
    Given the cli-server handler with the fixture commands registered
    When a /cli client sends start with argv ["fx-print","alive"]
    Then the handler sends frames:
      | type   | data         | code |
      | stdout | #".*alive.*" |      |
      | exit   |              | 0    |

  Scenario: a hosted command that throws frames the message on stderr and exits 1 (isaac-qvhy)
    Given the cli-server handler with the fixture commands registered
    When a /cli client sends start with argv ["fx-throw","boom"]
    Then the handler sends frames:
      | type   | data        | code |
      | stderr | #".*boom.*" |      |
      | exit   |             | 1    |

  Scenario: a hosted command leaves the server's process state untouched (isaac-qvhy)
    Given the cli-server handler with the fixture commands registered
    And the server process state is snapshotted
    When a /cli client sends start with argv ["fx-print","hi"]
    Then the handler sends frames:
      | type | data | code |
      | exit |      | 0    |
    And the server process state is unchanged

  Scenario: a local-only command is refused over the pipe (isaac-qvhy)
    Given the cli-server handler with the fixture commands registered
    When a /cli client sends start with argv ["fx-local"]
    Then the handler sends frames:
      | type   | data                     | code |
      | stderr | #".*run this on the host.*" |   |
      | exit   |                          | 2    |
    And no subprocess was spawned

  Scenario: a --root that is not the server's root is refused (isaac-qvhy)
    Given the cli-server handler with the fixture commands registered
    When a /cli client sends start with argv ["--root","/somewhere/else","fx-print","hi"]
    Then the handler sends frames:
      | type   | data          | code |
      | stderr | #".*--root.*" |      |
      | exit   |               | 2    |

  Scenario: a dropped socket keeps the hosted command alive for the grace window, then cancels it (isaac-qvhy)
    fx-block parks on block-until-cancelled! and its shutdown fn records that it ran.
    Given the cli-server handler with the fixture commands registered and grace window 200 ms
    When a /cli client sends start with argv ["fx-block"]
    And the /cli client disconnects
    Then the hosted command is still running
    When the grace window elapses
    Then the hosted command is no longer running
    And the hosted command's shutdown fn ran

  Scenario: a reattached client receives frames buffered while detached (isaac-qvhy)
    Given the cli-server handler with the fixture commands registered and grace window 200 ms
    When a /cli client sends start with argv ["fx-echo"]
    And the /cli client disconnects
    And the /cli client sends stdin "while away"
    And a /cli client sends attach with the issued stream-id
    Then the handler sends frames:
      | type   | data              | code |
      | stdout | #".*while away.*" |      |

  Scenario: a hosted command is logged with argv, timing, and exit code (isaac-qvhy)
    Given the cli-server handler with the fixture commands registered
    When a /cli client sends start with argv ["fx-exit","7"]
    Then the cli log has entries matching:
      | level | event                | argv            | stream-id |
      | :info | :cli/command-started | ["fx-exit" "7"] | #*        |
    And the cli log has entries matching:
      | level | event                 | code | duration-ms | stream-id |
      | :info | :cli/command-finished | 7    | #*          | #*        |

  Scenario: a command not yet marked hosted still runs as a subprocess (transitional; removed by isaac-dqy9)
    Given the cli-server handler with the fixture commands registered
    And the cli-server handler with a recording spawn stub
    When a /cli client sends start with argv ["fx-legacy","x"]
    Then the recorded spawn command is the isaac launcher with args ["fx-legacy","x"]

  # --- stale basis: the server, not the client, knows its classpath is behind --
  # Basis = foundation version + module SHAs (isaac-tki3 :basis). Config mtimes
  # are exempt (hot-reloaded). Read-only commands still run; anything else is
  # refused until restart. fx-read is a fixture command marked :read-only.

  Scenario: a server whose module basis is stale refuses a mutating command with restart pending
    Given the cli-server handler with the fixture commands registered
    And the server's loaded module basis is behind the on-disk basis
    When a /cli client sends start with argv ["fx-print","hi"]
    Then the handler sends frames:
      | type   | data                     | code |
      | stderr | #".*restart pending.*"   |      |
      | exit   |                          | 75   |
    And the cli log has entries matching:
      | level | event                       | argv           |
      | :warn | :cli/refused-stale-basis    | ["fx-print" "hi"] |

  Scenario: a server whose module basis is stale still runs a read-only command
    Given the cli-server handler with the fixture commands registered
    And the server's loaded module basis is behind the on-disk basis
    When a /cli client sends start with argv ["fx-read"]
    Then the handler sends frames:
      | type   | data          | code |
      | stdout | #".*read ok.*" |     |
      | exit   |               | 0    |

  Scenario: read-only is per subcommand when the manifest lists subcommands
    fx-multi is :read-only #{"list"}: "list" runs, "set" is refused.
    Given the cli-server handler with the fixture commands registered
    And the server's loaded module basis is behind the on-disk basis
    When a /cli client sends start with argv ["fx-multi","list"]
    Then the handler sends frames:
      | type | data | code |
      | exit |      | 0    |
    Given the cli-server handler with the fixture commands registered
    And the server's loaded module basis is behind the on-disk basis
    When a /cli client sends start with argv ["fx-multi","set"]
    Then the handler sends frames:
      | type | data | code |
      | exit |      | 75   |

  Scenario: a current basis runs everything
    Given the cli-server handler with the fixture commands registered
    When a /cli client sends start with argv ["fx-multi","set"]
    Then the handler sends frames:
      | type | data | code |
      | exit |      | 0    |

  # --- isaac-4o6r: /cli declares scope :cli; read-only commands need only
  # :cli/read (epic isaac-gym1). isaac-http attaches :isaac/principal to the
  # upgrade request; the handler compares the command's :read-only hint
  # (isaac-kjzq) against the principal's scopes before running anything.

  Scenario: a principal scoped cli/read may run a read-only command (isaac-4o6r)
    Given the cli-server handler with the fixture commands registered
    And the /cli client is principal "viewer" with scopes "cli/read"
    When a /cli client sends start with argv ["fx-read"]
    Then the handler sends frames:
      | type   | data           | code |
      | stdout | #".*read ok.*" |      |
      | exit   |                | 0    |

  Scenario: a principal scoped cli/read is refused a mutating command before it runs (isaac-4o6r)
    Given the cli-server handler with the fixture commands registered
    And the /cli client is principal "viewer" with scopes "cli/read"
    When a /cli client sends start with argv ["fx-print","hi"]
    Then the handler sends frames:
      | type   | data                | code |
      | stderr | #".*requires cli.*" |      |
      | exit   |                     | 77   |
    And the cli log has entries matching:
      | level | event               | principal | argv            |
      | :warn | :cli/refused-scope  | viewer    | ["fx-print" "hi"] |

  Scenario: read-only per subcommand follows the manifest hint (isaac-4o6r)
    Given the cli-server handler with the fixture commands registered
    And the /cli client is principal "viewer" with scopes "cli/read"
    When a /cli client sends start with argv ["fx-multi","list"]
    Then the handler sends frames:
      | type | data | code |
      | exit |      | 0    |
    Given the cli-server handler with the fixture commands registered
    And the /cli client is principal "viewer" with scopes "cli/read"
    When a /cli client sends start with argv ["fx-multi","set"]
    Then the handler sends frames:
      | type | data | code |
      | exit |      | 77   |

  Scenario: a principal scoped cli runs everything (isaac-4o6r)
    Given the cli-server handler with the fixture commands registered
    And the /cli client is principal "ops" with scopes "cli"
    When a /cli client sends start with argv ["fx-multi","set"]
    Then the handler sends frames:
      | type | data | code |
      | exit |      | 0    |
    And the cli log has entries matching:
      | level | event                | principal | argv              |
      | :info | :cli/command-started | ops       | ["fx-multi" "set"] |

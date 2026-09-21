Feature: /cli WebSocket endpoint
  Handler-level tests for isaac.cli-server.ws/handler. Auth is enforced by
  isaac-http before the handler runs; not asserted here.

  # isaac-dqy9 removed every subprocess scenario: the handler no longer spawns
  # anything, so the isaac-895i scenarios (cat, sh -c 'exit 3', "the spawned
  # command is always the isaac launcher"), the isaac-4tn1 subprocess grace
  # window, the isaac-iouj spawn-stub logging scenario, the five "spawn
  # command" batch scenarios, and the transitional "not yet marked hosted"
  # scenario are gone with the recording-spawn-stub steps. Their contracts are
  # carried by the embedded scenarios below (streaming duplex, containment,
  # grace window, reattach, logging) and, end to end, by isaac-cli-proxy
  # features/integration.feature.

  # The fixture commands are registered in the live CLI registry by the Given
  # (fx-echo, fx-print, fx-read, fx-multi, fx-exit, fx-throw, fx-block,
  # fx-local). Every command is embedded; fx-local is :local-only.

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

  Scenario: every command over the pipe is logged as hosted (isaac-dqy9)
    Nothing spawns any more, so :cli/command-started carries hosted true for
    every command. isaac-cli-proxy asserts the same key against a real server.
    Given the cli-server handler with the fixture commands registered
    When a /cli client sends start with argv ["fx-print","hi"]
    Then the cli log has entries matching:
      | level | event                | argv             | hosted |
      | :info | :cli/command-started | ["fx-print" "hi"] | true   |

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

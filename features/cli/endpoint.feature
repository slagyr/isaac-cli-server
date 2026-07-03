Feature: /cli WebSocket endpoint
  Handler-level tests for isaac.cli-server.ws/handler. Auth is enforced by
  isaac-server before the handler runs; not asserted here.

  Scenario: a batch command streams stdout and exits zero
    Given the cli-server handler
    When a /cli client sends start with argv ["--version"]
    Then the handler sends frames:
      | type   | data     | code |
      | stdout | #".*isaac.*" |      |
      | exit   |          | 0    |

  Scenario: empty argv streams usage and exits zero
    Given the cli-server handler
    When a /cli client sends start with argv []
    Then the handler sends frames:
      | type   | data      | code |
      | stdout | #".*Usage.*"  |      |
      | exit   |           | 0    |

  Scenario: CLI validation errors frame on stdout with nonzero exit
    Given the cli-server handler
    When a /cli client sends start with argv "logs,--bogus"
    Then the handler sends frames:
      | type   | data                      | code |
      | stdout | #".*Unknown option.*"    |      |
      | exit   |                      | 1    |

  Scenario: unknown commands exit nonzero with usage on stdout
    Given the cli-server handler
    When a /cli client sends start with argv ["not-a-command"]
    Then the handler sends frames:
      | type   | data                | code |
      | stdout | #".*Unknown command.*"  |      |
      | exit   |                     | 1    |

  Scenario: stdin frames are accepted after a batch command starts
    Given the cli-server handler
    When a /cli client sends start with argv ["--version"]
    And the /cli client sends stdin "ignored"
    And the /cli client sends stdin-close
    Then the handler sends frames:
      | type   | data     | code |
      | stdout | #".*isaac.*" |      |
      | exit   |          | 0    |
  @wip
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

  @wip
  Scenario: a subprocess that kills itself is contained — the server keeps serving (isaac-895i)
    Given the cli-server handler with spawn command "sh -c 'exit 3'"
    When a /cli client sends start with argv []
    Then the handler sends frames:
      | type | code |
      | exit | 3    |
    Given the cli-server handler with spawn command "sh -c 'echo alive'"
    When a /cli client sends start with argv []
    Then the handler sends frames:
      | type   | data         | code |
      | stdout | #".*alive.*" |      |
      | exit   |              | 0    |

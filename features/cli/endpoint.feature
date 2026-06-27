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
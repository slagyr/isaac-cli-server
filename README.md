# 🍏 Isaac CLI Server ⚡

<img align="left" width="200" src="https://raw.githubusercontent.com/slagyr/isaac-cli-server/main/isaac-cli-server.png" alt="isaac-cli-server" style="margin-right: 20px; margin-bottom: 10px;">

Remote CLI host for [Isaac](https://github.com/slagyr/isaac). Exposes the
authenticated `/cli` WebSocket endpoint: spawns the real isaac launcher as a
subprocess and streams stdin/stdout/stderr back to the client.

Pairs with [isaac-cli-proxy](https://github.com/slagyr/isaac-cli-proxy)
(`isaac remote …`). Wire protocol: [PROTOCOL.md](PROTOCOL.md).

<br>

[![CI Tests](https://github.com/slagyr/isaac-cli-server/actions/workflows/ci-tests.yml/badge.svg)](https://github.com/slagyr/isaac-cli-server/actions/workflows/ci-tests.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Clojure](https://img.shields.io/badge/Clojure-1.11%2B-blue?logo=clojure)](https://clojure.org)
[![Babashka](https://img.shields.io/badge/Babashka-1.3%2B-red?logo=clojure)](https://babashka.org)
[![Java](https://img.shields.io/badge/Java-21%2B-orange?logo=openjdk)](https://openjdk.org/)

<br clear="left">

## Development

```bash
bb spec       # Run Clojure specs
bb features   # Run Gherkin feature scenarios
bb ci         # Run both
```

Depends on [isaac-foundation](https://github.com/slagyr/isaac-foundation) and
[isaac-server](https://github.com/slagyr/isaac-server).

## License

Copyright © 2026 Micah Martin. See [LICENSE](LICENSE).

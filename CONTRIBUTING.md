# Contributing to Faster

Thanks for your interest in Faster. Bug reports, documentation fixes,
benchmarks on other chains and code changes are all welcome.

## Before you start

- **Bugs:** open an issue with what you ran, what you expected and what
  happened. Include the component, its version, the chain and node client,
  and the relevant logs.
- **Features and larger changes:** open an issue first to discuss the design,
  so your time goes into a change that can be merged.
- **Security issues:** do not open a public issue; see [SECURITY.md](SECURITY.md).

## Repository layout

| Directory | Project | Toolchain |
|---|---|---|
| `faster-explorer/services/faster` | Explorer indexer and API | Rust, clang (RocksDB) |
| `faster-explorer/services/frontend` | Explorer UI | Node.js 20+ |
| `faster-explorer/services/clobarchiver` | CLOB archiver | Go |
| `faster-sync` | History API (+ `reth/`, `clients/`) | Rust, clang |
| `faster-rpc` | JSON-RPC server | Rust |
| `faster-index` | Indexing framework | Node.js 22.6+ |

Each project builds and tests on its own; run commands from its directory.

## Development checks

These are the checks the release pipeline runs. CI does not run on every push
or pull request, so please run the ones for the projects you touch.

```sh
# faster-sync
cd faster-sync && cargo fmt --all --check \
  && cargo clippy --locked --workspace --all-targets -- -D warnings \
  && cargo test --locked --release --workspace

# faster-sync reth extension (slow: builds reth)
cd faster-sync/reth && cargo fmt --check && cargo clippy --locked --release -- -D warnings

# faster-rpc
cd faster-rpc && cargo fmt --check \
  && cargo clippy --locked --all-targets -- -D warnings \
  && cargo test --locked --release

# explorer
cd faster-explorer/services/faster && cargo fmt --check && cargo test --locked
cd faster-explorer/services/frontend && npm ci && npm run typecheck && npm run build

# faster-index
cd faster-index && npm ci && npm test
```

To check the whole stack end to end, run `./fasterctl up demo` and
`./fasterctl status`.

## Performance changes

Faster is built around measured performance. For a change that affects speed,
include before and after numbers on the same machine, with the command used,
in the pull request. faster-sync's [benchmark notes](faster-sync/docs/BENCHMARK.md)
describe the existing method.

## Pull requests

- Keep each pull request to one change, with tests for new behaviour.
- Update the component's README or docs when behaviour or settings change,
  and add an entry to its CHANGELOG.
- Include the checks you ran in the pull request. Maintainers can also run
  the full suite on a branch with Actions → checks → Run workflow.

## License

By contributing, you agree that your contributions are dual licensed under
the MIT license and the Apache License, Version 2.0, as described in the
[README](README.md#license).

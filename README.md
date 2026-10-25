# Pseudogress

Pseudogress simulates many virtual PostgreSQL databases on one server, so
that a PostgreSQL client can be load-tested without a database's limits and
costs: thousands of databases and connections, with the latency you choose.

A virtual database is a name that a client connects to. Classes match names
by pattern, and give their databases a recording to answer from, users,
latency and faults.

- **Record mode** forwards each class's connections to a real PostgreSQL and
  records what it answers.
- **Replay mode** answers from the recordings, with the latency and faults in
  each class's settings.

## Status

Version 0.1.0, the first release. It is built to load-test multi-tenant
PostgREST, and its configuration and recording format may still change
before 1.0.

## Usage

```sh
nix-build                                        # or nix-shell --run 'cargo build --release'
result/bin/pseudogress record pseudogress.toml   # SIGINT or SIGTERM writes the recordings
result/bin/pseudogress replay pseudogress.toml
```

It prints the addresses it listens on to stderr, so a port of 0 in the
configuration picks a free one.

Each recording has a times file next to it (`small.times.jsonl` for
`small.jsonl`), and the two belong together. Both are sorted, so a
recording kept in version control changes only where PostgreSQL's answers
did.

The admin API has no authentication: whoever reaches it can read the
metrics, change latency and faults, and end connections. Bind it to
localhost or a private network.

## Platforms

Linux and macOS.

Simulated latencies under a millisecond need a machine that wakes a
sleeping thread within tens of microseconds of when it asked. Some
virtual machines take milliseconds (OrbStack's Linux VMs on macOS, for
one): there, these latencies come out too long, and the timer tests
(`cargo test timer::`) fail. GitHub's hosted macOS runners fire every
timer about half its delay late, so on them the tests only check that
no sleep wakes early or is lost.

## Running the tests

```sh
nix-shell --run 'cargo test'
```

The tests of record mode and the end-to-end test start throwaway
PostgreSQL clusters, from the PostgreSQL in the nix shell; without one on
the PATH, they skip themselves. Some tests open thousands of connections.

## Configuration

```toml
listen = ["127.0.0.1:5432"]
admin = "127.0.0.1:8081"
secret = "change me"   # passwords are HMACs of the database and user under it

[[class]]
name = "small"
databases = "small-*"
users = ["authenticator"]
recording = "recordings/small.jsonl"
max_connections = 60
upstream = { host = "127.0.0.1", port = 5433, user = "postgres", database = "app" }
# ignore_parameters = ["select set_config("]   # answered by text, recorded once

[class.settings]
round_trip = "500us"
execution_scale = 1.0   # 0 ignores the recorded execution times
# execution = { median = "1ms", p99 = "20ms" }   # log-normal, per statement
# execution_per_mib = "2ms"                      # per MiB of the answer

[class.settings.faults]
stall = 0.001
stall_for = "2s"
```

[docs/DESIGN.md](docs/DESIGN.md) explains what Pseudogress simulates and
what it doesn't, the recording format, the admin API and measured
performance.

## Licence

AGPL-3.0-only.

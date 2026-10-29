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

Version 0.1.1; classes from an HTTP service (below) are not in a release yet.
It is built to load-test multi-tenant PostgREST, and its configuration, the
service's answers and the recording format may still change before 1.0.

## Usage

```sh
nix-build                                        # or nix-shell --run 'cargo build --release'
result/bin/pseudogress record pseudogress.toml   # SIGINT or SIGTERM writes the recordings
result/bin/pseudogress replay pseudogress.toml
```

The command line, in full:

```
usage: pseudogress replay CONFIG [--recordings DIR]
       pseudogress record CONFIG [--class-url URL --recordings DIR [--max-classes N]]
```

Without the flags, which are for [classes from an HTTP
service](#classes-from-an-http-service), both modes use the configuration's
classes only. A usage error exits with code 2.

It prints the addresses it listens on to stderr, so a port of 0 in the
configuration picks a free one.

Each recording has a times file next to it (`small.times.jsonl` for
`small.jsonl`), and the two belong together. Both are sorted, so a
recording kept in version control changes only where PostgreSQL's answers
did.

The admin API has no authentication: whoever reaches it can read the
metrics, change latency and faults, and end connections. Bind it to
localhost or a private network.

## Download

Each release has one `.tar.gz` archive per platform on its
[page](https://github.com/mkleczek/pseudogress/releases), and a
`SHA256SUMS` file. The archive holds the `pseudogress` executable, the
licence and this README. Download the archive for your platform and
`SHA256SUMS`, then check and unpack it, and run the executable:

```sh
sha256sum -c --ignore-missing SHA256SUMS   # macOS: shasum -a 256 -c --ignore-missing SHA256SUMS
tar -xzf pseudogress-<version>-linux-x86_64.tar.gz
pseudogress-<version>-linux-x86_64/pseudogress
```

- **Linux**, x86_64 and aarch64: glibc 2.28 or newer, which is RHEL 8,
  Debian 10, Ubuntu 20.04 and later. Distributions based on musl, such as
  Alpine, are not supported.
- **macOS** 11 or newer, Apple silicon and Intel, in one archive. macOS
  quarantines a file downloaded with a browser, and the quarantine carries
  over to the unpacked files; remove it after unpacking with
  `xattr -dr com.apple.quarantine pseudogress-<version>-macos-universal`,
  or download with `curl -LO`.

## Nix

Run it without cloning:

```sh
nix run github:mkleczek/pseudogress -- replay pseudogress.toml
```

To pin a version, add a tag or a commit to the URL:
`github:mkleczek/pseudogress/<tag>` or `github:mkleczek/pseudogress/<commit>`.
Releases from v0.1.1 on have a `flake.nix`; v0.1.0 doesn't.

As a flake input, your `flake.lock` pins the commit, and the overlay adds
`pkgs.pseudogress`; the package is also
`inputs.pseudogress.packages.${system}.default`:

```nix
inputs.pseudogress.url = "github:mkleczek/pseudogress";
# ...
# The NixOS option. With `import nixpkgs { ... }`, pass the list as `overlays`.
nixpkgs.overlays = [ inputs.pseudogress.overlays.default ];
```

Without flakes, `default.nix` and `shell.nix` take `pkgs` (`<nixpkgs>` by
default), so `nix build -f .` and `nix develop -f shell.nix` work in a
checkout. To use the package in another setup, import the overlay from a
pinned commit:

```nix
import <nixpkgs> {
  overlays = [
    (import "${builtins.fetchTarball {
      url = "https://github.com/mkleczek/pseudogress/archive/<commit>.tar.gz";
      sha256 = "<hash>";   # optional, makes it a fixed download
    }}/overlay.nix")
  ];
}
```

`nix-prefetch-url --unpack <url>` prints the `sha256` of a tarball.

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
what it doesn't, the recording format, how classes load from a service, the
admin API and measured performance.

## Classes from an HTTP service

A load test of a multi-tenant client can have more tenants than anyone wants
to list, with database names that don't say which class they belong to. In
record mode Pseudogress asks an HTTP service for the class of a database that
no class matches, so the configuration lists only the classes that are fixed.

### Record and replay

```sh
# Record: the service gives the classes of the databases that no class matches.
pseudogress record pseudogress.toml \
  --class-url 'http://classes.internal:8080/classes/{database}?user={user}' \
  --recordings recordings
# Replay: from the recordings alone; the service is not called.
pseudogress replay pseudogress.toml --recordings recordings
```

- **Where classes are recorded.** A service's class is recorded in
  `DIR/<name>.jsonl`, with its times file beside it. Pseudogress chooses the
  path, so the service can't make it write elsewhere.
- **`classes.toml`.** At shutdown, after the recordings, record mode writes
  `DIR/classes.toml`: the service's classes as `[[class]]` tables, sorted by
  name, empty if there are none. Replay mode reads it and serves those
  classes after the configuration's.
- **`--recordings DIR`.** Record mode needs it with `--class-url`, and creates
  the directory at start, so that a bad path fails before any connection.
  Replay mode needs `DIR/classes.toml` to exist.

### What the service gets and answers

- **The request.** A `GET` of the URL, where `{database}` and, if the URL has
  it, `{user}` become the percent-encoded database and user of the
  connection. Only `http://`. The service has 5 seconds to answer.
- **200.** The body is the class, as JSON.
- **404.** No such database: the client gets FATAL 3D000, as for any unknown
  database.
- **Anything else** is a failure: another status, a network error, a
  timeout, invalid JSON or an invalid class. The client gets FATAL 08006
  `class service: <reason>`, and the reason goes to stderr. Nothing is
  remembered, so the next connection to the database asks again.

The class has the fields of a `[[class]]` table, except `recording`.
`upstream` is required, `name` is made of letters, digits, `_` and `-` as it
names files, and `databases` must match the database that was asked about. An
unknown field, including `recording`, is refused.
[docs/class.schema.json](docs/class.schema.json) is its JSON Schema, generated
from the types of the configuration. An example answer:

```json
{
  "name": "tenants",
  "databases": "tenant-*",
  "users": ["authenticator"],
  "upstream": { "host": "127.0.0.1", "port": 5433, "user": "postgres", "database": "app" },
  "max_connections": 60,
  "settings": { "round_trip": "500us" }
}
```

### Limits, and what shows in the admin API

- **`--max-classes N`.** The most classes the service may add, 256 by
  default; the configuration's don't count. Beyond it a lookup fails with
  FATAL 08006, whose message says to return classes whose pattern covers a
  group of databases.
- **Patterns cover groups of databases.** `tenant-*`, not `tenant-17`: one
  lookup then serves every database of the group, and the cap holds.
- **`GET /classes`** lists the classes in matching order, each with its
  database pattern and a `source` of `config` or `service`.
- **Metrics**, only with `--class-url`: `pseudogress_class_lookups_total`
  with a `result` of `found`, `not_found` or `error`, and the histogram
  `pseudogress_class_lookup_seconds`. A request counts, not each connection
  that waits for the same one.

## Licence

AGPL-3.0-only.

# The design of Pseudogress

A load test of a PostgreSQL client should find the limits of the client,
not of the databases behind it. Pseudogress takes the databases out of the
numbers: it speaks the PostgreSQL protocol to the client and answers from
recordings of a real PostgreSQL, with latency and faults that the test
chooses. One machine can then serve thousands of databases and connections.

## Why not a real database

- **A database per client tenant on one server.** PostgreSQL runs a process
  per connection, so a thousand databases with a few connections each exceed
  what one server runs, and the server's CPU shows up in every number.
- **One database behind a pooler** (PgBouncer, Supavisor). The pooler lifts
  the connection limit, but LISTEN doesn't work with transaction pooling, and
  the pooler's latency and CPU get measured too. It stays useful as a realism
  check.
- **A fake inside the client.** It has no sockets, file descriptors, TLS or
  SCRAM, which are part of what a load test measures.

## Virtual databases

A virtual database is a name that a client connects to.

- **Classes.** A class matches database names with a pattern, where `*`
  matches any characters; the first class wins: the configuration's in their
  order, then the ones a class service added. It gives its databases a
  recording, users, latency, faults and `max_connections`. Two classes can't
  have the same name, nor, in record mode, the same recording.
- **Existence.** A name that matches no class, and that the class service
  doesn't know if there is one, gets FATAL 3D000, as in PostgreSQL. Nothing is
  created ahead, so a test can use millions of names.
- **State.** A database has state only while it has connections: the
  connections and the channels they listen on. The registry is sharded by
  the name's hash, so connecting and disconnecting take one shard's lock and
  nothing is done per statement.
- **Passwords.** A password is fixed in the configuration, or derived: the
  lowercase hex of HMAC-SHA-256 keyed with the secret, over the database
  name, a zero byte and the user name. A load generator derives the same,
  so nothing is stored per database.

## Fidelity

Pseudogress behaves like PostgreSQL wherever the behaviour costs the client
something, or the results would mislead.

- **Connection setup.**
  - SSLRequest gets `S` and a TLS handshake when TLS is configured, `N`
    otherwise. Sessions never resume, as PostgreSQL has neither tickets nor a
    session cache.
  - SCRAM-SHA-256 at 4096 iterations, PostgreSQL's default, and
    SCRAM-SHA-256-PLUS on TLS. The client pays PostgreSQL's real cost of a
    connection; Pseudogress hashes each password once and caches the
    verifier.
  - Every answer the client waits for during setup comes one round trip
    after its request, except the SSL answer: the class, and with it the
    round trip, is known only from the startup message.
  - The recorded parameter statuses, BackendKeyData, and a delay chosen from
    the recorded backend start times.
- **Batches.** A connection reads messages up to a Sync, a Flush or a simple
  Query, has the backend answer them, waits for the simulated time and
  writes the answer before it reads on, as a PostgreSQL backend does. The
  answer goes out in writes of at most 8 KiB, PostgreSQL's send buffer, and
  each write waits for the client: a client that stops reading stalls its
  connection.
- **Simulated time.** A batch crosses half a round trip, waits for the
  batches before it, executes, and the answer crosses the other half.
  Batches sent together pay the network once, but their executions add up,
  as on a real server.
  - A statement (an Execute or a simple query) takes a time chosen from its
    recorded execution times, times the class's `execution_scale`, plus a
    time drawn from a log-normal distribution with the class's `execution`
    median and 99th percentile, if it has one, plus `execution_per_mib` for
    each MiB of its recorded answer.
  - A load test that compares builds sets `execution_scale` to 0: recorded
    times differ between recordings and come from a local database, while
    configured ones are the same for every build.
  - Connection start times are the recorded ones, scaled.
  - tokio's timer rounds up to milliseconds, so the last few milliseconds
    of every wait go to a timer thread that blocks on one OS timer set to
    the earliest deadline: a timerfd on Linux, a kqueue timer that the
    kernel doesn't coalesce on macOS. Wake-ups come within tens of
    microseconds, and waiting costs no CPU.
- **Protocol.** The extended protocol with named and unnamed statements and
  portals, pipelining, skipping to Sync after an error, the simple protocol
  with several statements, the transaction status from the answers' command
  tags, 25P02 in a failed transaction and `ROLLBACK` for its COMMIT, and
  42P05 for a Parse of a named statement that wasn't closed.
- **Notifications.** LISTEN and UNLISTEN in simple queries register the
  connection; the admin API sends notifications to a database, a class, or a
  random fraction of a class's open databases. They go out when the
  connection is idle.
- **Cancellation.** A CancelRequest with a known key replaces the answer of
  the batch that is waiting with 57014. A batch that ended in a Flush has
  no ReadyForQuery to replace: the connection skips to the client's next
  Sync, which gets it, as after any error.
- **Faults**, per class, with probabilities: refused and hanging
  connections, rejected passwords (28P01), resets, stalls, and 57014 when a
  batch's execution exceeds `statement_timeout`. `max_connections` gives
  FATAL 53300, and the admin API terminates connections with FATAL 57P01.
- **Metrics** per class, in Prometheus' format: connections, connects,
  authentication failures, statements, batches in flight, bytes, faults,
  recording misses, fallbacks to the recording of other parameters,
  Pseudogress' own processing time per batch, and how late answers are sent
  after their simulated time, which shows when Pseudogress itself adds to the
  latency being measured.
  - **Lookups**, with a class service only, are not per class: a counter by
    result (found, not found, error) and a histogram of their durations. A
    request counts, not each connection that waits for a shared one.

## Record and replay

### Recording

- **Proxy.** In record mode Pseudogress authenticates the client itself
  and connects to the class's upstream with credentials of its own: SCRAM
  with channel binding can't be passed through. It forwards every batch and
  records what PostgreSQL answered, byte for byte. After an error it
  doesn't forward what PostgreSQL ignores, the messages up to the next Sync.
  When the upstream fails the client gets FATAL 08006 and its connection
  ends.
- **What's recorded**, one JSON object per line, messages in base64 (values
  of 64 KiB and more zlib-compressed first, so that large answers fit in
  version control):
  - **Execute**, by statement text, parameter formats, parameters and result
    formats: the portal's description and the rows, error included.
  - **Describe** of a statement, by statement text and parameter types.
  - **Simple queries**, by text.
  - **Startup**: the parameter statuses and the backend start times.
  - Execution times, up to 1000 samples per entry. They and the start
    times are not in these lines but in the times file.
- **Statements that ignore parameters.** Some clients run a statement on
  every request whose parameters change each time and whose answer they
  ignore (PostgREST's `set_config` call, with the request's claims and
  headers). A class lists their prefixes in `ignore_parameters`, and their
  Executes are keyed by statement text and result formats alone: the
  recording keeps one entry per statement, with the first answer and the
  execution times of every execution. Otherwise it would grow with every
  request.
- **Describes added.** An Execute entry needs its portal's description, but
  some clients (tokio-postgres) never describe a portal. Pseudogress sends
  the Describe for them and keeps its answer from the client.
- **Times per Execute.** PostgreSQL holds its answers until a Flush or a
  Sync, so Pseudogress sends a Flush after every Execute and times each one
  from the end of the one before it.
- **Writing.** SIGINT or SIGTERM writes every class's recording, replacing
  its files together: both are written aside and synced before either is
  renamed, so that a write that fails, on a full disk say, leaves the
  previous recording. The recording file has its entries sorted, and the
  times file next to it (`small.times.jsonl` for `small.jsonl`) has the
  times of each entry, by a hash of its line. Connections ask in a
  different order on every run and the times differ, so recording again
  would rewrite everything: this way only the lines whose statement or
  answer changed do, and a recording can be kept in version control and
  reviewed as a diff.

### Replay

- **Loading.** Recordings are indexed at start, with every answer encoded
  once. A Parse resolves its statement and a Bind its recorded Execute, so
  an Execute is a copy of bytes, with no lock and no allocation. A times
  file that doesn't match its recording is refused, naming the line.
- **Lookup.** By the full key, then by statement text and result formats
  alone, which serves parameters that differ from the recorded ones, with
  the first answer in the recording file's order. Each such fallback is
  counted, so that a test can check that its recording covers its requests.
  A statement whose text starts with one of the class's `ignore_parameters`
  (decided once, when the recording is indexed) is looked up by statement
  text and result formats alone, and isn't counted as a fallback.
  A statement without a recording gets ErrorResponse XX000 and counts as a
  miss; there is never a made-up answer.
- **Column override.** A class can replace a column of recorded rows with
  generated JSON of a given size, to vary the size of answers without
  recording one fixture per size.

### Loading classes on demand

A multi-tenant client's database names don't always say which class a database
belongs to, and a test can have too many tenants to list their classes. With
`--class-url`, record mode asks an HTTP service for the class of a database
that no class matches, and adds it. The request, the answer and the JSON
Schema in `docs/class.schema.json` are in the README.

- **Where the lookup sits.** In connection setup, after the startup message
  and before authentication, which needs the class's users and password. A
  database that a class matches never waits for a lookup, so the connections
  of known classes cost what they did.
- **Shared lookups.** Connections to one unknown database share one request
  and its outcome. Matching runs again before a lookup starts and after one
  ends, because a class loaded meanwhile may cover the name: with a pattern
  for a group, one request serves the group's connections.
- **No negative caching.** A 404 or a failure is not remembered, so a database
  that the service learns of later works, and a failure of the service doesn't
  stick. The cost is a request for each connection to an unknown name.
- **The class's checks.** The service's class gets the configuration's checks,
  and its `databases` must match the database that was asked about. A name
  that is already loaded is the same class when the service returns an
  identical definition again, and an error when the definition differs or
  the configuration has the name.
- **Append-only registry.** The classes, the rules of `databases` and the
  targets of record mode are registries with stable indexes, as a connection
  holds its class as an index. Their capacity is fixed at start, the
  configuration's classes plus `--max-classes`, so a slot never moves and
  readers take no lock. Adding takes a mutex, and goes to the connector first,
  which can refuse a class, then to the classes, and last to the databases,
  which make it reachable: a class is visible to a connection only once all
  three have it. Replay fills the registries at start and never adds, so its
  per-batch lookup costs what it did.
- **Order of adding.** The service's classes go after the configuration's, in
  the order they were added, and the first match wins. A service class can't
  take over a database of the configuration.
- **Overlapping patterns are the service's error.** Pseudogress doesn't
  compare the patterns of the classes that a service returns. If two cover one
  database, the earlier added gets it, and which was added first depends on
  the order of the connections, so two runs can differ.
- **The service doesn't choose file paths.** Pseudogress records a class in
  `DIR/<name>.jsonl`, so a service can't make it write elsewhere. A name has no
  dot: the times of `a` are in `a.times.jsonl`, which would be the recording
  of a class named `a.times`.
- **The cap.** `--max-classes` (256 by default) bounds what a service that
  returns a class per database can cost: each class holds a recording in
  memory until shutdown, and a file. Beyond it a lookup fails with 08006.
- **`classes.toml`, the hand-off to replay.** At shutdown, after the
  recordings, record mode writes the service's classes as `[[class]]` tables
  with `recording = "<name>.jsonl"` relative to the directory. They are sorted
  by name, not in the order of connections, so that the file changes only
  where the classes did. It is written aside and renamed, like the recordings.
  Replay mode loads it from `--recordings` and appends the classes to the
  configuration's with the same checks; a duplicate name is an error. It never
  calls the service: the file and the recordings are all it needs.

### What replay doesn't model

- Query plans, lock waits and contention inside the database, beyond the
  recorded execution times.
- Data that changes: writes don't change what reads return.
- Values in simple-protocol statements: they are part of the text, so a
  value that wasn't recorded is a miss. Clients that send parameters with
  the extended protocol don't have this problem.
- Savepoints, COPY and cursors (PortalSuspended).
- Read replicas: Pseudogress always reports a primary.
- TLS to the upstream in record mode.

## Admin API

- `GET /metrics`
- `GET /classes`: the classes in matching order, each with its database
  pattern and a `"source"` of `"config"` or `"service"`.
- `GET /databases/{name}`: the open database's connections and listeners.
- `POST /notify`: `{"channel", "payload"}` and a target.
- `POST /terminate`: a target.
- `GET` and `PUT /classes/{name}/settings`: the round trip, the execution
  scale, model and time per MiB, and the faults, which connections read once
  per batch.

A target is `{"database": name}` or `{"class": name}`, with an optional
`"fraction"` of the class's open databases.

## Performance

Measured on a 10-core Apple laptop, replaying pgbench's select-only script
in prepared mode (release build, pgbench on the same machine):

- 5,000 concurrent connections over five databases: 86,000 transactions
  per second, with pgbench as the limit; Pseudogress used about 2 cores at
  most and 192 MB, about 40 KB per connection.
- Pseudogress' own processing took 2.3 µs per batch on average.
- A round trip of 1 ms added 1.08 ms to one client's latency, and one of
  5 ms added 5.12 ms.
- One client with a round trip of 500 µs cost Pseudogress 0.09 cores.

## Implementation

- **Rust, on pgwire's message layer.** pgwire decodes and encodes the
  messages; its handler API is not used, as it rebuilds rows from typed
  values, which byte replay doesn't need. Rust has no garbage-collector
  pauses to add to the tail latency being measured.
- **One task per connection**, generic over the stream, so that plain and
  TLS connections are both monomorphized, with no dynamic dispatch.
- **Modules** with narrow APIs: `session` (the protocol), `replay` and
  `record` (the two kinds of backend behind a trait), `databases`, `auth`,
  `tls`, `timer`, `metrics`, `admin`, `loader` (loading classes on demand)
  and `service` (the client of a class service, written by hand on tokio like
  the admin server, so that it adds no dependency). Shared data
  (configuration, recording format, classes, the registry) is in plain
  modules that the others read.

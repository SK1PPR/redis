# Redis Clone in Rust

A Redis-compatible server implemented in Rust, built as a systems programming project to explore RESP parsing, event loops, in-memory data structures, replication mechanics, and performance profiling.

The server is intentionally small compared with production Redis, but it supports a broad subset of Redis-style commands and includes a benchmark/profiling workflow for comparing hot paths against a local Redis instance.

## Features

- RESP command parsing
- Single-threaded TCP event loop powered by `mio`
- String commands: `PING`, `ECHO`, `SET`, `GET`, `DEL`, `EXISTS`, `INCR`, `TYPE`
- Expiring keys via `SET ... EX/PX`
- Lists: `RPUSH`, `LPUSH`, `LRANGE`, `LLEN`, `LPOP`, `BLPOP`, `BRPOP`
- Transactions: `MULTI`, `EXEC`, `DISCARD`
- Sorted sets: `ZADD`, `ZRANK`, `ZRANGE`, `ZCARD`, `ZSCORE`, `ZREM`
- Streams: `XADD`, `XRANGE`, `XREAD`
- Geospatial commands: `GEOADD`, `GEOPOS`, `GEODIST`, `GEOSEARCH`
- Pub/Sub: `SUBSCRIBE`, `PUBLISH`, `UNSUBSCRIBE`
- Basic replication handshake and command propagation scaffolding
- RDB loading for basic string keys
- Performance benchmark and profiling scripts

## Requirements

- Rust toolchain
- Optional, for comparison benchmarks: Redis installed locally (`redis-server`)
- Optional, for CPU sampling on macOS: `sample`

## Run

```bash
./your_program.sh --port 6379
```

Or directly with Cargo:

```bash
cargo run --release -- --port 6379
```

Useful runtime flags:

```bash
cargo run --release -- --port 6380
cargo run --release -- --port 6381 --replicaof "127.0.0.1 6380"
cargo run --release -- --dir /tmp --dbfilename dump.rdb
```

## Try It

In another terminal:

```bash
redis-cli -p 6379 PING
redis-cli -p 6379 SET hello world
redis-cli -p 6379 GET hello
redis-cli -p 6379 ZADD scores 10 alice
redis-cli -p 6379 ZRANK scores alice
redis-cli -p 6379 GEOADD places -122.4194 37.7749 sf
redis-cli -p 6379 GEODIST places sf sf
```

## Test

```bash
cargo test
```

## Benchmarking

The repo includes a dependency-free Python benchmark harness that compares this clone against a local Redis server over TCP loopback:

```bash
python3 scripts/perf_compare.py --count 2000 --pipeline 128
```

Profile a specific workload on macOS:

```bash
cargo build --release
python3 scripts/profile_clone.py --workload geodist --count 20000 --seconds 8
```

See [PERFORMANCE.md](PERFORMANCE.md) for methodology, caveats, before/after results, and profiling notes.

Important: these are local development benchmarks, not official Redis benchmarks. Redis is production software with far more behavior and operational maturity than this learning project.

## Project Layout

```text
src/
  commands/      command enum, parser, executor, RESP responses
  protocol/      RESP parser
  server/        TCP server, client state, event loop, event-loop handle
  storage/       in-memory data structures, RDB helpers, replication config
scripts/
  perf_compare.py              clone-vs-Redis benchmark harness
  profile_clone.py             macOS CPU sampling runner
  render_performance_charts.py chart generator for benchmark reports
```

## Notes

- This project is Redis-compatible in spirit, not a drop-in replacement for production Redis.
- The implementation favors clarity and learning over complete Redis semantics.
- Some commands are intentionally partial and may not support every Redis option.

## License

MIT

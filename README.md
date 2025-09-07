## Redis server in Rust

This repository contains a Redis-compatible server implemented in Rust. It supports core commands like `PING`, `ECHO`, `SET`, `GET`, and includes event loop handling, RESP protocol parsing, and optional replication flags.

### Features
- Replication (master/replica via `--replicaof`)
- Sorted sets
- Streams
- Pub/Sub
- Lists
- Persistent storage (configurable `--dir`, `--dbfilename`)
- Geospatial commands

### Requirements
- Rust toolchain (1.70+ recommended)

### Run
```bash
./your_program.sh --port 6379
```

Or via Cargo directly:
```bash
cargo run --release -- --port 6379
```

### Project layout
- `src/main.rs`: CLI, startup, and configuration parsing
- `src/lib.rs`: Library exports
- `src/server/`: TCP server, event loop
- `src/protocol/`: RESP encoder/decoder
- `src/commands/`: Command parsing, execution, and responses
- `src/storage/`: In-memory structures, persistence helpers, replication config

### Notes
- Set data directory and RDB filename via `--dir` and `--dbfilename`.
- Configure replication via `--replicaof "<host> <port>"`.

### License
MIT

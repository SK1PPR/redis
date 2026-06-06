# Performance Testing

This repo includes a dependency-free benchmark harness that compares the Rust
server with a local `redis-server` by sending pipelined RESP commands over TCP.

## Run

```bash
python3 scripts/perf_compare.py --count 10000 --pipeline 128
```

The script starts this clone on port `6381` with `cargo run --release` and a
real Redis server on port `6382`. To compare already-running servers:

```bash
python3 scripts/perf_compare.py --external-clone --external-redis --clone-port 6379 --redis-port 6380
```

Run a focused workload while iterating on one subsystem:

```bash
python3 scripts/perf_compare.py --workload set --workload get-hot --count 50000
```

Profile the clone while a workload runs:

```bash
cargo build --release
python3 scripts/profile_clone.py --workload geodist --count 20000 --seconds 8
```

Profiles are written to `target/profiles/*.sample.txt` using macOS `sample`.

## Workloads

- `ping`: event loop, parser, response serialization baseline.
- `set`: write path, storage insertion, replication fanout checks.
- `get-hot`: repeated string reads and response serialization.
- `incr`: numeric string mutation.
- `rpush` and `lpop`: list append and front-pop behavior.
- `zadd` and `zrank-tail`: sorted set insertion and member lookup behavior.
- `geoadd`, `geopos-hot`, `geodist`, and `geosearch`: geospatial writes,
  member lookup, distance calculation, and radius scans.

## Baseline Result

On a local run with `--count 5000 --pipeline 128`, the clone compared to Redis
as follows:

| workload | clone ops/s | redis ops/s | clone/redis |
| --- | ---: | ---: | ---: |
| `ping` | 506,892 | 721,930 | 70.21% |
| `set` | 243,157 | 611,300 | 39.78% |
| `get-hot` | 438,587 | 570,841 | 76.83% |
| `incr` | 523,597 | 647,962 | 80.81% |
| `rpush` | 510,295 | 657,145 | 77.65% |
| `lpop` | 362,131 | 562,258 | 64.41% |
| `zadd` | 117,609 | 475,046 | 24.76% |
| `zrank-tail` | 55,928 | 546,237 | 10.24% |

Focused geo run with `--count 2000 --pipeline 128`:

| workload | clone ops/s | redis ops/s | clone/redis |
| --- | ---: | ---: | ---: |
| `geoadd` | 132,830 | 313,343 | 42.39% |
| `geopos-hot` | 185,918 | 240,711 | 77.24% |
| `geodist` | 131,122 | 479,626 | 27.34% |
| `geosearch` | 489 | 508 | 96.14% |

## Profile-Guided Improvements

Before fixing the indexed lookup and hot-path stdout issues, the large
cardinality profiles showed:

| workload | count | clone ops/s | redis ops/s | clone/redis |
| --- | ---: | ---: | ---: | ---: |
| `geodist` | 20000 | 16,885 | 539,992 | 3.13% |
| `zrank-tail` | 30000 | 11,334 | 692,090 | 1.64% |
| `geoadd` | 30000 | 21,379 | 453,122 | 4.72% |
| `set` | 50000 | 227,652 | 705,326 | 32.28% |

The corresponding sample reports were:

- `target/profiles/geodist-1780738415.sample.txt`
- `target/profiles/zrank-tail-1780738429.sample.txt`
- `target/profiles/geoadd-1780738443.sample.txt`
- `target/profiles/set-1780738451.sample.txt`

After adding zset/geo member indexes, lazy rank rebuilding, and removing
unconditional `SET` stdout writes:

| workload | count | clone ops/s | redis ops/s | clone/redis |
| --- | ---: | ---: | ---: | ---: |
| `geodist` | 20000 | 435,152 | 637,296 | 68.28% |
| `zrank-tail` | 30000 | 557,755 | 785,753 | 70.98% |
| `geoadd` | 30000 | 427,686 | 450,222 | 94.99% |
| `set` | 50000 | 525,456 | 746,616 | 70.38% |

The post-fix sample reports were:

- `target/profiles/geodist-1780747970.sample.txt`
- `target/profiles/zrank-tail-1780747986.sample.txt`
- `target/profiles/geoadd-1780747994.sample.txt`
- `target/profiles/set-1780748003.sample.txt`

Post-fix full run with `--count 2000 --pipeline 128`:

| workload | clone ops/s | redis ops/s | clone/redis |
| --- | ---: | ---: | ---: |
| `ping` | 580,404 | 768,443 | 75.53% |
| `set` | 428,155 | 511,024 | 83.78% |
| `get-hot` | 432,561 | 513,457 | 84.24% |
| `incr` | 483,345 | 519,413 | 93.06% |
| `rpush` | 410,481 | 421,138 | 97.47% |
| `lpop` | 380,195 | 499,995 | 76.04% |
| `zadd` | 395,733 | 395,036 | 100.18% |
| `zrank-tail` | 423,113 | 526,564 | 80.35% |
| `geoadd` | 382,059 | 372,041 | 102.69% |
| `geopos-hot` | 191,863 | 230,396 | 83.28% |
| `geodist` | 389,348 | 494,912 | 78.67% |
| `geosearch` | 472 | 498 | 94.69% |

## Current Hot Spots To Investigate

- Normal client traffic was blocked because `RespParser::new()` started in RDB
  mode. The default now starts in normal RESP mode, and replication can still
  switch to RDB parsing with `set_expecting_rdb`.
- Do not force `LevelFilter::Debug` in `main`; a local run with debug logging
  enabled dropped simple command throughput by roughly 2-3x because hot-path
  `log::debug!` calls became active.
- `RespParser` converts every bulk string into a new `String`, so command
  parsing allocates per argument before command execution even starts.
- `Storage::get` clones the whole `Unit` and then clones the stored string,
  adding avoidable allocation and copy cost to reads.
- Lists are backed by `Vec<String>`; `LPUSH`, `LPOP`, and `BLPOP` from the left
  shift elements and become expensive as lists grow. `VecDeque` is a better fit.
- Sorted sets now have member indexes for direct member lookup, but rank indexes
  are rebuilt lazily as a whole key. A skiplist/tree with order statistics would
  make mixed write/rank workloads more consistent.
- `GEOSEARCH` still scans the whole geo set before filtering. A geohash-cell
  candidate search would avoid testing every member.
- Streams are backed by a plain `Vec`, so `XRANGE` and `XREAD` scan linearly.
  A `BTreeMap<StreamId, Entry>` would make range reads scale with the requested
  range rather than total stream length.

#!/usr/bin/env python3
"""Compare this Redis clone against real Redis with pipelined RESP workloads."""

from __future__ import annotations

import argparse
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_VALUE = "x" * 32


@dataclass(frozen=True)
class Workload:
    name: str
    commands: list[list[str]]


@dataclass
class Server:
    name: str
    host: str
    port: int
    process: subprocess.Popen | None = None


class RespReader:
    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        self.buffer = bytearray()

    def read_response(self) -> object:
        while True:
            parsed = self._try_parse(0)
            if parsed is not None:
                value, consumed = parsed
                del self.buffer[:consumed]
                return value
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("server closed connection while reading RESP")
            self.buffer.extend(chunk)

    def _try_parse(self, pos: int) -> tuple[object, int] | None:
        if pos >= len(self.buffer):
            return None
        prefix = self.buffer[pos : pos + 1]
        if prefix in (b"+", b"-", b":"):
            end = self._line_end(pos + 1)
            if end is None:
                return None
            text = self.buffer[pos + 1 : end].decode("utf-8", "replace")
            if prefix == b":":
                return int(text), end + 2 - pos
            return text, end + 2 - pos
        if prefix == b"$":
            end = self._line_end(pos + 1)
            if end is None:
                return None
            size = int(self.buffer[pos + 1 : end])
            body_start = end + 2
            if size == -1:
                return None, body_start - pos
            body_end = body_start + size
            if body_end + 2 > len(self.buffer):
                return None
            data = bytes(self.buffer[body_start:body_end])
            return data, body_end + 2 - pos
        if prefix == b"*":
            end = self._line_end(pos + 1)
            if end is None:
                return None
            count = int(self.buffer[pos + 1 : end])
            if count == -1:
                return None, end + 2 - pos
            items = []
            cursor = end + 2
            for _ in range(count):
                parsed = self._try_parse(cursor)
                if parsed is None:
                    return None
                value, consumed = parsed
                items.append(value)
                cursor += consumed
            return items, cursor - pos
        raise ValueError(f"unsupported RESP prefix: {prefix!r}")

    def _line_end(self, start: int) -> int | None:
        idx = self.buffer.find(b"\r\n", start)
        return idx if idx >= 0 else None


def encode_command(parts: Iterable[str]) -> bytes:
    encoded = [str(part).encode("utf-8") for part in parts]
    out = bytearray(f"*{len(encoded)}\r\n".encode("ascii"))
    for part in encoded:
        out.extend(f"${len(part)}\r\n".encode("ascii"))
        out.extend(part)
        out.extend(b"\r\n")
    return bytes(out)


def wait_for_port(host: str, port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    last_error: OSError | None = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.25) as sock:
                sock.sendall(encode_command(["PING"]))
                reader = RespReader(sock)
                reader.read_response()
                return
        except OSError as exc:
            last_error = exc
            time.sleep(0.05)
    raise RuntimeError(f"timed out waiting for {host}:{port}: {last_error}")


def run_commands(server: Server, commands: list[list[str]], pipeline: int) -> float:
    payloads = [encode_command(command) for command in commands]
    with socket.create_connection((server.host, server.port), timeout=5.0) as sock:
        sock.settimeout(30.0)
        reader = RespReader(sock)
        start = time.perf_counter()
        sent = 0
        while sent < len(payloads):
            batch = payloads[sent : sent + pipeline]
            sock.sendall(b"".join(batch))
            for _ in batch:
                reader.read_response()
            sent += len(batch)
        elapsed = time.perf_counter() - start
    return len(commands) / elapsed


def flush_server(server: Server) -> None:
    with socket.create_connection((server.host, server.port), timeout=5.0) as sock:
        sock.sendall(encode_command(["FLUSHALL"]))
        RespReader(sock).read_response()


def delete_keys(server: Server, keys: list[str], pipeline: int = 16) -> None:
    if not keys:
        return
    commands = [["DEL", *keys[i : i + 256]] for i in range(0, len(keys), 256)]
    run_commands(server, commands, pipeline=pipeline)


def make_workloads(count: int) -> list[Workload]:
    keys = [f"bench:{i}" for i in range(count)]
    geo_points = geo_dataset(count)
    return [
        Workload("ping", [["PING"] for _ in range(count)]),
        Workload("set", [["SET", key, DEFAULT_VALUE] for key in keys]),
        Workload("get-hot", [["GET", "bench:hot"] for _ in range(count)]),
        Workload("incr", [["INCR", "bench:counter"] for _ in range(count)]),
        Workload("rpush", [["RPUSH", "bench:list", str(i)] for i in range(count)]),
        Workload("lpop", [["LPOP", "bench:list"] for _ in range(count)]),
        Workload(
            "zadd",
            [["ZADD", "bench:zset", str(i), f"member:{i}"] for i in range(count)],
        ),
        Workload("zrank-tail", [["ZRANK", "bench:zset", f"member:{count - 1}"] for _ in range(count)]),
        Workload(
            "geoadd",
            [
                ["GEOADD", "bench:geo", f"{lon:.6f}", f"{lat:.6f}", member]
                for lon, lat, member in geo_points
            ],
        ),
        Workload("geopos-hot", [["GEOPOS", "bench:geo", "geo:0"] for _ in range(count)]),
        Workload("geodist", [["GEODIST", "bench:geo", "geo:0", f"geo:{count - 1}"] for _ in range(count)]),
        Workload(
            "geosearch",
            [["GEOSEARCH", "bench:geo", "FROMLONLAT", "-122.4194", "37.7749", "BYRADIUS", "5000", "km"] for _ in range(count)],
        ),
    ]


def geo_dataset(count: int) -> list[tuple[float, float, str]]:
    points = []
    for i in range(count):
        lon = -122.4194 + ((i % 200) - 100) * 0.01
        lat = 37.7749 + ((i // 200) % 200 - 100) * 0.01
        points.append((lon, lat, f"geo:{i}"))
    return points


def prepare_for_workload(server: Server, workload: Workload, count: int) -> None:
    try:
        flush_server(server)
    except Exception:
        # The clone does not implement FLUSHALL yet.
        pass
    delete_keys(
        server,
        [f"bench:{i}" for i in range(count)]
        + ["bench:hot", "bench:counter", "bench:list", "bench:zset", "bench:geo"],
    )
    if workload.name == "get-hot":
        run_commands(server, [["SET", "bench:hot", DEFAULT_VALUE]], pipeline=1)
    elif workload.name == "lpop":
        run_commands(
            server,
            [["RPUSH", "bench:list", str(i)] for i in range(count)],
            pipeline=256,
        )
    elif workload.name == "zrank-tail":
        run_commands(
            server,
            [["ZADD", "bench:zset", str(i), f"member:{i}"] for i in range(count)],
            pipeline=256,
        )
    elif workload.name in {"geopos-hot", "geodist", "geosearch"}:
        run_commands(
            server,
            [
                ["GEOADD", "bench:geo", f"{lon:.6f}", f"{lat:.6f}", member]
                for lon, lat, member in geo_dataset(count)
            ],
            pipeline=256,
        )


def start_clone(port: int) -> subprocess.Popen:
    command = ["cargo", "run", "--release", "--", "--port", str(port)]
    return subprocess.Popen(
        command,
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def start_redis(port: int, data_dir: Path) -> subprocess.Popen:
    redis_server = shutil.which("redis-server")
    if redis_server is None:
        raise RuntimeError("redis-server not found on PATH")
    command = [
        redis_server,
        "--port",
        str(port),
        "--save",
        "",
        "--appendonly",
        "no",
        "--dir",
        str(data_dir),
    ]
    return subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def stop(process: subprocess.Popen | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=3)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=10_000, help="commands per workload")
    parser.add_argument("--pipeline", type=int, default=128, help="pipeline depth")
    parser.add_argument("--clone-port", type=int, default=6381)
    parser.add_argument("--redis-port", type=int, default=6382)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--external-clone", action="store_true", help="do not start the clone")
    parser.add_argument("--external-redis", action="store_true", help="do not start redis-server")
    parser.add_argument("--workload", action="append", help="run only a named workload")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    tmp = tempfile.TemporaryDirectory(prefix="redis-perf-")
    clone = Server("clone", args.host, args.clone_port)
    redis = Server("redis", args.host, args.redis_port)

    try:
        if not args.external_clone:
            clone.process = start_clone(args.clone_port)
        if not args.external_redis:
            redis.process = start_redis(args.redis_port, Path(tmp.name))

        wait_for_port(clone.host, clone.port)
        wait_for_port(redis.host, redis.port)

        workloads = make_workloads(args.count)
        if args.workload:
            wanted = set(args.workload)
            workloads = [workload for workload in workloads if workload.name in wanted]
            unknown = wanted - {workload.name for workload in workloads}
            if unknown:
                raise RuntimeError(f"unknown workloads: {', '.join(sorted(unknown))}")

        print(f"commands/workload={args.count} pipeline={args.pipeline}")
        print(f"{'workload':<14} {'clone ops/s':>14} {'redis ops/s':>14} {'clone/redis':>12}")
        print("-" * 59)
        for workload in workloads:
            prepare_for_workload(clone, workload, args.count)
            prepare_for_workload(redis, workload, args.count)
            clone_ops = run_commands(clone, workload.commands, args.pipeline)
            redis_ops = run_commands(redis, workload.commands, args.pipeline)
            ratio = clone_ops / redis_ops if redis_ops else 0.0
            print(f"{workload.name:<14} {clone_ops:>14,.0f} {redis_ops:>14,.0f} {ratio:>11.2%}")
        return 0
    finally:
        stop(clone.process)
        stop(redis.process)
        tmp.cleanup()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)

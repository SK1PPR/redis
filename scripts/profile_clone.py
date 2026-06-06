#!/usr/bin/env python3
"""Run a benchmark workload while sampling the clone process on macOS."""

from __future__ import annotations

import argparse
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]


def encode_command(parts: Iterable[str]) -> bytes:
    encoded = [part.encode("utf-8") for part in parts]
    out = bytearray(f"*{len(encoded)}\r\n".encode("ascii"))
    for part in encoded:
        out.extend(f"${len(part)}\r\n".encode("ascii"))
        out.extend(part)
        out.extend(b"\r\n")
    return bytes(out)


def wait_for_clone(host: str, port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.25) as sock:
                sock.sendall(encode_command(["PING"]))
                sock.recv(128)
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"clone did not become ready on {host}:{port}")


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
    parser.add_argument("--workload", required=True)
    parser.add_argument("--count", type=int, default=50_000)
    parser.add_argument("--pipeline", type=int, default=128)
    parser.add_argument("--seconds", type=int, default=8)
    parser.add_argument("--clone-port", type=int, default=6381)
    parser.add_argument("--redis-port", type=int, default=6382)
    parser.add_argument("--host", default="127.0.0.1")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    profile_dir = ROOT / "target" / "profiles"
    profile_dir.mkdir(parents=True, exist_ok=True)
    profile_path = profile_dir / f"{args.workload}-{int(time.time())}.sample.txt"

    clone = subprocess.Popen(
        [str(ROOT / "target" / "release" / "redis-rs"), "--port", str(args.clone_port)],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        wait_for_clone(args.host, args.clone_port)
        sampler = subprocess.Popen(
            ["sample", str(clone.pid), str(args.seconds), "-file", str(profile_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        bench = subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts" / "perf_compare.py"),
                "--external-clone",
                "--workload",
                args.workload,
                "--count",
                str(args.count),
                "--pipeline",
                str(args.pipeline),
                "--clone-port",
                str(args.clone_port),
                "--redis-port",
                str(args.redis_port),
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        sampler_output, _ = sampler.communicate(timeout=args.seconds + 10)

        print(bench.stdout, end="")
        if bench.stderr:
            print(bench.stderr, file=sys.stderr, end="")
        print(f"profile={profile_path}")
        if sampler.returncode != 0:
            print(sampler_output, file=sys.stderr, end="")
        return bench.returncode or sampler.returncode
    finally:
        stop(clone)


if __name__ == "__main__":
    raise SystemExit(main())

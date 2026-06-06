#!/usr/bin/env python3
"""Render SVG charts for the performance report without external packages."""

from __future__ import annotations

import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "performance"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def svg_escape(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def bar_chart(rows: list[dict[str, str]], path: Path) -> None:
    width = 1100
    row_h = 44
    top = 80
    left = 150
    chart_w = 760
    height = top + row_h * len(rows) + 80
    max_ops = max(float(row["redis_ops_s"]) for row in rows)
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#fbfaf7"/>',
        '<text x="40" y="42" font-family="Inter, Arial, sans-serif" font-size="28" font-weight="700" fill="#1f2933">Redis clone vs Redis throughput</text>',
        '<text x="40" y="66" font-family="Inter, Arial, sans-serif" font-size="14" fill="#52616b">Commands/second, count=2000, pipeline=128. Higher is better.</text>',
    ]
    for i, row in enumerate(rows):
        y = top + i * row_h
        clone = float(row["clone_ops_s"])
        redis = float(row["redis_ops_s"])
        clone_w = clone / max_ops * chart_w
        redis_w = redis / max_ops * chart_w
        label = svg_escape(row["workload"])
        lines.extend(
            [
                f'<text x="40" y="{y + 24}" font-family="Inter, Arial, sans-serif" font-size="14" fill="#1f2933">{label}</text>',
                f'<rect x="{left}" y="{y + 4}" width="{redis_w:.1f}" height="14" fill="#d7dde2"/>',
                f'<rect x="{left}" y="{y + 23}" width="{clone_w:.1f}" height="14" fill="#2f7dd1"/>',
                f'<text x="{left + chart_w + 16}" y="{y + 16}" font-family="Inter, Arial, sans-serif" font-size="12" fill="#52616b">Redis {redis:,.0f}</text>',
                f'<text x="{left + chart_w + 16}" y="{y + 35}" font-family="Inter, Arial, sans-serif" font-size="12" fill="#1f2933">Clone {clone:,.0f} ({float(row["clone_percent"]):.0f}%)</text>',
            ]
        )
    lines.extend(
        [
            f'<rect x="{left}" y="{height - 48}" width="16" height="10" fill="#2f7dd1"/><text x="{left + 22}" y="{height - 39}" font-family="Inter, Arial, sans-serif" font-size="12" fill="#1f2933">clone</text>',
            f'<rect x="{left + 92}" y="{height - 48}" width="16" height="10" fill="#d7dde2"/><text x="{left + 114}" y="{height - 39}" font-family="Inter, Arial, sans-serif" font-size="12" fill="#52616b">Redis</text>',
            "</svg>",
        ]
    )
    path.write_text("\n".join(lines))


def improvement_chart(rows: list[dict[str, str]], path: Path) -> None:
    width = 980
    row_h = 56
    top = 84
    left = 160
    chart_w = 560
    height = top + row_h * len(rows) + 80
    max_speedup = max(float(row["speedup"]) for row in rows)
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#fbfaf7"/>',
        '<text x="40" y="42" font-family="Inter, Arial, sans-serif" font-size="28" font-weight="700" fill="#1f2933">Profile-guided speedups</text>',
        '<text x="40" y="66" font-family="Inter, Arial, sans-serif" font-size="14" fill="#52616b">Before vs after clone throughput on large-cardinality workloads.</text>',
    ]
    for i, row in enumerate(rows):
        y = top + i * row_h
        speedup = float(row["speedup"])
        w = speedup / max_speedup * chart_w
        lines.extend(
            [
                f'<text x="40" y="{y + 28}" font-family="Inter, Arial, sans-serif" font-size="14" fill="#1f2933">{svg_escape(row["workload"])}</text>',
                f'<rect x="{left}" y="{y + 8}" width="{w:.1f}" height="24" rx="4" fill="#0f9f6e"/>',
                f'<text x="{left + w + 12}" y="{y + 26}" font-family="Inter, Arial, sans-serif" font-size="13" fill="#1f2933">{speedup:.1f}x</text>',
                f'<text x="{left}" y="{y + 49}" font-family="Inter, Arial, sans-serif" font-size="12" fill="#52616b">{float(row["before_clone_ops_s"]):,.0f} -> {float(row["after_clone_ops_s"]):,.0f} ops/s</text>',
            ]
        )
    lines.append("</svg>")
    path.write_text("\n".join(lines))


def main() -> None:
    bar_chart(read_csv(OUT / "final_benchmark.csv"), OUT / "clone_vs_redis.svg")
    improvement_chart(read_csv(OUT / "profile_improvements.csv"), OUT / "profile_guided_speedups.svg")


if __name__ == "__main__":
    main()

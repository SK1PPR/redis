#!/bin/sh
#
# Simple helper to run the Redis server locally

set -e

cd "$(dirname "$0")"
exec cargo run --release -- "$@"

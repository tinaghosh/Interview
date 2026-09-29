#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "$0")" && pwd)"
output_dir="$repo_dir/out"

rm -rf "$output_dir"
mkdir -p "$output_dir"

find "$repo_dir/src/main/java" "$repo_dir/src/test/java" -name '*.java' -print0 \
  | xargs -0 javac --release 17 -d "$output_dir"

java -cp "$output_dir" com.example.tokenvalidation.StarterTestRunner
java -cp "$output_dir" com.example.tokenvalidation.CircuitBreakerTestRunner
java -cp "$output_dir" com.example.tokenvalidation.TimeoutTestRunner
java -cp "$output_dir" com.example.tokenvalidation.ServiceCircuitBreakerTestRunner

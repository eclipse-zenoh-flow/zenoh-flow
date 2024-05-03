#
# Copyright © 2024 ZettaScale Technology <contact@zettascale.tech>
#
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
# which is available at https://www.apache.org/licenses/LICENSE-2.0.
#
# SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
#
# Contributors:
#   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
#

TARGET_DIR := `pwd` / "target"
OUT_FILE := `pwd` / "greetings.txt"
BUILD := "debug"

# Perform all verification on the code
code-checks:
    cargo build -p zenoh-flow-runtime --no-default-features
    cargo build -p zenoh-flow-runtime --no-default-features --features zenoh
    cargo build -p zenoh-flow-daemon
    cargo nextest run
    cargo test --doc
    cargo clippy --all-targets -- --deny warnings
    cargo +nightly fmt --check

# Test the standalone-runtime executable with the getting-started flow
test-standalone-runtime: standalone-runtime--bg
    @sleep 3
    z_put -k "zf/getting-started/hello" -v "standalone runtime"
    @sleep 3
    diff {{ OUT_FILE }} tests/expected-standalone-runtime.txt
    killall zenoh-flow-standalone-runtime
    rm {{ OUT_FILE }}

[macos]
standalone-runtime--bg: build-runtime build-examples
    RUST_LOG=zenoh_flow=trace ./target/debug/zenoh-flow-standalone-runtime \
        --vars TARGET_DIR={{ TARGET_DIR }} \
        --vars BUILD={{ BUILD }} \
        --vars DLL_EXTENSION="dylib" \
        --vars OUT_FILE={{ OUT_FILE }} \
        ./examples/flows/getting-started.yaml &

[linux]
standalone-runtime--bg: build-runtime build-examples
    RUST_LOG=zenoh_flow=trace ./target/debug/zenoh-flow-standalone-runtime \
        --vars TARGET_DIR={{ TARGET_DIR }} \
        --vars BUILD={{ BUILD }} \
        --vars DLL_EXTENSION="so" \
        --vars OUT_FILE={{ OUT_FILE }} \
        ./examples/flows/getting-started.yaml &

# Build just the standalone-runtime.
build-runtime:
    cargo build -p zenoh-flow-standalone-runtime

# Build all the examples.
build-examples:
    cargo build --examples

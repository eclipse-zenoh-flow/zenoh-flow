TARGET_DIR := `pwd` / "target"
OUT_FILE := `pwd` / "greetings.txt"
BUILD := "debug"

# Perform all verification on the code
code-checks:
    cargo nextest run
    cargo test --doc
    cargo clippy --all-targets -- --deny warnings
    cargo fmt --check

# Test the standalone-runtime executable with the getting-started flow
test-standalone-runtime: standalone-runtime--bg
    @sleep 3
    z_put -k "zf/getting-started/hello" -v "standalone runtime"
    @sleep 3
    diff {{ OUT_FILE }} tests/expected-standalone-runtime.txt
    killall zenoh-flow-standalone-runtime

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

TARGET_DIR := `pwd` / "target"
OUT_FILE := `pwd` / "greetings.txt"

# Perform all verification on the code
code-checks:
    cargo nextest run
    cargo test --doc
    cargo clippy --all-targets -- --deny warnings
    cargo fmt --check

# Test the standalone-runtime executable with the getting-started flow
[macos]
test-standalone-runtime: build-runtime build-examples
    RUST_LOG=zenoh_flow=trace ./target/debug/zenoh-flow-standalone-runtime -- \
        --vars TARGET_DIR={{ TARGET_DIR }} \
        --vars DLL_EXTENSION="dylib" \
        --vars OUT_FILE={{ OUT_FILE }} \
        ./examples/flows/getting-started.yaml &
    sleep 2
    z_put -k "zf/getting-started/hello" -v "standalone runtime"
    sleep 1
    diff {{ OUT_FILE }} tests/expected-standalone-runtime.txt

# Build just the standalone-runtime.
build-runtime:
    cargo build -p zenoh-flow-standalone-runtime

# Build all the examples.
build-examples:
    cargo build --examples

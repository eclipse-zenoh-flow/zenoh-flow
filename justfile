# Perform all verification on the code
code-checks:
    cargo nextest run
    cargo test --doc
    cargo clippy --all-targets -- --deny warnings
    cargo fmt --check

# Build all targets.
build: build-daemon build-runtime build-zfctl build-plugin build-examples

# Runs a Zenoh-Flow standalone daemon named "foo" in the background.
daemon-foo--bg: build-daemon
    ./target/debug/zenoh-flow-standalone-daemon foo -r 6f120064965aaf023c158bc86158ad5d &

# Runs a Zenoh-Flow standalone daemon named "bar" in the background.
daemon-bar--bg: build-daemon
    ./target/debug/zenoh-flow-standalone-daemon bar -r d87b5e0b9d33ddf3850e83b52857a29f &

build-daemon:
    cargo build -p zenoh-flow-standalone-daemon

build-plugin:
    cargo build -p zenoh-plugin-zenoh-flow

build-zfctl:
    cargo build -p zfctl

getting-started: build-runtime build-examples
    RUST_LOG=zenoh_flow=trace ./target/debug/zenoh-flow-standalone-runtime ./examples/flows/getting-started.yaml

# Test the standalone-runtime executable with the getting-started flow
test-standalone-runtime: build-runtime build-examples
    RUST_LOG=zenoh_flow=trace ./target/debug/zenoh-flow-standalone-runtime ./examples/flows/getting-started.yaml &
    sleep 2
    z_put -k "zf/getting-started/hello" -v "standalone runtime"
    sleep 1
    diff /tmp/greetings.txt tests/expected-standalone-runtime.txt

build-runtime:
    cargo build -p zenoh-flow-standalone-runtime

build-examples:
    cargo build --examples

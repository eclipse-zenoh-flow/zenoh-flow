# Zenoh-Flow Examples

## Build

You can build the CLI with the following command:
```bash
cargo build
```

To build all the Zenoh-Flow node libraries used in the examples, run:
```bash
cargo build --examples
```

Alternatively, you can build a single Zenoh-Flow node library using the following command:
```bash
cargo build --example <node>
```

## Run the Examples

The examples are organized into two folders:
- [flows](./flows): Contains all the dataflow configuration files (`.yaml`).
- [examples](./examples): Contains all the Rust source code for the nodes.

You can run any flow **locally** with the following command:
```bash
./target/debug/zfctl run-local --vars TARGET_DIR=$(pwd)/target ./examples/flows/<name>.yaml
```

For example:
```bash
./target/debug/zfctl run-local --vars TARGET_DIR=$(pwd)/target ./examples/flows/getting-started.yaml
```

Additionally, you can override variables other than `TARGET_DIR`. For example:
- `BUILD=debug` or `BUILD=release`
- `DLL_EXTENSION=so`, `DLL_EXTENSION=dylib`, or `DLL_EXTENSION=dll`

### Getting Started

This example writes everything it receives under the key expression `zf/getting-started/hello` into the file `/tmp/greetings.txt`.

1. Run the dataflow:
   ```bash
   ./target/debug/zfctl run-local --vars TARGET_DIR=$(pwd)/target ./examples/flows/getting-started.yaml
   ```

2. Monitor the output file:
   ```bash
   tail -f /tmp/greetings.txt
   ```

3. Publish a message to `zf/getting-started/hello`. This can be done using a `zenohd` router with REST enabled:
   ```bash
   zenohd --rest-http-port 8000
   ```
   ```bash
   curl -X PUT -d 'world' http://localhost:8000/zf/getting-started/hello
   ```

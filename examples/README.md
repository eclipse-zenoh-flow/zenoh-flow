# Zenoh-Flow examples

## How to run

### Build

We can create all the zenoh-flow node libraries used in the examples with the following command:
   ```bash
  cargo build --examples
   ```

Alternatively, we can create a single library of a zenoh-flow node with the following command:
   ```bash
  cargo build --example name_node 

  i.e.
  cargo build --example greetings-maker
   ```

### Update the paths

For each YAML file in the list below, check that the paths and filenames are
correct:
- flows/period-miss-detector.yaml 
- flows/getting-started.yaml
- examples/file-writer/file-writer.yaml
- examples/greetings-maker/greetings-maker.yaml
- examples/period-miss-detector/period-miss-detector.yaml

### Launch

#### 1st terminal: Zenoh

```shell
cd ~/dev/zenoh && ./target/debug/zenohd -c ~/.config/zenoh-flow/zenoh.json
```

#### 2nd terminal: Zenoh-Flow daemon

```shell
cd ~/dev/zenoh-flow/ && ./target/debug/zenoh-flow-daemon -c ~/.config/zenoh-flow/runtime.yaml
```

#### 3rd terminal: launch the flow

```shell
cd ~/dev/zenoh-flow && ./target/debug/zfctl launch ~/dev/zenoh-flow/examples/data-flow.yaml
```

Then, if the flow was successfully launched, put values at regular intervals into the "greetings-maker" example:

```shell
# If you have compiled the `z_put` example of Zenoh in debug
$ZENOH/target/debug/examples/z_put -k "zf/getting-started/hello" -v "Alice"
```

Alternatively, in the "period-miss-detector" example, put values at regular intervals with:

```shell
cd ~/dev/zenoh && ./target/debug/examples/z_put -k "zf/period-miss-detector" -v "3.1416"
```
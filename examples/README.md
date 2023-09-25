# Zenoh-Flow examples

## How to run

### Build

   With the follows command it's possible to build only the examples :
   ```bash
  cargo build --examples
   ```

   with the follows command it's possible to build only a single example:
   ```bash
  cargo build --example name_example 
   ```
### Update the paths

For each YAML file in the list below, check that the paths and filenames are
correct:
- data-flow.yaml
- examples/period-miss-detector/period-miss-detector.yaml
- examples/file-writer/file-writer.yaml
- examples/greetings-maker/greetings-maker.yaml


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

Then, if the flow was successfully launched, put values at regular intervals:

```shell
# If you have compiled the `z_put` example of Zenoh in debug
$ZENOH/target/debug/examples/z_put -k "zf/getting-started/hello" -v "Alice"
```

```shell
cd ~/dev/zenoh && ./target/debug/examples/z_put -k "zf/period-miss-detector" -v "3.1416"
```
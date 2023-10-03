# Zenoh-Flow examples

## How to run

### Build

We can create all the zenoh-flow node libraries used in the examples with the following command:
   ```bash
  cargo build --examples
   ```

Alternatively, we can create a single library of a zenoh-flow node with the following command:
   ```bash
  cargo build --example <node>
   ```

### Update and run the examples
For each YAML file in the list below, check that the paths and filenames are
correct:
- flows/period-miss-detector.yaml 
- flows/getting-started.yaml
- examples/file-writer/file-writer.yaml
- examples/greetings-maker/greetings-maker.yaml
- examples/period-miss-detector/period-miss-detector.yaml

#### 1st terminal: Launch Zenoh router
For more details you can consult the [documentation](https://github.com/eclipse-zenoh/zenoh-flow/wiki/Installation-(v0.4.0)#zenoh-plugin)

```shell
cd ~/dev/zenoh && ./target/debug/zenohd -c ~/.config/zenoh-flow/zenoh.json
```

#### 2nd terminal: Zenoh-Flow daemon

```shell
cd ~/dev/zenoh-flow/ && ./target/debug/zenoh-flow-daemon -c ~/.config/zenoh-flow/runtime.yaml
```

#### 3rd terminal: launch the flow

```shell
./target/debug/zfctl launch ~/dev/zenoh-flow/examples/data-flow.yaml
```

If you have enabled the REST plugin of Zenoh
```shell
curl -X PUT -d '"Hello World!"' http://localhost:8000/zf/gettig-started/hello
```

For the "period-miss-detector" example:

```shell
curl -X PUT -d ‘2340' http://localhost:8000/zf/period-miss-detector
```
#### 4rd terminal:

The Sink node used in both examples creates a text file where the node writes the strings it receives.
We can see the "getting-started" test file with:

```
tail -f /tmp/greetings.txt
```

For the "period-miss-detector" example:

```
tail -f /tmp/period-log.txt
```


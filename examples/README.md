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

### Configure and run the examples

We first have to update all the occurrences of `{{ BASE_DIR }}` in the YAML descriptors to match our system.

#### Launch the flow

```shell
./target/debug/zfctl launch ~/dev/zenoh-flow/examples/data-flow.yaml
```

If you have enabled the REST plugin of Zenoh
```shell
curl -X PUT -d 'world' http://localhost:8000/zf/getting-started/hello
```

For the "period-miss-detector" example:

```shell
curl -X PUT -d '2340' http://localhost:8000/zf/period-miss-detector
```
#### Show the result:

The Sink node used in both examples creates a text file where the node writes the strings it receives.
We can see the "getting-started" test file with:

```
tail -f /tmp/greetings.txt
```

For the "period-miss-detector" example:

```
tail -f /tmp/period-log.txt
```


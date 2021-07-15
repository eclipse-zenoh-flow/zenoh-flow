# Eclipse Zenoh Flow

Zenoh Flow aims at providing a Zenoh-based dataflow programming framework for computations that span from the cloud to the device.

:warning: **This software is still in alpha status and should _not_ be used in production. Breaking changes are likely to happen and the API is not stable.**

-----------
## Description

Users can describe a dataflow "pipeline" that should run on one or multiple Zenoh Flow instances via a `yaml` file. This file constitutes the entry point of Zenoh Flow.

A pipeline is composed of set of _sources_ — producing data, _operators_ — computing over the data, and _sinks_ — consuming the resulting data. These components are _dynamically_ loaded at runtime.

The different instances leverage Zenoh’s publish-subscribe model to communicate in a transparent manner for the user, i.e. without any configuration or intervention as Zenoh is built-in.

We provide several working examples that illustrate how to author components and the yaml file describing a dataflow pipeline.

-----------
## How to build it

Install [Cargo and Rust](https://doc.rust-lang.org/cargo/getting-started/installation.html). Zenoh Flow can be successfully compiled with Rust stable (>= 1.5.1), so no special configuration is required — except for certain examples.

To build Zenoh Flow, just type the following command after having followed the previous instructions:

```bash
$ cargo build --release
```

-----------
## How to run

If you launched the previous command, the Zenoh Flow runtime is located in `target/release/runtime`. This executable expects the following arguments:

- the path of the dataflow graph to execute: `--graph-file zenoh-flow-examples/graphs/fizz_buzz_pipeline.yaml`,
- a name for the runtime: `--runtime foo`.

The graph describes the different components composing the dataflow. Although mandatory, the name of the runtime is used to "deploy" the graph on different "runtime instances" (see the related examples).

-----------
## Examples

### FizzBuzz

First, compile the relevant examples:

```bash
cargo build --example manual-source --example example-fizz --example example-buzz --example generic-sink
```

This will create, depending on your OS, the libraries that the pipeline will fetch.

#### Single runtime

To run all components on the same Zenoh Flow runtime:

```bash
./target/release/runtime --graph-file zenoh-flow-examples/graphs/fizz_buzz_pipeline.yaml --runtime foo
```

_Note: in that particular case the `--runtime foo` is discarded._

#### Multiple runtimes

In a first machine, run:

```bash
./target/release/runtime --graph-file zenoh-flow-examples/graphs/fizz-buzz-multiple-runtimes.yaml --runtime foo
```

In a second machine, run:

```bash
./target/release/runtime --graph-file zenoh-flow-examples/graphs/fizz-buzz-multiple-runtimes.yaml --runtime bar
```

:warning: If you change the name of the runtime in the yaml file, the name(s) passed as argument of the previous commands must be changed accordingly.

:warning: Without configuration, the different machines need to be on the _same local network_ for this example to work. See how to add a [Zenoh router](https://zenoh.io/docs/getting-started/key-concepts/#zenoh-router) if you want to connect them through the internet.

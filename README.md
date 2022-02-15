# Eclipse Zenoh-Flow

[![Join the chat at https://gitter.im/atolab/zenoh-flow](https://badges.gitter.im/atolab/zenoh-flow.svg)](https://gitter.im/atolab/zenoh-flow?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Zenoh-Flow provides a zenoh-based dataflow programming framework for computations that span from the cloud to the device.

:warning: **This software is still in alpha status and should _not_ be used in production. Breaking changes are likely to happen and the API is not stable.**

-----------
## Description

Zenoh-Flow allow users to declare a dataflow graph, via a YAML file, and use tags to express location affinity and requirements for the operators that makeup the graph. When deploying the dataflow graph, Zenoh-Flow automatically deals with distribution by linking remote operators through zenoh.

A dataflow is composed of set of _nodes_: _sources_ — producing data, _operators_ — computing over the data, and _sinks_ — consuming the resulting data. These nodes are _dynamically_ loaded at runtime.

Remote source, operators, and sinks leverage zenoh to communicate in a transparent manner. In other terms, the dataflow the dafalow graph retails location transparency and could be deployed in different ways depending on specific needs.

Zenoh-Flow provides several working examples that illustrate how to define operators, sources and sinks as well as how to declaratively define they dataflow graph by means of a YAML file.

-----------
## How to build it

Install [Cargo and Rust](https://doc.rust-lang.org/cargo/getting-started/installation.html). Zenoh Flow can be successfully compiled with Rust stable (>= 1.5.1), so no special configuration is required — except for certain examples.

To build Zenoh-Flow, just type the following command after having followed the previous instructions:

```bash
$ cargo build --release
```


-----------
## How to build the docs

To build Zenoh-Flow documentation, just type the following command after having followed the previous instructions:

```bash
$ cargo doc
```

The HTML documentation can then be found under `./target/doc/zenoh_flow/index.html`.


-----------
## How to run

Assuming that the previous steps completed successfully, you'll find the Zenoh-Flow runtime under `target/release/runtime`. This executable expects the following arguments:

- the path of the dataflow graph to execute: `--graph-file zenoh-flow-examples/graphs/fizz_buzz_pipeline.yaml`,
- a name for the runtime: `--runtime foo`.

The graph describes the different nodes composing the dataflow. Although mandatory, the name of the runtime is used to "deploy" the graph on different "runtime instances" (see the related examples).


-----------
## Creating your nodes

Assuming that the build steps completed successfully, you'll be able to use the `cargo zenoh-flow` subcommand to create a boilerplate for your nodes.
First let's ensure to have the `cargo-zenoh-flow` binary in the Cargo path.

```bash
$ ln -s $(pwd)/target/release/cargo-zenoh-flow ~/.cargo/bin/
```

Then you can create your own node with:

```bash
$ cd ~
$ cargo zenoh-flow new myoperator
```

By default `cargo zenoh-flow` generates the template for an operator. In order to create a source or a sink you need to add either `--kind source` or `--kind sink`.\
The `Cargo.toml` will contain metadata information (eg. the inputs/outputs) used during the build process to generate the descriptor.


More information about the `cargo zenoh-flow` can be obtained using `cargo zenoh-flow --help`.\
You can now modify the `src/lib.rs` file with your business logic and update the `Cargo.toml` according to the `inputs/outputs` that you need.

Once you are done you can build it:

```bash
$ cargo zenoh-flow build
```

It will provide you the path of the descriptor for the new node, that can be used inside a flow descriptor.

-----------
## Examples

Examples can be found in our [example repository](https://github.com/atolab/zenoh-flow-examples).
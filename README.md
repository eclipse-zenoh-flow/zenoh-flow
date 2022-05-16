<img src="https://raw.githubusercontent.com/eclipse-zenoh/zenoh/master/zenoh-dragon.png" height="150">


[![Eclipse CI](https://ci.eclipse.org/zenoh/buildStatus/icon?job=zenoh-flow-nightly&subject=Eclipse%20CI)](https://ci.eclipse.org/zenoh/view/Zenoh%20Flow/job/zenoh-flow-nightly/)
[![CI](https://github.com/eclipse-zenoh/zenoh-flow/actions/workflows/ci.yml/badge.svg)](https://github.com/eclipse-zenoh/zenoh-flow/actions/workflows/ci.yml)
[![Discussion](https://img.shields.io/badge/discussion-on%20github-blue)](https://github.com/eclipse-zenoh/roadmap/discussions)
[![Discord](https://img.shields.io/badge/chat-on%20discord-blue)](https://discord.gg/vSDSpqnbkm)


# Eclipse Zenoh-Flow

Zenoh-Flow is the union of Zenoh and data flow programming: a declarative framework for computations that span from the _Cloud_ to the _Thing_.

:warning: **This software is still in alpha status and should _not_ be used in production. Breaking changes are likely to happen and the API is not yet stable.**
:warning: **The documentation is still scarce. Do not hesitate to contact us on Discord.**

## Description

Zenoh-Flow aims at simplifying and structuring (i) the _declaration_, (ii) the _deployment_ and (iii) the _writing_ of "complex" applications that can span from the Cloud to the Thing (or close to it).

To these ends, Zenoh-Flow leverages the _data flow programming model_ --- where applications are viewed as a directed graph of computing units, and _Zenoh_ --- an Edge-native, data-centric, location transparent, communication middleware.

This makes for a powerful combination as Zenoh offers flexibility and extensibility while data flow programming structures computations. The main benefit of this approach is that this allows us to decorrelate applications from the underlying infrastructure: data are published and subscribed to (_automatically_ with Zenoh-Flow) without the need to know where they are actually located.


## Core principles

Zenoh-Flow centers the definition of an application around a **description file**. This file acts as a contract that Zenoh-Flow will enforce.

In it, developers specify the different computation units --- the _nodes_, how they are connected --- the _links_, and how they should be deployed --- the _mapping_.

After validating these specifications, Zenoh-Flow will first create the necessary connections and then load each node. The types of connections created as well as the way nodes are loaded are discussed in more details [here](). The most notable aspect is that Zenoh-Flow optimizes the connections: data will go through the network only if nodes are located on different machines.


## Documentation

To build the documentation:

```bash
$ cargo doc
```

The HTML documentation can then be found under `./target/doc/zenoh_flow/index.html`.


## How to use

A working [Cargo and Rust](https://doc.rust-lang.org/cargo/getting-started/installation.html) installation is a pre-requisite.

Then download the repository:

```bash
git clone https://github.com/eclipse-zenoh/zenoh-flow && cd zenoh-flow
```

We assume in the following that `./` points to the root of this repository.


### Start Zenoh

As its name indicates, Zenoh-Flow relies on Zenoh. So you first need to install and start a Zenoh router on your device.

The instructions to install Zenoh are located [here](https://zenoh.io/docs/getting-started/installation/).

Once installed, you need to start a `zenohd` with the configuration we provide:

```bash
zenohd -c ./zenoh-flow-daemon/etc/zenoh-zf-router.json
```

With this configuration, Zenoh will start storages for specific keys that are used internally by Zenoh-Flow. These keys are notably what allow Zenoh-Flow daemons to discover each other (as long as the routers, to which the daemons are attached, can communicate).


### Start Zenoh-Flow

Build Zenoh-Flow in release:

```bash
cd zenoh-flow && cargo build --release
```

This will produce the following executables under the `target/release` directory: `zenoh-flow-daemon`, `zfctl` and `cargo-zenoh-flow`.

- `zenoh-flow-daemon` is what will take care of starting and stopping nodes, as well as deploying a Zenoh-Flow application.
- `zfctl` is what we use to interact (using Zenoh) with all the `zenoh-flow-daemon` discovered.
- `cargo-zenoh-flow` is a help tool that produces boilerplate code for Zenoh-Flow nodes.

To launch an application we need to: 1) start a daemon and 2) interact with it through `zfctl`.

A Zenoh-Flow daemon relies on some configurations and variables. For this to work, we need to move few files:

```bash
sudo mkdir -p /etc/zenoh-flow/extensions.d
sudo cp ./zenoh-flow-daemon/etc/runtime.yaml /etc/zenoh-flow
sudo cp ./zenoh-flow-daemon/etc/zenoh-daemon.json /etc/zenoh-flow
```

We can then start the daemon:

```bash
./target/release/zenoh-flow-daemon
```

Next, `zfctl`. We also need to copy a configuration file:

```bash
mkdir -p ~/.config/zenoh-flow
cp ./zfctl/.config/zfctl-zenoh.json ~/.config/zenoh-flow
```

To check that the Zenoh-Flow daemon is correctly running and `zfctl` is set up, you can do:

```bash
./target/release/zfctl list runtimes
```

This should return a list just like this one:

```
+--------------------------------------+---------------------------+--------+
| UUID                                 | Name                      | Status |
+--------------------------------------+---------------------------+--------+
| 49936f69-2c87-55f0-9df4-d1fba2fadd38 | Juliens-MacBook-Pro.local | Ready  |
+--------------------------------------+---------------------------+--------+
```

If you see this, you can now launch applications!
Assuming your application is described in `app.yaml`, you would launch it via:

```bash
./target/release/zfctl launch app.yaml > app.uuid
```

:book: The redirection of the standard output is to "capture" the unique identifier associated to this instance of your application.

And you can stop everything via:

```bash
./target/release/zfctl destroy "$(cat app.uuid)"
```

We encourage you to look at the examples available in our [examples repository](https://github.com/ZettaScaleLabs/zenoh-flow-examples) for more!

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

-----

🧑‍💻 We are currently keeping our documentation and guides in the [Wiki](https://github.com/eclipse-zenoh/zenoh-flow/wiki) tab of this repository.

-----

## Installation

Follow our guide [here](https://github.com/eclipse-zenoh/zenoh-flow/wiki/Installation-(v0.4.0))!

## Getting Started

The best way to learn Zenoh-Flow is to go through our [getting started guide](https://github.com/eclipse-zenoh/zenoh-flow/wiki/Getting-started-(v0.4.0)).

## Examples

We encourage you to look at the examples available in our [examples repository](https://github.com/ZettaScaleLabs/zenoh-flow-examples).

🚗 If you still want more, we also ported an [Autonomous Driving Pipeline](https://github.com/ZettaScaleLabs/stunt)!

compile the examples using "cargo build --examples" 

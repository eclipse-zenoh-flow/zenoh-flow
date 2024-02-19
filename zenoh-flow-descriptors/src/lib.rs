//
// Copyright (c) 2021 - 2023 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

//! This crate centralises the different *descriptors* used in Zenoh-Flow.
//!
//! Descriptors describe the different parts that compose an application: the nodes (Source(s), Operator(s) and
//! Sink(s)), the links (how the nodes are connected) and possibly where they should run.
//!
//! Descriptors are **enforced** by Zenoh-Flow, giving precise control to application developer.
//!
//! All Rust `struct` exposed by this crate implement the [Deserialize](serde::Deserialize) and
//! [Serialize](serde::Serialize) traits. The purpose is to encourage users to describe their application in dedicated
//! files (which are then fed to a Zenoh-Flow runtime to be parsed), which eases separating the integration from the
//! development.
//!
//! The entry point in order to describe -- in a separate file -- a data flow is the [DataFlowDescriptor].
//!
//! # Note
//!
//! In its current state, Zenoh-Flow does not easily support creating your data flow through code. It is planned in a
//! future release to bring better support for this use-case.
//!
//! Users interested to do so should look into the `Flattened` family of structures, starting with the
//! [FlattenedDataFlowDescriptor].

pub(crate) mod dataflow;
pub(crate) mod flattened;
pub(crate) mod io;
pub(crate) mod nodes;
pub(crate) mod uri;

pub use dataflow::DataFlowDescriptor;
pub use flattened::dataflow::FlattenedDataFlowDescriptor;
pub use flattened::nodes::operator::FlattenedOperatorDescriptor;
pub use flattened::nodes::sink::{FlattenedSinkDescriptor, SinkVariant};
pub use flattened::nodes::source::{FlattenedSourceDescriptor, SourceVariant};

pub use io::{InputDescriptor, LinkDescriptor, OutputDescriptor};

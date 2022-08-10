//
// Copyright (c) 2022 ZettaScale Technology
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

//!
//! Zenoh Flow provides a zenoh-based data flow programming framework for
//! computations that span from the cloud to the device.
//!
//! Zenoh Flow allows users to declare a data flow graph, via a YAML file,
//! and uses tags to express location affinity and requirements for the
//! operators that makeup the graph. When deploying the data flow graph,
//! Zenoh Flow automatically deals with distribution by linking
//! remote operators through zenoh.
//!
//! A data flow is composed of set of nodes: sources — producing data, operators
//! — computing over the data, and sinks — consuming the resulting data.
//!  These nodes are dynamically loaded at runtime.
//!
//! Remote source, operators, and sinks leverage zenoh to communicate in a
//! transparent manner. In other terms, the data flow the data flow graph retails
//! location transparency and could be deployed in
//! different ways depending on specific needs.
//!
//! Zenoh Flow provides several working examples that illustrate how to
//! define operators, sources and sinks as well as how to
//! declaratively define they data flow graph by means of a YAML file.

use const_format::formatcp;

pub use ::zenoh_flow_derive;

pub use ::async_std;
pub use ::bincode;
pub use ::paste;
pub use ::serde;
pub use ::typetag;

pub mod error;
pub mod macros;
pub mod model;
pub mod runtime;
pub mod traits;
pub mod types;

pub mod prelude {
    pub use crate::error::ZFError;
    pub use crate::runtime::dataflow::instance::io::{Input, Inputs, Output, Outputs, Streams};
    pub use crate::runtime::message::{DataMessage, Message};
    pub use crate::traits::{
        AsyncIteration, Deserializable, DowncastAny, Operator, Sink, Source, ZFData,
    };
    pub use crate::types::{Configuration, Context, Data, NodeId, PortId, RuntimeId, ZFResult};
    pub use crate::{export_operator, export_sink, export_source};
}

/// Commit id of latest commit on Zenoh Flow
pub const GIT_VERSION: &str = git_version::git_version!(prefix = "v", cargo_prefix = "v");

/// Version of Zenoh Flow Cargo Package
/// This is used to verify compatibility in nodes dynamic loading.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Complete string with the Zenoh Flow version, including commit id.
pub const FULL_VERSION: &str = formatcp!("{}-{}", VERSION, GIT_VERSION);

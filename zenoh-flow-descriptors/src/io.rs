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

use serde::{Deserialize, Serialize};
use std::fmt;
use zenoh_flow_commons::{NodeId, PortId, SharedMemoryConfiguration};

/// An `InputDescriptor` describes an Input port of a Zenoh-Flow node.
///
/// FIXME@J-Loudet See [links] for their usage.
#[derive(Debug, Hash, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct InputDescriptor {
    pub node: NodeId,
    pub input: PortId,
}

impl fmt::Display for InputDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}.{}", self.node, self.input))
    }
}

impl InputDescriptor {
    pub fn new(node: impl AsRef<str>, input: impl AsRef<str>) -> Self {
        Self {
            node: node.as_ref().into(),
            input: input.as_ref().into(),
        }
    }
}

/// An `OutputDescriptor` describes an Output port of a Zenoh-Flow node.
///
/// See [LinkDescriptor][crate::link::LinkDescriptor] for their usage.
#[derive(Debug, Clone, Hash, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutputDescriptor {
    pub node: NodeId,
    pub output: PortId,
}

impl fmt::Display for OutputDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}.{}", self.node, self.output))
    }
}

impl OutputDescriptor {
    pub fn new(node: impl AsRef<str>, output: impl AsRef<str>) -> Self {
        Self {
            node: node.as_ref().into(),
            output: output.as_ref().into(),
        }
    }
}

/// A `LinkDescriptor` describes a link in Zenoh-Flow: a connection from an Output to an Input.
///
/// A link is composed of:
/// - an [OutputDescriptor],
/// - an [InputDescriptor],
/// - (optional) Zenoh shared-memory parameters.
///
/// # Example
///
/// The textual representation, in YAML, of a link is as following:
/// ```yaml
/// from:
///   node : Counter
///   output : Counter
/// to:
///   node : SumOperator
///   input : Number
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LinkDescriptor {
    pub from: OutputDescriptor,
    pub to: InputDescriptor,
    #[serde(default, alias = "shm", alias = "shared-memory")]
    pub shared_memory: Option<SharedMemoryConfiguration>,
}

impl std::fmt::Display for LinkDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} => {}", self.from, self.to)
    }
}

impl LinkDescriptor {
    pub fn new(
        from: OutputDescriptor,
        to: InputDescriptor,
        shm: SharedMemoryConfiguration,
    ) -> Self {
        Self {
            from,
            to,
            shared_memory: Some(shm),
        }
    }

    pub fn new_no_shm(from: OutputDescriptor, to: InputDescriptor) -> Self {
        Self {
            from,
            to,
            shared_memory: None,
        }
    }
}

//
// Copyright © 2021 ZettaScale Technology <contact@zettascale.tech>
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

use std::fmt;

use serde::{Deserialize, Serialize};
#[cfg(feature = "shared-memory")]
use zenoh_flow_commons::SharedMemoryConfiguration;
use zenoh_flow_commons::{NodeId, PortId};

/// An `InputDescriptor` uniquely describes an Input port of a Zenoh-Flow node.
///
/// # Example
///
/// ```
/// # use zenoh_flow_descriptors::InputDescriptor;
/// # let input_desc = r#"
/// node: Operator
/// input: i-operator
/// # "#;
/// # serde_yaml::from_str::<InputDescriptor>(input_desc).unwrap();
/// ```
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

/// An `OutputDescriptor` uniquely describes an Output port of a Zenoh-Flow node.
///
/// # Example
///
/// ```
/// # use zenoh_flow_descriptors::OutputDescriptor;
/// # let output_desc = r#"
/// node: Operator
/// output: o-operator
/// # "#;
/// # serde_yaml::from_str::<OutputDescriptor>(output_desc).unwrap();
/// ```
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
/// - *(optional, disabled by default)* Zenoh shared-memory parameters.
///
/// # Example
///
/// The textual representation, in YAML, of a link is as following:
/// ```
/// # use zenoh_flow_descriptors::LinkDescriptor;
/// # let link_desc = r#"
/// from:
///   node : Operator
///   output : o-operator
/// to:
///   node : Sink
///   input : i-sink
/// # "#;
/// # serde_yaml::from_str::<LinkDescriptor>(link_desc).unwrap();
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LinkDescriptor {
    pub from: OutputDescriptor,
    pub to: InputDescriptor,
    #[cfg(feature = "shared-memory")]
    #[serde(default, alias = "shm", alias = "shared-memory")]
    pub shared_memory: Option<SharedMemoryConfiguration>,
}

impl std::fmt::Display for LinkDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} => {}", self.from, self.to)
    }
}

impl LinkDescriptor {
    pub fn new(from: OutputDescriptor, to: InputDescriptor) -> Self {
        Self {
            from,
            to,
            #[cfg(feature = "shared-memory")]
            shared_memory: None,
        }
    }

    #[cfg(feature = "shared-memory")]
    pub fn set_shared_memory(mut self, shm: SharedMemoryConfiguration) -> Self {
        self.shared_memory = Some(shm);
        self
    }
}

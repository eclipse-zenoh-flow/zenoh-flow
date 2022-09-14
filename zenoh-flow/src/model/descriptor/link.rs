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

use crate::types::{NodeId, PortId, PortType};
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};

/// The description of a link.
///
/// Example:
///
/// ```yaml
///
/// from:
///   node : Counter
///   output : Counter
/// to:
///   node : SumOperator
///   input : Number
///
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LinkDescriptor {
    pub from: OutputDescriptor,
    pub to: InputDescriptor,
    pub size: Option<usize>,
    pub queueing_policy: Option<String>,
    pub priority: Option<usize>,
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
            size: None,
            queueing_policy: None,
            priority: None,
        }
    }
}

/// The description of a port.
///
/// Example:
///
/// ```yaml
/// id: Counter
/// type: usize
/// ```
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct PortDescriptor {
    #[serde(alias = "id")]
    pub port_id: PortId,
    #[serde(alias = "type")]
    pub port_type: PortType,
}

impl std::fmt::Display for PortDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}:{}", self.port_id, self.port_type)
    }
}

impl PortDescriptor {
    pub fn new(port_id: impl AsRef<str>, port_type: impl AsRef<str>) -> Self {
        Self {
            port_id: port_id.as_ref().into(),
            port_type: port_type.as_ref().into(),
        }
    }
}

/// Describes one output
///
/// Example:
///
/// ```yaml
/// node : Counter
/// output : Counter
/// ```
///
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

/// Describes an Output of a Composite Operator.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompositeOutputDescriptor {
    pub id: Arc<str>,
    pub node: NodeId,
    pub output: PortId,
}

impl CompositeOutputDescriptor {
    pub fn new(id: impl AsRef<str>, node: impl AsRef<str>, output: impl AsRef<str>) -> Self {
        Self {
            id: id.as_ref().into(),
            node: node.as_ref().into(),
            output: output.as_ref().into(),
        }
    }
}

impl From<CompositeOutputDescriptor> for OutputDescriptor {
    fn from(composite: CompositeOutputDescriptor) -> Self {
        Self {
            node: composite.node,
            output: composite.output,
        }
    }
}

/// Describes one input
///
/// Example:
///
/// ```yaml
/// node : SumOperator
/// input : Number
/// ```
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
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

/// Describes an Input of a Composite Operator.
///
/// # Example
///
/// ```yaml
/// id: UniquePortIdentifier
/// node: Node
/// input: Input
/// ```
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct CompositeInputDescriptor {
    pub id: Arc<str>,
    pub node: NodeId,
    pub input: PortId,
}

impl CompositeInputDescriptor {
    pub fn new(id: impl AsRef<str>, node: impl AsRef<str>, input: impl AsRef<str>) -> Self {
        Self {
            id: id.as_ref().into(),
            node: node.as_ref().into(),
            input: input.as_ref().into(),
        }
    }
}

impl From<CompositeInputDescriptor> for InputDescriptor {
    fn from(composite: CompositeInputDescriptor) -> Self {
        Self {
            node: composite.node,
            input: composite.input,
        }
    }
}

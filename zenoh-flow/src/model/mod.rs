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

pub mod connector;
pub mod dataflow;
pub mod link;
pub mod node;

use crate::error::ZFError;
use crate::model::link::PortDescriptor;
use crate::serde::{Deserialize, Serialize};
use crate::types::{DurationDescriptor, NodeId, PortId};
use std::fmt;

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

/// The kind of a graph node.
/// It is used as discriminant to understand the kind
/// of a node in the graph. (e.g. it is a source, a sink or an operator?)
/// It is used internally when all nodes are mixed inside some data structure.
///
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeKind {
    Operator,
    Sink,
    Source,
}

impl std::str::FromStr for NodeKind {
    type Err = ZFError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "operator" => Ok(Self::Operator),
            "sink" => Ok(Self::Sink),
            "source" => Ok(Self::Source),
            _ => Err(ZFError::ParsingError(
                "unable to parse node kind".to_string(),
            )),
        }
    }
}

impl std::string::ToString for NodeKind {
    fn to_string(&self) -> String {
        match self {
            Self::Operator => String::from("operator"),
            Self::Sink => String::from("sink"),
            Self::Source => String::from("source"),
        }
    }
}

impl Default for NodeKind {
    fn default() -> Self {
        NodeKind::Operator
    }
}
// Registry metadata

/// The metadata for a node stored in the registry.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RegistryNode {
    pub id: NodeId,
    pub kind: NodeKind,
    pub classes: Vec<String>,
    pub tags: Vec<RegistryNodeTag>,
    pub inputs: Vec<PortDescriptor>,
    pub outputs: Vec<PortDescriptor>,
    pub period: Option<DurationDescriptor>,
}

impl RegistryNode {
    /// Adds a the given [`RegistryNodeTag`](`RegistryNodeTag`) for this node.
    /// This is like adding a version/flavor of that node, similar to
    /// Docker's tag.
    pub fn add_tag(&mut self, tag: RegistryNodeTag) {
        let index = self.tags.iter().position(|t| t.name == tag.name);
        match index {
            Some(i) => {
                let mut old_tag = self.tags.remove(i);
                for architecture in tag.architectures.into_iter() {
                    old_tag.add_architecture(architecture);
                }
                self.tags.push(old_tag);
            }
            None => {
                self.tags.push(tag);
            }
        }
    }
}

/// The tag of a node in the graph.
/// A tag represents a version/flavor of a node.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RegistryNodeTag {
    pub name: String,
    pub requirement_labels: Vec<String>,
    pub architectures: Vec<RegistryNodeArchitecture>,
}

impl RegistryNodeTag {
    pub fn add_architecture(&mut self, arch: RegistryNodeArchitecture) {
        let index = self
            .architectures
            .iter()
            .position(|a| a.os == arch.os && a.arch == arch.arch);
        match index {
            Some(i) => {
                self.architectures.remove(i);
                self.architectures.push(arch);
            }
            None => {
                self.architectures.push(arch);
            }
        }
    }
}

/// The information about the architecure/os for a node in the registry.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RegistryNodeArchitecture {
    pub arch: String,
    pub os: String,
    pub uri: String,
    pub checksum: String,
    pub signature: String,
}

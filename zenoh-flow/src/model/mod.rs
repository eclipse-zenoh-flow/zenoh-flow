//
// Copyright (c) 2017, 2021 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//

pub mod connector;
pub mod dataflow;
pub mod deadline;
pub mod link;
pub mod node;

use crate::model::link::PortDescriptor;
use crate::serde::{Deserialize, Serialize};
use crate::ZFError;
use crate::{DurationDescriptor, NodeId, PortId};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OutputDescriptor {
    pub node: NodeId,
    pub output: PortId,
}

impl fmt::Display for OutputDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}.{}", self.node, self.output))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct InputDescriptor {
    pub node: NodeId,
    pub input: PortId,
}

impl fmt::Display for InputDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}.{}", self.node, self.input))
    }
}

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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RegistryNodeArchitecture {
    pub arch: String,
    pub os: String,
    pub uri: String,
    pub checksum: String,
    pub signature: String,
}

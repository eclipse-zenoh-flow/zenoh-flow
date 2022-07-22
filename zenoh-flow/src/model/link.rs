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

use crate::model::{InputDescriptor, OutputDescriptor};
use crate::{PortId, PortType};
use serde::{Deserialize, Serialize};

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
#[derive(Serialize, Deserialize, Debug, Clone)]
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

/// The description of a port.
///
/// Example:
///
/// ```yaml
/// id: Counter
/// type: usize
/// ```
#[derive(Debug, Serialize, Deserialize, Clone)]
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

/// The record of a link.
///
/// Example:
///
/// ```yaml
/// uid: 10
/// from:
///   node : Counter
///   output : Counter
/// to:
///   node : SumOperator
///   input : Number
///
/// ```
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LinkRecord {
    pub uid: u32,
    pub from: OutputDescriptor,
    pub to: InputDescriptor,
    pub size: Option<usize>,
    pub queueing_policy: Option<String>,
    pub priority: Option<usize>,
}

impl std::fmt::Display for LinkRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({}) {} => {}", self.uid, self.from, self.to)
    }
}

impl From<(LinkDescriptor, u32)> for LinkRecord {
    fn from(data: (LinkDescriptor, u32)) -> Self {
        let (desc, uid) = data;

        Self {
            uid,
            from: desc.from,
            to: desc.to,
            size: desc.size,
            queueing_policy: desc.queueing_policy,
            priority: desc.priority,
        }
    }
}

/// The record of a port.
///
/// Example:
///
/// ```yaml
/// id: Counter
/// uid: 3
/// type: usize
/// ```
#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct PortRecord {
    pub uid: u32,
    #[serde(alias = "id")]
    pub port_id: PortId,
    #[serde(alias = "type")]
    pub port_type: PortType,
}

impl std::fmt::Display for PortRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({}) {}:{}", self.uid, self.port_id, self.port_type)
    }
}

impl From<(PortDescriptor, u32)> for PortRecord {
    fn from(data: (PortDescriptor, u32)) -> Self {
        let (desc, uid) = data;

        Self {
            uid,
            port_id: desc.port_id,
            port_type: desc.port_type,
        }
    }
}

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

use crate::model::descriptor::{InputDescriptor, LinkDescriptor, OutputDescriptor, PortDescriptor};
use crate::types::{PortId, PortType};
use serde::{Deserialize, Serialize};

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
    pub uid: usize,
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

impl From<(LinkDescriptor, usize)> for LinkRecord {
    fn from(data: (LinkDescriptor, usize)) -> Self {
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
    pub uid: usize,
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

impl From<(PortDescriptor, usize)> for PortRecord {
    fn from(data: (PortDescriptor, usize)) -> Self {
        let (desc, uid) = data;

        Self {
            uid,
            port_id: desc.port_id,
            port_type: desc.port_type,
        }
    }
}

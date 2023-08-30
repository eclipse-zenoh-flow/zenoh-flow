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

use crate::model::descriptor::{InputDescriptor, LinkDescriptor, OutputDescriptor};
use crate::types::PortId;
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
    pub uid: u32,
    pub from: OutputDescriptor,
    pub to: InputDescriptor,
    pub shared_memory_element_size: Option<usize>,
    pub shared_memory_elements: Option<usize>,
    pub shared_memory_backoff: Option<u64>,
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
            shared_memory_element_size: desc.shared_memory_element_size,
            shared_memory_elements: desc.shared_memory_elements,
            shared_memory_backoff: desc.shared_memory_backoff,
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
/// ```
#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub struct PortRecord {
    pub uid: u32,
    #[serde(alias = "id")]
    pub port_id: PortId,
}

impl std::fmt::Display for PortRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({}) {}", self.uid, self.port_id)
    }
}

impl From<(PortId, u32)> for PortRecord {
    fn from(data: (PortId, u32)) -> Self {
        let (port_id, uid) = data;

        Self { uid, port_id }
    }
}

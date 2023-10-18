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

use super::ISubstituable;
use crate::{InputDescriptor, OutputDescriptor};

use serde::{Deserialize, Serialize};
use zenoh_flow_commons::{deserialize_id, NodeId, PortId};

/// TODO@J-Loudet example?
/// TODO@J-Loudet documentation?
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompositeOutputDescriptor {
    pub id: PortId,
    #[serde(deserialize_with = "deserialize_id")]
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

impl ISubstituable<NodeId> for CompositeOutputDescriptor {
    fn substitute(&mut self, subs: &super::Substitutions<NodeId>) {
        if let Some(new_id) = subs.get(&self.node) {
            self.node = new_id.clone();
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
    pub id: PortId,
    #[serde(deserialize_with = "deserialize_id")]
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

impl ISubstituable<NodeId> for CompositeInputDescriptor {
    fn substitute(&mut self, subs: &super::Substitutions<NodeId>) {
        if let Some(new_id) = subs.get(&self.node) {
            self.node = new_id.clone();
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

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

use crate::composite::{ISubstituable, Substitutions};
use crate::{InputDescriptor, OutputDescriptor};
use zenoh_flow_commons::{NodeId, SharedMemoryConfiguration, SharedMemoryParameters};

use serde::{Deserialize, Serialize};
use zenoh_flow_records::LinkRecord;

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
    #[serde(default)]
    pub shared_memory: SharedMemoryConfiguration,
}

impl std::fmt::Display for LinkDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} => {}", self.from, self.to)
    }
}

impl ISubstituable<NodeId> for LinkDescriptor {
    fn substitute(&mut self, subs: &Substitutions<NodeId>) {
        if let Some(new_id) = subs.get(&self.from.node) {
            self.from.node = new_id.clone();
        }

        if let Some(new_id) = subs.get(&self.to.node) {
            self.to.node = new_id.clone();
        }
    }
}

impl ISubstituable<OutputDescriptor> for LinkDescriptor {
    fn substitute(&mut self, subs: &Substitutions<OutputDescriptor>) {
        if let Some(new_output) = subs.get(&self.from) {
            self.from = new_output.clone();
        }
    }
}

impl ISubstituable<InputDescriptor> for LinkDescriptor {
    fn substitute(&mut self, subs: &Substitutions<InputDescriptor>) {
        if let Some(new_input) = subs.get(&self.to) {
            self.to = new_input.clone();
        }
    }
}

impl LinkDescriptor {
    pub fn new(from: OutputDescriptor, to: InputDescriptor) -> Self {
        Self {
            from,
            to,
            shared_memory: SharedMemoryConfiguration::default(),
        }
    }

    pub fn into_record(self, default_shared_memory: &SharedMemoryParameters) -> LinkRecord {
        LinkRecord {
            from: self.from.into(),
            to: self.to.into(),
            shared_memory: SharedMemoryParameters::from_configuration(
                &self.shared_memory,
                default_shared_memory,
            ),
        }
    }
}

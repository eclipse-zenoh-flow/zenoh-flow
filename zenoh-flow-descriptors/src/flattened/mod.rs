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

pub(crate) mod nodes;
pub use nodes::operator::FlattenedOperatorDescriptor;
pub use nodes::sink::FlattenedSinkDescriptor;
pub use nodes::source::FlattenedSourceDescriptor;

pub(crate) mod uri;

pub(crate) mod dataflow;
pub use dataflow::FlattenedDataFlowDescriptor;

pub(crate) mod validator;

use crate::nodes::operator::composite::{CompositeInputDescriptor, CompositeOutputDescriptor};
use crate::{InputDescriptor, LinkDescriptor, OutputDescriptor};
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use zenoh_flow_commons::NodeId;

/// TODO@J-Loudet documentation?
pub trait ISubstituable<T: Hash + PartialEq + Eq> {
    fn substitute(&mut self, subs: &Substitutions<T>);
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

impl ISubstituable<NodeId> for CompositeOutputDescriptor {
    fn substitute(&mut self, subs: &Substitutions<NodeId>) {
        if let Some(new_id) = subs.get(&self.node) {
            self.node = new_id.clone();
        }
    }
}

impl ISubstituable<NodeId> for CompositeInputDescriptor {
    fn substitute(&mut self, subs: &Substitutions<NodeId>) {
        if let Some(new_id) = subs.get(&self.node) {
            self.node = new_id.clone();
        }
    }
}

/// `Substitutions` is an insert only structure that keeps track of all the substitutions to perform.
///
/// It is leveraged in Zenoh-Flow during the flattening of data flows.
#[derive(Debug, PartialEq, Eq)]
pub struct Substitutions<T: Hash + PartialEq + Eq>(HashMap<T, T>);

impl<T: Hash + PartialEq + Eq> Default for Substitutions<T> {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl<T: Hash + PartialEq + Eq> Deref for Substitutions<T> {
    type Target = HashMap<T, T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Hash + PartialEq + Eq> DerefMut for Substitutions<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: Hash + PartialEq + Eq> From<HashMap<T, T>> for Substitutions<T> {
    fn from(value: HashMap<T, T>) -> Self {
        Self(value)
    }
}

impl<T: Hash + PartialEq + Eq, const N: usize> From<[(T, T); N]> for Substitutions<T> {
    fn from(value: [(T, T); N]) -> Self {
        Self(HashMap::from(value))
    }
}

impl<T: Hash + PartialEq + Eq + Display> Substitutions<T> {
    pub fn apply(&self, substituables: &mut [impl ISubstituable<T>]) {
        substituables
            .iter_mut()
            .for_each(|substituable| substituable.substitute(self))
    }
}

#[derive(Default, Debug, PartialEq, Eq)]
pub struct Patch {
    pub subs_inputs: Substitutions<InputDescriptor>,
    pub subs_outputs: Substitutions<OutputDescriptor>,
}

impl Patch {
    pub fn new(
        subs_inputs: Substitutions<InputDescriptor>,
        subs_outputs: Substitutions<OutputDescriptor>,
    ) -> Self {
        Self {
            subs_inputs,
            subs_outputs,
        }
    }

    pub fn apply(self, links: &mut [LinkDescriptor]) {
        self.subs_inputs.apply(links);
        self.subs_outputs.apply(links);
    }
}

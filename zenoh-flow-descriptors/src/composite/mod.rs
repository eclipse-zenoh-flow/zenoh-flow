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

mod io;
mod operator;

pub use io::{CompositeInputDescriptor, CompositeOutputDescriptor};
pub use operator::CompositeOperatorDescriptor;

use std::{
    collections::HashMap,
    fmt::Display,
    hash::Hash,
    ops::{Deref, DerefMut},
};

/// TODO@J-Loudet documentation?
pub trait ISubstituable<T: Hash + PartialEq + Eq> {
    fn substitute(&mut self, subs: &Substitutions<T>);
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
    // pub fn new_with(key: T, value: T) -> Self {
    //     Self(HashMap::from([(key, value)]))
    // }

    // TODO: instead of a slice, provide an iterator?
    pub fn apply(&self, substituables: &mut [impl ISubstituable<T>]) {
        substituables
            .iter_mut()
            .for_each(|substituable| substituable.substitute(self))
    }
}

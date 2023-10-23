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

mod nodes;
use std::{
    collections::HashSet,
    fmt::{Debug, Display},
    sync::Arc,
};

pub use nodes::{FlattenedOperatorDescriptor, FlattenedSinkDescriptor, FlattenedSourceDescriptor};

mod dataflow;
pub use dataflow::FlattenedDataFlowDescriptor;

use crate::vars::Vars;
use crate::{composite::Substitutions, InputDescriptor, LinkDescriptor, OutputDescriptor};
use serde::de::DeserializeOwned;
use zenoh_flow_commons::{Configuration, NodeId, Result};

pub(crate) trait IFlattenableComposite: DeserializeOwned + Display + Debug + Clone {
    type Flattened: Debug + Display;
    type Flattenable: IFlattenable<Flattened = Self::Flattened>;

    fn flatten_composite(
        self,
        id: NodeId,
        overwritting_configuration: Configuration,
        vars: Vars,
        ancestors: &mut HashSet<Arc<str>>,
    ) -> Result<(Vec<Self::Flattened>, Vec<LinkDescriptor>, Patch)>;
}

pub trait IFlattenable: DeserializeOwned + Display + Debug {
    type Flattened: Debug + Display;

    fn flatten(self, id: NodeId, overwritting_configuration: Configuration) -> Self::Flattened;
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

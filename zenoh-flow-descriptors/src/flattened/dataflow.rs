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

use crate::{
    DataFlowDescriptor, FlattenedOperatorDescriptor, FlattenedSinkDescriptor,
    FlattenedSourceDescriptor, LinkDescriptor,
};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::Arc,
};
use zenoh_flow_commons::{Configuration, NodeId, Result, RuntimeId, Vars};

use super::validator::Validator;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FlattenedDataFlowDescriptor {
    pub name: Arc<str>,
    pub sources: Vec<FlattenedSourceDescriptor>,
    pub operators: Vec<FlattenedOperatorDescriptor>,
    pub sinks: Vec<FlattenedSinkDescriptor>,
    pub links: Vec<LinkDescriptor>,
    pub mapping: HashMap<RuntimeId, HashSet<NodeId>>,
}

impl Display for FlattenedDataFlowDescriptor {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl FlattenedDataFlowDescriptor {
    pub fn try_flatten(mut data_flow: DataFlowDescriptor, vars: Vars) -> Result<Self> {
        let mut flattened_operators = Vec::with_capacity(data_flow.operators.len());
        for operator_desc in data_flow.operators {
            let operator_id = operator_desc.id.clone();
            let (mut flat_ops, mut flat_links, patch) = FlattenedOperatorDescriptor::try_flatten(
                operator_desc,
                data_flow.configuration.clone(),
                Configuration::default(),
                vars.clone(),
                &mut HashSet::default(),
            )?;

            // Update the mapping: removing the id of the composite node & adding the "leaves".
            let flattened_ids: Vec<_> = flat_ops.iter().map(|op| op.id.clone()).collect();
            for nodes in data_flow.mapping.values_mut() {
                if nodes.remove(&operator_id) {
                    nodes.extend(flattened_ids.clone().into_iter());
                }
            }

            // NOTE: This `append` has to be done after updating the mapping as it drains the content of the vector.
            flattened_operators.append(&mut flat_ops);
            patch.apply(&mut data_flow.links);
            data_flow.links.append(&mut flat_links);
        }

        let sources = data_flow
            .sources
            .into_iter()
            .map(|source_desc| {
                FlattenedSourceDescriptor::try_flatten(
                    source_desc,
                    vars.clone(),
                    data_flow.configuration.clone(),
                )
            })
            .collect::<Result<Vec<_>>>()?;

        let sinks = data_flow
            .sinks
            .into_iter()
            .map(|sink_desc| {
                FlattenedSinkDescriptor::try_flatten(
                    sink_desc,
                    vars.clone(),
                    data_flow.configuration.clone(),
                )
            })
            .collect::<Result<Vec<_>>>()?;

        let flattened_data_flow = Self {
            name: data_flow.name,
            sources,
            operators: flattened_operators,
            sinks,
            links: data_flow.links,
            mapping: data_flow.mapping,
        };

        Validator::validate(&flattened_data_flow)
            .context("The provided data flow does not appear to be valid")?;

        Ok(flattened_data_flow)
    }
}

#[cfg(test)]
#[path = "./tests.rs"]
mod tests;

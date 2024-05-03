//
// Copyright © 2021 ZettaScale Technology <contact@zettascale.tech>
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

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::Arc,
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use zenoh_flow_commons::{Configuration, InstanceId, NodeId, Result, RuntimeId, Vars};

use super::validator::Validator;
use crate::{
    DataFlowDescriptor, FlattenedOperatorDescriptor, FlattenedSinkDescriptor,
    FlattenedSourceDescriptor, LinkDescriptor,
};

/// A `FlattenedDataFlowDescriptor` is a self-contained description of a data flow.
///
/// If a `FlattenedDataFlowDescriptor` is obtained after a Zenoh-Flow runtime has processed a [DataFlowDescriptor] then
/// this flattened descriptor is also guaranteed to be *valid*: it respects a set of constraints imposed by
/// Zenoh-Flow. See the [try_flatten](FlattenedDataFlowDescriptor::try_flatten()) method for a detailed explanation.
///
/// # ⚠️ Warning: risky manual creation
///
/// Although the fields are public, manually creating an instance is risky as no validation is performed. This could
/// have many practical implications, e.g. your application could produce no data as your nodes are not (well)
/// connected.
///
/// TODO Provide a builder API to check its validity.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct FlattenedDataFlowDescriptor {
    /// *(optional)* A unique identifier of an instance of this data flow.
    ///
    /// If provided, Zenoh-Flow will not generate one when instantiating the flow and keep this value instead.
    ///
    /// ⚠️ *Note that this will prevent having multiple instances of this data flow on the same Zenoh network*.
    pub id: Option<InstanceId>,
    /// A human-readable description of the data flow.
    pub name: Arc<str>,
    /// A non-empty list of Sources.
    pub sources: Vec<FlattenedSourceDescriptor>,
    /// A list of Operators.
    #[serde(default)]
    pub operators: Vec<FlattenedOperatorDescriptor>,
    /// A non-empty list of Sinks.
    pub sinks: Vec<FlattenedSinkDescriptor>,
    /// The complete list of links.
    pub links: Vec<LinkDescriptor>,
    /// *(optional)* A list specifying on which device the nodes run.
    ///
    /// Note that, if this field is omitted or only covers a part of the nodes, Zenoh-Flow will assign the nodes without
    /// a mapping to the Zenoh-Flow runtime that instantiates the data flow.
    #[serde(default)]
    pub mapping: HashMap<RuntimeId, HashSet<NodeId>>,
}

impl Display for FlattenedDataFlowDescriptor {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl FlattenedDataFlowDescriptor {
    /// Flatten the provided [DataFlowDescriptor], centralising all descriptors and checking its validity.
    ///
    /// The flattening process is recursive: to flatten the data flow, all nodes must first be flattened.
    ///
    /// For Sources and Sinks this flattening process is similar: retrieve the descriptor if it was not written directly
    /// inside the data flow and expose, for all variations, the same information.
    ///
    /// For Operators this process is different when it is Composite. In this particular case, its entry in the list of
    /// Operators (at the data flow level) must be replaced with the Operators it contains and the links that points to
    /// it must be modified to instead point to the actual Operator(s) that receive / send data.
    ///
    /// A `FlattenedDataFlowDescriptor` does not contain a [Configuration] as it has been propagated (possibly extended)
    /// to each node.
    ///
    /// A set of substitution [Vars] can be provided when flattening. This set allows overwriting the `vars` section of
    /// the provided [DataFlowDescriptor].
    ///
    /// # Validity
    ///
    /// A data flow in Zenoh-Flow is considered valid if:
    /// - it has at least one Source and one Sink,
    /// - all nodes, regardless of their type, have a different identifier,
    /// - no node has two inputs or two outputs with the same identifier,
    /// - all outputs are connected to at least one input,
    /// - all inputs are connected to at least one output.
    ///
    /// # Errors
    ///
    /// A flattening operation can fail for multiple reasons:
    /// - The flattening of an Operator failed.
    /// - The flattening of a Source failed.
    /// - The flattening of a Sink failed.
    /// - The flattened data flow is not valid.
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
            id: data_flow.id,
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

    /// Returns the unique identifier of the Zenoh-Flow runtime on which the node is configured to run.
    ///
    /// If there is no mapping entry for this specific node, `None` is returned.
    pub fn get_runtime(&self, node: &NodeId) -> Option<&RuntimeId> {
        for (runtime_id, nodes) in self.mapping.iter() {
            if nodes.contains(node) {
                return Some(runtime_id);
            }
        }

        None
    }
}

#[cfg(test)]
#[path = "./tests.rs"]
mod tests;

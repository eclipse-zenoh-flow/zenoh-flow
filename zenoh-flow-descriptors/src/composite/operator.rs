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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::composite::Substitutions;
use crate::flattened::{IFlattenableComposite, Patch};
use crate::{
    CompositeInputDescriptor, CompositeOutputDescriptor, FlattenedOperatorDescriptor,
    InputDescriptor, LinkDescriptor, NodeDescriptor, OperatorDescriptor, OutputDescriptor,
};

use serde::{Deserialize, Serialize};
use zenoh_flow_commons::{Configuration, IMergeOverwrite, NodeId, Result, Vars};

/// A `Composite Operator` Zenoh-Flow node.
///
/// A Composite Operator is a meta-operator: it groups together one or more Operators in a single descriptor. Its main
/// purpose is to simplify the creation of data flow graphs by allowing this form of grouping.
///
/// # `configuration` section caveats
///
/// The `configuration` section of a Composite Operator supersedes the same section in the Operator(s) it references.
/// See the documentation for more details regarding this behavior.
///
/// # Examples
///
/// ```
/// use zenoh_flow_descriptors::CompositeOperatorDescriptor;
///
/// let yaml = "
/// name: CompositeOperator
///
/// configuration:
///   name: foo
///
/// operators:
///   - id: InnerOperator1
///     descriptor: file:///home/zenoh-flow/nodes/operator1.so
///
///   - id: InnerOperator2
///     descriptor: file:///home/zenoh-flow/nodes/operator2.so
///
/// links:
///   - from:
///       node: InnerOperator1
///       output: out-2
///     to:
///       node: InnerOperator2
///       input: in-1
///
/// inputs:
///   - id: CompositeOperator-in
///     node: InnerOperator1
///     input: in-1
///
/// outputs:
///   - id: CompositeOperator-out
///     node: InnerOperator2
///     output: out-1
/// ";
///
/// let composite_operator_yaml = serde_yaml::from_str::<CompositeOperatorDescriptor>(&yaml).unwrap();
///
/// let json = "
/// {
///   \"name\": \"CompositeOperator\",
///
///   \"configuration\": {
///     \"name\": \"foo\"
///   },
///
///   \"operators\": [
///     {
///       \"id\": \"InnerOperator1\",
///       \"descriptor\": \"file:///home/zenoh-flow/nodes/operator1.so\"
///     },
///     {
///       \"id\": \"InnerOperator2\",
///       \"descriptor\": \"file:///home/zenoh-flow/nodes/operator2.so\"
///     }
///   ],
///
///   \"links\": [
///     {
///       \"from\": {
///         \"node\": \"InnerOperator1\",
///         \"output\": \"out-2\"
///       },
///       \"to\": {
///         \"node\": \"InnerOperator2\",
///         \"input\": \"in-1\"
///       }
///     }
///   ],
///
///   \"inputs\": [
///     {
///       \"id\": \"CompositeOperator-in\",
///       \"node\": \"InnerOperator1\",
///       \"input\": \"in-1\"
///     }
///   ],
///
///   \"outputs\": [
///     {
///       \"id\": \"CompositeOperator-out\",
///       \"node\": \"InnerOperator2\",
///       \"output\": \"out-1\"
///     }
///   ]
/// }";
///
/// let composite_operator_json = serde_json::from_str::<CompositeOperatorDescriptor>(&json).unwrap();
/// assert_eq!(composite_operator_yaml, composite_operator_json);
/// assert_eq!(composite_operator_yaml.configuration.get("name").unwrap().as_str(), Some("foo"));
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct CompositeOperatorDescriptor {
    pub name: NodeId,
    pub inputs: Vec<CompositeInputDescriptor>,
    pub outputs: Vec<CompositeOutputDescriptor>,
    pub operators: Vec<NodeDescriptor>,
    pub links: Vec<LinkDescriptor>,
    #[serde(default)]
    pub configuration: Configuration,
}

impl std::fmt::Display for CompositeOperatorDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Composite Operator: {}", self.name)
    }
}

impl IFlattenableComposite for CompositeOperatorDescriptor {
    type Flattened = FlattenedOperatorDescriptor;
    type Flattenable = OperatorDescriptor;

    fn flatten_composite(
        mut self,
        composite_id: NodeId,
        overwriting_configuration: Configuration,
        vars: Vars,
        ancestors: &mut HashSet<Arc<str>>,
    ) -> Result<(Vec<Self::Flattened>, Vec<LinkDescriptor>, Patch)> {
        let mut flattened_operators = Vec::with_capacity(self.operators.len());
        let composite_configuration = self.configuration;

        for operator_desc in self.operators.into_iter() {
            // The overwriting_configuration coming from upstream has the highest priority.
            // The current node's configuration has higher priority than the composite's configuration.
            let node_overwriting_configuration = overwriting_configuration.clone().merge_overwrite(
                operator_desc
                    .configuration
                    .clone()
                    .merge_overwrite(composite_configuration.clone()),
            );

            let (mut operators, mut links, patch) = operator_desc
                .flatten_maybe_composite::<CompositeOperatorDescriptor>(
                    node_overwriting_configuration,
                    vars.clone(),
                    // If we don't clone the ancestors between successive calls, consecutive composite operators
                    // referring to the same descriptor would be falsely flagged as "infinite recursions".
                    &mut ancestors.clone(),
                )?;

            flattened_operators.append(&mut operators);
            // We patch the links before appending the new ones to avoid some useless work.
            patch.apply(&mut self.links);
            self.links.append(&mut links);
        }

        // We have processed all operators. Time to patch.
        // 1. Prepend each operator id with the id of the composite.
        let subs_nodes: Substitutions<NodeId> = flattened_operators
            .iter_mut()
            .map(|flattened_operator| {
                (
                    flattened_operator.id.clone(),                  // previous id
                    flattened_operator.composite_id(&composite_id), // `composite_id` returns a clone of the updated id
                )
            })
            .collect::<HashMap<_, _>>()
            .into();

        // 2. Apply the `NodeId` substitutions on the links + the composite inputs/outputs.
        subs_nodes.apply(&mut self.links);
        subs_nodes.apply(&mut self.inputs);
        subs_nodes.apply(&mut self.outputs);

        // We need to tell upstream how to update the links that involve this Composite:
        let subs_inputs: Substitutions<InputDescriptor> = self
            .inputs
            .into_iter()
            .map(|input| {
                let old_input = InputDescriptor {
                    node: composite_id.clone(),
                    input: input.id.clone(),
                };
                (old_input, input.into())
            })
            .collect::<HashMap<_, _>>()
            .into();

        let subs_outputs: Substitutions<OutputDescriptor> = self
            .outputs
            .into_iter()
            .map(|output| {
                let old_output = OutputDescriptor {
                    node: composite_id.clone(),
                    output: output.id.clone(),
                };
                (old_output, output.into())
            })
            .collect::<HashMap<_, _>>()
            .into();

        Ok((
            flattened_operators,
            self.links,
            Patch::new(subs_inputs, subs_outputs),
        ))
    }
}

#[cfg(test)]
#[path = "./tests.rs"]
mod tests;

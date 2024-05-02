//
// Copyright (c) 2021 - 2024 ZettaScale Technology
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

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use url::Url;
use zenoh_flow_commons::{Configuration, IMergeOverwrite, NodeId, PortId, Result, Vars};

use crate::{
    flattened::{Patch, Substitutions},
    nodes::operator::{
        composite::CompositeOperatorDescriptor, CustomOperatorDescriptor, OperatorDescriptor,
        OperatorVariants,
    },
    uri, InputDescriptor, LinkDescriptor, OutputDescriptor,
};

/// A `FlattenedOperatorDescriptor` is a self-contained description of an Operator node.
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct FlattenedOperatorDescriptor {
    /// The unique (within a data flow) identifier of the Operator.
    pub id: NodeId,
    /// A human-readable description of the Operator.
    pub description: Option<Arc<str>>,
    /// The path to the implementation of the Operator.
    #[serde(alias = "Library")]
    pub library: Url,
    /// The identifiers of the inputs the Operator uses.
    pub inputs: Vec<PortId>,
    /// The identifiers of the outputs the Operator uses.
    pub outputs: Vec<PortId>,
    /// Pairs of `(key, value)` to change the behaviour of the Operator without altering its implementation.
    #[serde(default)]
    pub configuration: Configuration,
}

/// The Operator variant after it has been fetched (if it was remote) but before it has been flattened.
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(untagged)]
enum LocalOperatorVariants {
    Composite(CompositeOperatorDescriptor),
    Custom(CustomOperatorDescriptor),
}

impl Display for FlattenedOperatorDescriptor {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl FlattenedOperatorDescriptor {
    /// Attempts to flatten a [OperatorDescriptor] into a [FlattenedOperatorDescriptor].
    ///
    /// If the descriptor needs to be fetched this function will first fetch it, propagate and merge the overwriting
    /// [Vars], and expand them.
    ///
    /// It will then attempt to parse the descriptor into either a regular Operator or a Composite.
    ///
    /// If it is a Composite, things get more complicated. In short, a Composite need to be expanded into a set of
    /// regular Operators and their links, which we then need to add to the data flow.
    ///
    /// But that's not all, we also need to replace few elements in the data flow: the composite node should be dropped
    /// from the list of Operators & the links should be updated to point to the actual inputs / outputs of the expanded
    /// operators (instead of that of the Composite).
    /// To make that part easier we created dedicated structures [Patch] and [Substitutions].
    ///
    /// Finally, we need to merge the different configurations.
    ///
    /// # Errors
    ///
    /// The flattening process can fail if:
    /// - we cannot retrieve the remote descriptor,
    /// - we failed to parse the remote descriptor into either a regular Operator or a Composite,
    /// - we are expanding a Composite that we have already expanded before, effectively creating an infinite loop,
    /// - we failed to flatten an Operator within a Composite for any of the above reasons.
    pub(crate) fn try_flatten(
        operator_descriptor: OperatorDescriptor,
        mut outer_configuration: Configuration,
        mut overwritting_configuration: Configuration,
        overwritting_vars: Vars,
        ancestors: &mut HashSet<Url>,
    ) -> Result<(Vec<Self>, Vec<LinkDescriptor>, Patch)> {
        let descriptor = match operator_descriptor.variant {
            OperatorVariants::Remote(remote_desc) => {
                if !ancestors.insert(remote_desc.descriptor.clone()) {
                    bail!(
                        r#"
Possible infinite recursion detected, the following descriptor appears to include itself:
    {}
"#,
                        remote_desc.descriptor
                    );
                }

                // We only have access here to the inner configuration of a remote operator. As the configuration
                // declared here as higher priority than the outer_configuration, we merge here.
                outer_configuration = remote_desc
                    .configuration
                    .merge_overwrite(outer_configuration);

                let (mut descriptor, _) = uri::try_load_descriptor::<LocalOperatorVariants>(
                    &remote_desc.descriptor,
                    overwritting_vars.clone(),
                )
                .context(format!(
                    "Failed to load Operator from < {} >",
                    &remote_desc.descriptor
                ))?;

                if let LocalOperatorVariants::Custom(ref mut desc) = descriptor {
                    let description = desc.description.take();
                    desc.description = remote_desc.description.or(description);
                }

                descriptor
            }
            OperatorVariants::Custom(custom_desc) => LocalOperatorVariants::Custom(custom_desc),
        };

        match descriptor {
            LocalOperatorVariants::Custom(custom_desc) => Ok((
                vec![Self {
                    id: operator_descriptor.id,
                    description: custom_desc.description,
                    library: custom_desc.library,
                    inputs: custom_desc.inputs,
                    outputs: custom_desc.outputs,
                    // An inline operator's configuration has higher priority than the outer configuration. In turn, the
                    // overwriting configuration has the highest priority.
                    configuration: overwritting_configuration.merge_overwrite(
                        custom_desc
                            .configuration
                            .merge_overwrite(outer_configuration),
                    ),
                }],
                vec![],
                Patch::default(),
            )),
            LocalOperatorVariants::Composite(mut composite_desc) => {
                let mut flattened_operators = vec![];

                overwritting_configuration =
                    overwritting_configuration.merge_overwrite(outer_configuration);

                for operator_desc in composite_desc.operators {
                    let (mut flat_ops, mut links, patch) = Self::try_flatten(
                        operator_desc,
                        composite_desc.configuration.clone(),
                        overwritting_configuration.clone(),
                        overwritting_vars.clone(),
                        ancestors,
                    )?;

                    flattened_operators.append(&mut flat_ops);
                    patch.apply(&mut composite_desc.links);
                    composite_desc.links.append(&mut links);
                }

                // We have processed all operators. Time to patch.
                // 1. Prepend each operator id with the id of the composite.
                let subs_nodes: Substitutions<NodeId> = flattened_operators
                    .iter_mut()
                    .map(|flat_op| {
                        let old_id = flat_op.id.clone();
                        let composite_id: NodeId =
                            format!("{}>{}", &operator_descriptor.id, &old_id).into();
                        flat_op.id = composite_id.clone();
                        (old_id, composite_id)
                    })
                    .collect::<HashMap<_, _>>()
                    .into();

                // 2. Apply the `NodeId` substitutions on the links + the composite inputs/outputs.
                subs_nodes.apply(&mut composite_desc.links);
                subs_nodes.apply(&mut composite_desc.inputs);
                subs_nodes.apply(&mut composite_desc.outputs);

                // We need to tell upstream how to update the links that involve this Composite:
                let subs_inputs: Substitutions<InputDescriptor> = composite_desc
                    .inputs
                    .into_iter()
                    .map(|input| {
                        let old_input = InputDescriptor {
                            node: operator_descriptor.id.clone(),
                            input: input.id.clone(),
                        };
                        (old_input, input.into())
                    })
                    .collect::<HashMap<_, _>>()
                    .into();

                let subs_outputs: Substitutions<OutputDescriptor> = composite_desc
                    .outputs
                    .into_iter()
                    .map(|output| {
                        let old_output = OutputDescriptor {
                            node: operator_descriptor.id.clone(),
                            output: output.id.clone(),
                        };
                        (old_output, output.into())
                    })
                    .collect::<HashMap<_, _>>()
                    .into();

                Ok((
                    flattened_operators,
                    composite_desc.links,
                    Patch::new(subs_inputs, subs_outputs),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_no_configuration() {
        let yaml_str = r#"
id: operator-2
description: operator-2
library: file:///home/zenoh-flow/nodes/liboperator_0.so
inputs:
  - in-0
outputs:
  - out-0
"#;

        let flat_operator: FlattenedOperatorDescriptor =
            serde_yaml::from_str(yaml_str).expect("Failed to deserialise");
        assert!(serde_yaml::to_string(&flat_operator).is_ok());
    }

    #[test]
    fn test_serialize_full() {
        let yaml_str = r#"
id: "operator-0"
description: "operator-0"
library: file:///home/zenoh-flow/nodes/liboperator_0.so
inputs:
  - in-0
outputs:
  - out-0
configuration:
  answer: 42
"#;

        let flat_operator: FlattenedOperatorDescriptor =
            serde_yaml::from_str(yaml_str).expect("Failed to deserialise");
        assert!(serde_yaml::to_string(&flat_operator).is_ok());
    }
}

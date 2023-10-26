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

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::Arc,
};
use zenoh_flow_commons::{Configuration, IMergeOverwrite, NodeId, PortId, Result, Vars};

use crate::{
    flattened::{uri, Patch, Substitutions},
    nodes::operator::{
        composite::CompositeOperatorDescriptor, CustomOperatorDescriptor, OperatorVariants,
    },
    InputDescriptor, LinkDescriptor, OperatorDescriptor, OutputDescriptor,
};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct FlattenedOperatorDescriptor {
    pub id: NodeId,
    pub description: Arc<str>,
    pub library: Arc<str>,
    pub inputs: Vec<PortId>,
    pub outputs: Vec<PortId>,
    #[serde(default)]
    pub configuration: Configuration,
}

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
    pub fn try_flatten(
        operator_descriptor: OperatorDescriptor,
        mut overwritting_configuration: Configuration,
        overwritting_vars: Vars,
        ancestors: &mut HashSet<Arc<str>>,
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

                let (descriptor, _) = uri::try_load_descriptor::<LocalOperatorVariants>(
                    &remote_desc.descriptor,
                    overwritting_vars.clone(),
                )
                .context(format!(
                    "Failed to load Operator from < {} >",
                    &remote_desc.descriptor
                ))?;

                // CAVEAT: the inner configuration of the outmost operator has the highest priority. We know we are in
                // that situation if the operator has no ancestor.
                if ancestors.is_empty() {
                    overwritting_configuration = remote_desc
                        .configuration
                        .merge_overwrite(overwritting_configuration);
                }

                descriptor
            }
            OperatorVariants::Custom(custom_desc) => {
                // CAVEAT: the inner configuration of the outmost operator has the highest priority. We know we are in
                // that situation if the operator has no ancestor.
                if ancestors.is_empty() {
                    overwritting_configuration = custom_desc
                        .configuration
                        .clone()
                        .merge_overwrite(overwritting_configuration);
                }

                LocalOperatorVariants::Custom(custom_desc)
            }
        };

        match descriptor {
            LocalOperatorVariants::Custom(custom_desc) => Ok((
                vec![Self {
                    id: operator_descriptor.id,
                    description: custom_desc.description,
                    library: custom_desc.library,
                    inputs: custom_desc.inputs,
                    outputs: custom_desc.outputs,
                    configuration: overwritting_configuration
                        .merge_overwrite(custom_desc.configuration),
                }],
                vec![],
                Patch::default(),
            )),
            LocalOperatorVariants::Composite(mut composite_desc) => {
                let mut flattened_operators = vec![];

                overwritting_configuration = overwritting_configuration
                    .merge_overwrite(composite_desc.configuration.clone());

                for operator_desc in composite_desc.operators {
                    let (mut flat_ops, mut links, patch) = Self::try_flatten(
                        operator_desc,
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
                            format!("{}/{}", &operator_descriptor.id, &old_id).into();
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

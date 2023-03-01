//
// Copyright (c) 2022 ZettaScale Technology
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

use crate::model::descriptor::link::{CompositeInputDescriptor, CompositeOutputDescriptor};
use crate::model::descriptor::node::NodeDescriptor;
use crate::model::descriptor::{LinkDescriptor, LoadedNode, PortDescriptor};
use crate::types::configuration::Merge;
use crate::types::{Configuration, NodeId};
use crate::zfresult::{ErrorKind, ZFResult as Result};
use crate::{bail, zferror};
use async_recursion::async_recursion;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Describes a simple operator.
///
/// Example:
///
///
/// ```yaml
/// id : PrintSink
/// uri: file://./target/release/libmy_op.so
/// configuration:
///   by: 10
/// inputs:
///     - id: Number
///       type: usize
/// outputs:
///   - id: Multiplied
///     type: usize
/// ```
///
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct OperatorDescriptor {
    pub id: NodeId,
    pub inputs: Vec<PortDescriptor>,
    pub outputs: Vec<PortDescriptor>,
    pub uri: Option<String>,
    pub configuration: Option<Configuration>,
}

impl std::fmt::Display for OperatorDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Operator (Simple)", self.id)
    }
}

impl LoadedNode for OperatorDescriptor {
    fn from_parameters(
        id: NodeId,
        configuration: Option<Configuration>,
        uri: Option<String>,
        inputs: Option<Vec<PortDescriptor>>,
        outputs: Option<Vec<PortDescriptor>>,
        _operators: Option<Vec<NodeDescriptor>>,
        _links: Option<Vec<LinkDescriptor>>,
        _composite_inputs: Option<Vec<CompositeInputDescriptor>>,
        _compisite_outpus: Option<Vec<CompositeOutputDescriptor>>,
    ) -> Result<Self> {
        match (inputs, outputs) {
            (Some(inputs),Some(outputs)) =>{
                Ok(Self{
                    id,
                    configuration,
                    uri,
                    inputs,
                    outputs,
                })
            },
            _ => bail!(ErrorKind::InvalidData, "Creating a OperatorDescriptor requires: id, configuration, uri, inputs and outputs. Maybe some parameters are set as None?")
        }
    }

    fn get_id(&self) -> &NodeId {
        &self.id
    }

    fn set_id(&mut self, id: NodeId) {
        self.id = id
    }

    fn get_configuration(&self) -> &Option<Configuration> {
        &self.configuration
    }

    fn set_configuration(&mut self, configuration: Option<Configuration>) {
        self.configuration = configuration
    }

    fn from_yaml(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_yaml::from_str::<OperatorDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    fn from_json(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_json::from_str::<OperatorDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }
    fn to_json(&self) -> Result<String> {
        serde_json::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }

    /// A variant error is returned if serialization fails.
    fn to_yaml(&self) -> Result<String> {
        serde_yaml::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }
}

/// Describes a composite operator.
///
/// Example:
///
///
/// ```yaml
/// id: AMagicAIBasedOperator
/// configuration:
///   parameter1: value1
///   parameter2: value2
/// operators: # Operators composing the flow
///   - id: ComposedAIDownsampling
///     descritptor: [file://|...]/some/path/to/its/descritptor/file.yaml
///     configuration:
///       paramter3: value3
///   - id: MagicAI
///         descriptor: [file://|...]/some/path/to/its/descritptor/file.yaml
///   - id: MagicPostProcessing
///         descriptor: [file://|...]/some/path/to/its/descritptor/file.yaml
/// links:
/// - from:
///     node : ComposedAIDownsampling
///     output : Data
///   to:
///     node : MagicAI
///     input : In
/// - from:
///     node : MagicAI
///     output : Out
///   to:
///     node : MagicPostProcessing
///     input : Data
///
/// inputs:
///     - node: ComposedAIDownsampling
///       input: Data
/// outputs:
///   - node: MagicPostProcessing
///     output: Data
/// ```
///
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CompositeOperatorDescriptor {
    pub id: NodeId,
    pub inputs: Vec<CompositeInputDescriptor>,
    pub outputs: Vec<CompositeOutputDescriptor>,
    pub operators: Vec<NodeDescriptor>,
    pub links: Vec<LinkDescriptor>,
    pub configuration: Option<Configuration>,
}

impl std::fmt::Display for CompositeOperatorDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Operator (Composite)", self.id)
    }
}

impl CompositeOperatorDescriptor {
    /// Flattens the `CompositeOperatorDescriptor` by loading all the composite operators
    ///
    ///  # Errors
    /// A variant error is returned if loading operators fails. Or if the node does not contains an
    /// operator.
    #[async_recursion]
    pub(crate) async fn flatten(
        mut self,
        composite_id: NodeId,
        links: &mut Vec<LinkDescriptor>,
        global_configuration: Option<Configuration>,
        ancestors: &mut Vec<String>,
    ) -> Result<Vec<OperatorDescriptor>> {
        let mut simple_operators = vec![];
        self.configuration = global_configuration.merge_overwrite(self.configuration);

        for o in self.operators {
            let NodeDescriptor {
                id: operator_id,
                descriptor,
                configuration,
            } = o.clone();

            let configuration = self.configuration.clone().merge_overwrite(configuration);

            let res_simple = o.try_load_self::<OperatorDescriptor>().await; //OperatorDescriptor::from_yaml(&description);
            if let Ok(mut simple_operator) = res_simple {
                let new_id: NodeId = format!("{composite_id}/{operator_id}").into();

                let output_ids: HashMap<_, _> = self
                    .outputs
                    .iter()
                    .filter(|&output| output.node == operator_id)
                    .map(|output| (&output.id, &output.output))
                    .collect();

                let input_ids: HashMap<_, _> = self
                    .inputs
                    .iter()
                    .filter(|&input| input.node == operator_id)
                    .map(|input| (&input.id, &input.input))
                    .collect();

                // Updating all the links with the old id to the new ID
                for l in &mut self.links {
                    if l.from.node == operator_id {
                        log::trace!("Updating {} to {}", l.from.node, new_id);
                        l.from.node = new_id.clone();
                    }
                    if l.to.node == operator_id {
                        log::trace!("Updating {} to {}", l.to.node, new_id);
                        l.to.node = new_id.clone();
                    }
                }

                links
                    .iter_mut()
                    .filter(|link| {
                        link.from.node == composite_id
                            && output_ids.keys().contains(&&link.from.output)
                    })
                    .for_each(|link| {
                        link.from.node = new_id.clone();
                        link.from.output = (*output_ids.get(&&link.from.output).unwrap()).clone();
                    });

                links
                    .iter_mut()
                    .filter(|link| {
                        link.to.node == composite_id && input_ids.keys().contains(&&link.to.input)
                    })
                    .for_each(|link| {
                        link.to.node = new_id.clone();
                        link.to.input = (*input_ids.get(&&link.to.input).unwrap()).clone();
                    });

                // Updating the new id
                simple_operator.id = new_id;

                simple_operator.configuration = configuration
                    .clone()
                    .merge_overwrite(simple_operator.configuration);

                // Adding in the list of operators
                simple_operators.push(simple_operator);
                continue;
            }

            let res_composite = o.try_load_self::<CompositeOperatorDescriptor>().await; //CompositeOperatorDescriptor::from_yaml(&description);
            if let Ok(composite_operator) = res_composite {
                if let Ok(index) = ancestors.binary_search(&descriptor) {
                    bail!(
                        ErrorKind::GenericError, // FIXME Dedicated error?
                        "Possible recursion detected, < {} > would be included again after: {:?}",
                        descriptor,
                        &ancestors[index..]
                    );
                }
                ancestors.push(descriptor.clone());

                let mut operators = composite_operator
                    .flatten(operator_id, &mut self.links, configuration, ancestors)
                    .await?;

                for operator in operators.iter_mut() {
                    let new_id: NodeId = format!("{}/{}", composite_id, operator.id).into();
                    self.links
                        .iter_mut()
                        .filter(|link| link.from.node == operator.id || link.to.node == operator.id)
                        .for_each(|link| {
                            if link.from.node == operator.id {
                                link.from.node = new_id.clone();
                            }
                            if link.to.node == operator.id {
                                link.to.node = new_id.clone();
                            }
                        });

                    operator.id = new_id;
                }

                simple_operators.append(&mut operators);

                ancestors.pop();
                continue;
            }

            // If we arrive at that code it means that both attempts to parse the descriptor failed.
            log::error!(
                "Could not parse < {} > as either a Simple or a Composite Operator:",
                operator_id
            );
            log::error!("Simple: {:?}", res_simple.err().unwrap());
            log::error!("Composite: {:?}", res_composite.err().unwrap());

            bail!(
                ErrorKind::ParsingError,
                "Could not parse < {} >",
                operator_id
            );
        }

        links.append(&mut self.links);

        Ok(simple_operators)
    }
}

impl LoadedNode for CompositeOperatorDescriptor {
    fn from_parameters(
        id: NodeId,
        configuration: Option<Configuration>,
        _uri: Option<String>,
        _inputs: Option<Vec<PortDescriptor>>,
        _outputs: Option<Vec<PortDescriptor>>,
        operators: Option<Vec<NodeDescriptor>>,
        links: Option<Vec<LinkDescriptor>>,
        composite_inputs: Option<Vec<CompositeInputDescriptor>>,
        compisite_outpus: Option<Vec<CompositeOutputDescriptor>>,
    ) -> Result<Self> {
        match (operators, links, composite_inputs, compisite_outpus) {
            (Some(operators),Some(links), Some(composite_inputs), Some(compisite_outpus)) =>{
                Ok(Self{
                    id,
                    configuration,
                    operators,
                    links,
                    inputs: composite_inputs,
                    outputs: compisite_outpus,
                })
            },
            _ => bail!(ErrorKind::InvalidData, "Creating a CompositeOperatorDescriptor requires: id, configuration, uri, operators, links, composite_inputs and composite_inputs. Maybe some parameters are set as None?")
        }
    }

    fn get_id(&self) -> &NodeId {
        &self.id
    }

    fn set_id(&mut self, id: NodeId) {
        self.id = id
    }

    fn get_configuration(&self) -> &Option<Configuration> {
        &self.configuration
    }

    fn set_configuration(&mut self, configuration: Option<Configuration>) {
        self.configuration = configuration
    }

    fn from_yaml(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_yaml::from_str::<CompositeOperatorDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    fn from_json(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_json::from_str::<CompositeOperatorDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    fn to_json(&self) -> Result<String> {
        serde_json::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }

    fn to_yaml(&self) -> Result<String> {
        serde_yaml::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }
}

#[cfg(test)]
#[path = "../tests/flatten-composite.rs"]
mod tests;

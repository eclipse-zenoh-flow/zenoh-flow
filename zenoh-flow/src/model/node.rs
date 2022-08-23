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

use crate::model::link::{LinkDescriptor, PortRecord};
use crate::model::PortDescriptor;
use crate::types::{merge_configurations, PortType};
use crate::types::{Configuration, NodeId, RuntimeId};
use crate::zfresult::ErrorKind;
use crate::Result;
use crate::{bail, zferror};
use async_recursion::async_recursion;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{CompositeInputDescriptor, CompositeOutputDescriptor};

/// Describes a sink.
///
/// Example:
///
///
/// ```yaml
/// id : PrintSink
/// uri: file://./target/release/libgeneric_sink.so
/// configuration:
///   file: /tmp/generic-sink.txt
/// input:
///   id: Data
///   type: usize
/// tags: [linux]
/// ```
///
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SinkDescriptor {
    pub id: NodeId,
    pub inputs: Vec<PortDescriptor>,
    pub uri: Option<String>,
    pub configuration: Option<Configuration>,
    pub flags: Option<Vec<String>>,
    pub tags: Vec<String>,
}

impl std::fmt::Display for SinkDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Sink", self.id)
    }
}

impl SinkDescriptor {
    /// Creates a new `SinkDescriptor` from its YAML representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_yaml(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_yaml::from_str::<SinkDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    /// Creates a new `SinkDescriptor` from its JSON representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_json(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_json::from_str::<SinkDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    /// Returns the JSON representation of the `SinkDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }

    /// Returns the YAML representation of the `SinkDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_yaml(&self) -> Result<String> {
        serde_yaml::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }
}

/// Describes a source.
///
/// Example:
///
///
/// ```yaml
/// id : PrintSink
/// uri: file://./target/release/libcounter_source.so
/// configuration:
///   start: 10
/// output:
///   id: Counter
///   type: usize
/// tags: []
/// ```
///
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SourceDescriptor {
    pub id: NodeId,
    pub outputs: Vec<PortDescriptor>,
    pub uri: Option<String>,
    pub configuration: Option<Configuration>,
    pub flags: Option<Vec<String>>,
    pub tags: Vec<String>,
}

impl std::fmt::Display for SourceDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Source", self.id)
    }
}

impl SourceDescriptor {
    /// Creates a new `SourceDescriptor` from its YAML representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_yaml(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_yaml::from_str::<SourceDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    /// Creates a new `SourceDescriptor` from its JSON representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_json(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_json::from_str::<SourceDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    /// Returns the JSON representation of the `SourceDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }

    /// Returns the YAML representation of the `SourceDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_yaml(&self) -> Result<String> {
        serde_yaml::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }
}

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
/// tags: [something]
/// ```
///
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SimpleOperatorDescriptor {
    pub id: NodeId,
    pub inputs: Vec<PortDescriptor>,
    pub outputs: Vec<PortDescriptor>,
    pub uri: Option<String>,
    pub configuration: Option<Configuration>,
    pub flags: Option<Vec<String>>,
    pub tags: Vec<String>,
}

impl std::fmt::Display for SimpleOperatorDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Operator (Simple)", self.id)
    }
}

impl SimpleOperatorDescriptor {
    /// Creates a new `SimpleOperatorDescriptor` from its YAML representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_yaml(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_yaml::from_str::<SimpleOperatorDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    /// Creates a new `SimpleOperatorDescriptor` from its JSON representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_json(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_json::from_str::<SimpleOperatorDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    /// Returns the JSON representation of the `SimpleOperatorDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }

    /// Returns the YAML representation of the `SimpleOperatorDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_yaml(&self) -> Result<String> {
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
    /// Creates a new `CompositeOperatorDescriptor` from its YAML representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_yaml(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_yaml::from_str::<CompositeOperatorDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    /// Creates a new `CompositeOperatorDescriptor` from its JSON representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_json(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_json::from_str::<CompositeOperatorDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    /// Returns the JSON representation of the `CompositeOperatorDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }

    /// Returns the YAML representation of the `CompositeOperatorDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_yaml(&self) -> Result<String> {
        serde_yaml::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }

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
    ) -> Result<Vec<SimpleOperatorDescriptor>> {
        let mut simple_operators = vec![];

        for o in self.operators {
            let description = async_std::fs::read_to_string(&o.descriptor).await?;

            let res_simple = SimpleOperatorDescriptor::from_yaml(&description);
            if let Ok(mut simple_operator) = res_simple {
                let new_id: NodeId = format!("{}/{}", composite_id, o.id).into();

                let output_ids: HashMap<_, _> = self
                    .outputs
                    .iter()
                    .filter(|&output| output.node == o.id)
                    .map(|output| (&output.id, &output.output))
                    .collect();

                let input_ids: HashMap<_, _> = self
                    .inputs
                    .iter()
                    .filter(|&input| input.node == o.id)
                    .map(|input| (&input.id, &input.input))
                    .collect();

                // Updating all the links with the old id to the new ID
                for l in &mut self.links {
                    if l.from.node == o.id {
                        log::trace!("Updating {} to {}", l.from.node, new_id);
                        l.from.node = new_id.clone();
                    }
                    if l.to.node == o.id {
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

                simple_operator.configuration = merge_configurations(
                    global_configuration.clone(),
                    merge_configurations(simple_operator.configuration, self.configuration.clone()),
                );

                // Adding in the list of operators
                simple_operators.push(simple_operator);
                continue;
            }

            let res_composite = CompositeOperatorDescriptor::from_yaml(&description);
            if let Ok(composite_operator) = res_composite {
                if let Ok(index) = ancestors.binary_search(&o.descriptor) {
                    bail!(
                        ErrorKind::GenericError, // FIXME Dedicated error?
                        "Possible recursion detected, < {} > would be included again after: {:?}",
                        o.descriptor,
                        &ancestors[index..]
                    );
                }
                ancestors.push(o.descriptor.clone());

                let mut operators = composite_operator
                    .flatten(
                        o.id,
                        &mut self.links,
                        global_configuration.clone(),
                        ancestors,
                    )
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
                o.descriptor
            );
            log::error!("Simple: {:?}", res_simple.err().unwrap());
            log::error!("Composite: {:?}", res_composite.err().unwrap());

            bail!(
                ErrorKind::ParsingError,
                "Could not parse < {} >",
                o.descriptor
            );
        }

        links.append(&mut self.links);

        Ok(simple_operators)
    }
}

/// Describes an node of the graph
///
/// ```yaml
/// id : PrintSink
/// descriptor: file://./target/release/counter_source.yaml
/// configuration:
///   start: 10
///
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeDescriptor {
    pub id: NodeId,
    pub descriptor: String,
    pub flags: Option<Vec<String>>,
    pub configuration: Option<Configuration>,
}

impl std::fmt::Display for NodeDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "ID: {} Descriptor: {} Configuration: {:?}",
            self.id, self.descriptor, self.configuration
        )
    }
}

impl NodeDescriptor {
    /// Creates a new `NodeDescriptor` from its YAML representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_yaml(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_yaml::from_str::<NodeDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    /// Creates a new `NodeDescriptor` from its JSON representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_json(data: &str) -> Result<Self> {
        let dataflow_descriptor = serde_json::from_str::<NodeDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    /// Returns the JSON representation of the `NodeDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }

    /// Returns the YAML representation of the `NodeDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_yaml(&self) -> Result<String> {
        serde_yaml::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }

    /// Flattens the `NodeDescriptor` by loading all the composite operators
    ///
    ///  # Errors
    /// A variant error is returned if loading operators fails. Or if the
    /// node does not contains an operator
    pub async fn flatten(
        self,
        id: NodeId,
        links: &mut Vec<LinkDescriptor>,
        global_configuration: Option<Configuration>,
        ancestors: &mut Vec<String>,
    ) -> Result<Vec<SimpleOperatorDescriptor>> {
        let description = async_std::fs::read_to_string(&self.descriptor).await?;

        // We try to load the descriptor, first we try as simple one, if it fails we try as a
        // composite one, if that also fails it is malformed.
        let res_simple = SimpleOperatorDescriptor::from_yaml(&description);
        if let Ok(simple_operator) = res_simple {
            // TODO Handle configuration
            // TODO Handle flags
            return Ok(vec![simple_operator]);
        }

        let res_composite = CompositeOperatorDescriptor::from_yaml(&description);
        if let Ok(composite_operator) = res_composite {
            if let Ok(index) = ancestors.binary_search(&self.descriptor) {
                bail!(
                    ErrorKind::GenericError, // FIXME Dedicated error?
                    "Possible recursion detected, < {} > would be included again after: {:?}",
                    self.descriptor,
                    &ancestors[index..]
                );
            }

            ancestors.push(self.descriptor.clone());
            let res = composite_operator
                .flatten(id, links, global_configuration, ancestors)
                .await;
            ancestors.pop();

            return res;
        }

        log::error!("Could not parse operator < {} >", self.descriptor);
        log::error!("(Operator) {:?}", res_simple.err().unwrap());
        log::error!("(Composite) {:?}", res_composite.err().unwrap());

        bail!(
            ErrorKind::ParsingError,
            "Could not parse operator < {} >",
            self.descriptor
        )
    }

    /// Loads the source from the `NodeDescriptor`
    ///
    ///  # Errors
    /// A variant error is returned if loading source fails. Or if the
    ///  node does not contains an source
    pub async fn load_source(
        self,
        global_configuration: Option<Configuration>,
    ) -> Result<SourceDescriptor> {
        let descriptor_path = async_std::fs::read_to_string(&self.descriptor).await?;
        log::trace!("Loading source {}", self.descriptor);
        // We try to load the descriptor, first we try as simple one, if it fails
        // we try as a composite one, if that also fails it is malformed.
        match SourceDescriptor::from_yaml(&descriptor_path) {
            Ok(mut desc) => {
                desc.id = self.id;
                desc.configuration = merge_configurations(global_configuration, desc.configuration);
                desc.flags = self.flags;
                Ok(desc)
            }
            Err(e) => {
                log::warn!("Unable to flatten {}, error {}", self.id, e);
                Err(e)
            }
        }
    }

    /// Loads the sink from the `NodeDescriptor`
    ///
    ///  # Errors
    /// A variant error is returned if loading sink fails. Or if the
    ///  node does not contains an sink
    pub async fn load_sink(
        self,
        global_configuration: Option<Configuration>,
    ) -> Result<SinkDescriptor> {
        let descriptor_path = async_std::fs::read_to_string(&self.descriptor).await?;
        log::trace!("Loading sink {}", self.descriptor);

        // We try to load the descriptor, first we try as simple one, if it fails
        // we try as a composite one, if that also fails it is malformed.
        match SinkDescriptor::from_yaml(&descriptor_path) {
            Ok(mut desc) => {
                desc.id = self.id;
                desc.configuration = merge_configurations(global_configuration, desc.configuration);
                desc.flags = self.flags;
                Ok(desc)
            }
            Err(e) => {
                log::warn!("Unable to flatten {}, error {}", self.id, e);
                Err(e)
            }
        }
    }
}

// Records

/// A `SinkRecord` is an instance of a [`SinkDescriptor`](`SinkDescriptor`)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SinkRecord {
    pub id: NodeId,
    pub uid: u32,
    pub inputs: Vec<PortRecord>,
    pub uri: Option<String>,
    pub configuration: Option<Configuration>,
    pub runtime: RuntimeId,
}

impl std::fmt::Display for SinkRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Sink", self.id)
    }
}

impl SinkRecord {
    /// Returns the `PortType` for the input.
    pub fn get_input_type(&self, id: impl AsRef<str>) -> Option<&PortType> {
        self.inputs
            .iter()
            .find(|&lid| lid.port_id.as_ref() == id.as_ref())
            .map(|lid| &lid.port_type)
    }
}

/// A `SourceRecord` is an instance of a [`SourceDescriptor`](`SourceDescriptor`)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SourceRecord {
    pub id: NodeId,
    pub uid: u32,
    pub outputs: Vec<PortRecord>,
    pub uri: Option<String>,
    pub configuration: Option<Configuration>,
    pub runtime: RuntimeId,
}

impl std::fmt::Display for SourceRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Source", self.id)
    }
}

impl SourceRecord {
    /// Returns the `PortType` for the output.
    pub fn get_output_type(&self, id: impl AsRef<str>) -> Option<&PortType> {
        self.outputs
            .iter()
            .find(|&lid| lid.port_id.as_ref() == id.as_ref())
            .map(|lid| &lid.port_type)
    }
}

/// An `OperatorRecord` is an instance of an [`OperatorDescriptor`](`OperatorDescriptor`)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OperatorRecord {
    pub(crate) id: NodeId,
    pub uid: u32,
    pub(crate) inputs: Vec<PortRecord>,
    pub(crate) outputs: Vec<PortRecord>,
    pub(crate) uri: Option<String>,
    pub(crate) configuration: Option<Configuration>,
    pub(crate) runtime: RuntimeId,
}

impl std::fmt::Display for OperatorRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Operator", self.id)
    }
}

impl OperatorRecord {
    /// Returns the `PortType` for the given output.
    pub fn get_output_type(&self, id: impl AsRef<str>) -> Option<&PortType> {
        self.outputs
            .iter()
            .find(|&lid| lid.port_id.as_ref() == id.as_ref())
            .map(|lid| &lid.port_type)
    }

    /// Return the `PortType` for the given input.
    pub fn get_input_type(&self, id: impl AsRef<str>) -> Option<&PortType> {
        self.inputs
            .iter()
            .find(|&lid| lid.port_id.as_ref() == id.as_ref())
            .map(|lid| &lid.port_type)
    }
}

#[cfg(test)]
#[path = "./tests/flatten-composite.rs"]
mod tests;

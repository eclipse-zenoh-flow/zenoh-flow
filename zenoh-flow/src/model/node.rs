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

use crate::error::ZFError;
use crate::model::link::{LinkDescriptor, PortRecord};
use crate::model::{InputDescriptor, OutputDescriptor, PortDescriptor};
use crate::types::{merge_configurations, PortType, ZFResult};
use crate::types::{Configuration, NodeId, RuntimeId};
use async_recursion::async_recursion;
use serde::{Deserialize, Serialize};

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
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SinkDescriptor {
    pub id: NodeId,
    pub inputs: Vec<PortDescriptor>,
    pub uri: Option<String>,
    pub configuration: Option<Configuration>,
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
    pub fn from_yaml(data: &str) -> ZFResult<Self> {
        let dataflow_descriptor = serde_yaml::from_str::<SinkDescriptor>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
        Ok(dataflow_descriptor)
    }

    /// Creates a new `SinkDescriptor` from its JSON representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_json(data: &str) -> ZFResult<Self> {
        let dataflow_descriptor = serde_json::from_str::<SinkDescriptor>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
        Ok(dataflow_descriptor)
    }

    /// Returns the JSON representation of the `SinkDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_json(&self) -> ZFResult<String> {
        serde_json::to_string(&self).map_err(|_| ZFError::SerializationError)
    }

    /// Returns the YAML representation of the `SinkDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_yaml(&self) -> ZFResult<String> {
        serde_yaml::to_string(&self).map_err(|_| ZFError::SerializationError)
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
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SourceDescriptor {
    pub id: NodeId,
    pub outputs: Vec<PortDescriptor>,
    pub uri: Option<String>,
    pub configuration: Option<Configuration>,
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
    pub fn from_yaml(data: &str) -> ZFResult<Self> {
        let dataflow_descriptor = serde_yaml::from_str::<SourceDescriptor>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
        Ok(dataflow_descriptor)
    }

    /// Creates a new `SourceDescriptor` from its JSON representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_json(data: &str) -> ZFResult<Self> {
        let dataflow_descriptor = serde_json::from_str::<SourceDescriptor>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
        Ok(dataflow_descriptor)
    }

    /// Returns the JSON representation of the `SourceDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_json(&self) -> ZFResult<String> {
        serde_json::to_string(&self).map_err(|_| ZFError::SerializationError)
    }

    /// Returns the YAML representation of the `SourceDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_yaml(&self) -> ZFResult<String> {
        serde_yaml::to_string(&self).map_err(|_| ZFError::SerializationError)
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
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SimpleOperatorDescriptor {
    pub id: NodeId,
    pub inputs: Vec<PortDescriptor>,
    pub outputs: Vec<PortDescriptor>,
    pub uri: Option<String>,
    pub configuration: Option<Configuration>,
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
    pub fn from_yaml(data: &str) -> ZFResult<Self> {
        let dataflow_descriptor = serde_yaml::from_str::<SimpleOperatorDescriptor>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
        Ok(dataflow_descriptor)
    }

    /// Creates a new `SimpleOperatorDescriptor` from its JSON representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_json(data: &str) -> ZFResult<Self> {
        let dataflow_descriptor = serde_json::from_str::<SimpleOperatorDescriptor>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
        Ok(dataflow_descriptor)
    }

    /// Returns the JSON representation of the `SimpleOperatorDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_json(&self) -> ZFResult<String> {
        serde_json::to_string(&self).map_err(|_| ZFError::SerializationError)
    }

    /// Returns the YAML representation of the `SimpleOperatorDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_yaml(&self) -> ZFResult<String> {
        serde_yaml::to_string(&self).map_err(|_| ZFError::SerializationError)
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
    pub inputs: Vec<InputDescriptor>,
    pub outputs: Vec<OutputDescriptor>,
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
    pub fn from_yaml(data: &str) -> ZFResult<Self> {
        let dataflow_descriptor = serde_yaml::from_str::<CompositeOperatorDescriptor>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
        Ok(dataflow_descriptor)
    }

    /// Creates a new `CompositeOperatorDescriptor` from its JSON representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_json(data: &str) -> ZFResult<Self> {
        let dataflow_descriptor = serde_json::from_str::<CompositeOperatorDescriptor>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
        Ok(dataflow_descriptor)
    }

    /// Returns the JSON representation of the `CompositeOperatorDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_json(&self) -> ZFResult<String> {
        serde_json::to_string(&self).map_err(|_| ZFError::SerializationError)
    }

    /// Returns the YAML representation of the `CompositeOperatorDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_yaml(&self) -> ZFResult<String> {
        serde_yaml::to_string(&self).map_err(|_| ZFError::SerializationError)
    }

    /// Flattens the `CompositeOperatorDescriptor` by loading all the composite operators
    ///
    ///  # Errors
    /// A variant error is returned if loading operators fails. Or if the
    ///  node does not contains an operator
    #[async_recursion]
    pub async fn flatten(
        mut self,
        id: NodeId,
        global_configuration: Option<Configuration>,
    ) -> ZFResult<(
        Vec<SimpleOperatorDescriptor>,
        Vec<LinkDescriptor>,
        Vec<InputDescriptor>,
        Vec<OutputDescriptor>,
    )> {
        let mut simple_operators = vec![];
        // let mut links = vec![];
        for o in self.operators {
            let descriptor_path = async_std::fs::read_to_string(&o.descriptor).await?;
            log::trace!("Loading operator {}", o.descriptor);
            // We try to load the descriptor, first we try as simple one, if it fails
            // we try as a composite one, if that also fails it is malformed.
            match SimpleOperatorDescriptor::from_yaml(&descriptor_path) {
                Ok(mut desc) => {
                    log::trace!("This is a simple operator");
                    // Creating a new ID
                    let new_id: NodeId = format!("{}-{}", id, o.id).into();

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

                        // Adding in the list of links
                        // links.push(l.clone());
                    }

                    for i in &mut self.inputs {
                        if i.node == o.id {
                            i.node = new_id.clone();
                        }
                    }

                    for out in &mut self.outputs {
                        if out.node == o.id {
                            out.node = new_id.clone();
                        }
                    }

                    // Updating the new id
                    desc.id = new_id;

                    desc.configuration = merge_configurations(
                        global_configuration.clone(),
                        merge_configurations(desc.configuration, self.configuration.clone()),
                    );

                    // Adding in the list of operators
                    simple_operators.push(desc);
                }
                Err(_) => {
                    log::trace!("Try as composite one {}", o.descriptor);
                    match CompositeOperatorDescriptor::from_yaml(&descriptor_path) {
                        Ok(desc) => {
                            log::trace!("This is a composite operator");
                            let new_id: NodeId = format!("{}-{}", id, o.id).into();
                            match desc
                                .flatten(new_id.clone(), global_configuration.clone())
                                .await
                            {
                                Ok((ds, ls, ins, outs)) => {
                                    // Adding the result in the list of operators
                                    // and links
                                    simple_operators.extend(ds);
                                    self.links.extend(ls);

                                    // Updating the links
                                    for l in &mut self.links {
                                        if l.to.node == o.id {
                                            let matching_input = ins
                                                .iter()
                                                .find(|x| x.node.starts_with(&*new_id))
                                                .ok_or_else(|| {
                                                    ZFError::NodeNotFound(new_id.clone())
                                                })?;
                                            l.to.node = matching_input.node.clone();
                                        }

                                        if l.from.node == o.id {
                                            let matching_output = outs
                                                .iter()
                                                .find(|x| x.node.starts_with(&*new_id))
                                                .ok_or_else(|| {
                                                    ZFError::NodeNotFound(new_id.clone())
                                                })?;
                                            l.from.node = matching_output.node.clone();
                                        }
                                    }

                                    // Updating inputs and outputs
                                    for i in &mut self.inputs {
                                        if i.node == o.id {
                                            i.node = new_id.clone();
                                        }
                                    }

                                    for out in &mut self.outputs {
                                        if out.node == o.id {
                                            out.node = new_id.clone();
                                        }
                                    }
                                }
                                Err(e) => {
                                    log::warn!("Unable to flatten {}, error {}", o.id, e);
                                    return Err(e);
                                }
                            }
                        }
                        Err(e) => {
                            log::warn!("Unable to flatten {}, error {}", self.id, e);
                            return Err(e);
                        }
                    }
                }
            }
        }

        Ok((simple_operators, self.links, self.inputs, self.outputs))
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
    pub fn from_yaml(data: &str) -> ZFResult<Self> {
        let dataflow_descriptor = serde_yaml::from_str::<NodeDescriptor>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
        Ok(dataflow_descriptor)
    }

    /// Creates a new `NodeDescriptor` from its JSON representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_json(data: &str) -> ZFResult<Self> {
        let dataflow_descriptor = serde_json::from_str::<NodeDescriptor>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
        Ok(dataflow_descriptor)
    }

    /// Returns the JSON representation of the `NodeDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_json(&self) -> ZFResult<String> {
        serde_json::to_string(&self).map_err(|_| ZFError::SerializationError)
    }

    /// Returns the YAML representation of the `NodeDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_yaml(&self) -> ZFResult<String> {
        serde_yaml::to_string(&self).map_err(|_| ZFError::SerializationError)
    }

    /// Flattens the `NodeDescriptor` by loading all the composite operators
    ///
    ///  # Errors
    /// A variant error is returned if loading operators fails. Or if the
    ///  node does not contains an operator
    pub async fn flatten(
        self,
        id: NodeId,
        global_configuration: Option<Configuration>,
    ) -> ZFResult<(
        Vec<SimpleOperatorDescriptor>,
        Vec<LinkDescriptor>,
        Vec<InputDescriptor>,
        Vec<OutputDescriptor>,
    )> {
        let descriptor_path = async_std::fs::read_to_string(&self.descriptor).await?;
        log::trace!("Loading operator {}", self.descriptor);
        // We try to load the descriptor, first we try as simple one, if it fails
        // we try as a composite one, if that also fails it is malformed.
        match SimpleOperatorDescriptor::from_yaml(&descriptor_path) {
            Ok(mut desc) => {
                desc.id = id;
                desc.configuration = merge_configurations(
                    global_configuration,
                    merge_configurations(desc.configuration, self.configuration.clone()),
                );
                let ins = desc
                    .inputs
                    .iter()
                    .map(|e| InputDescriptor {
                        node: desc.id.clone(),
                        input: e.port_id.clone(),
                    })
                    .collect();
                let outs = desc
                    .outputs
                    .iter()
                    .map(|e| OutputDescriptor {
                        node: desc.id.clone(),
                        output: e.port_id.clone(),
                    })
                    .collect();
                Ok((vec![desc], vec![], ins, outs))
            }
            Err(_) => match CompositeOperatorDescriptor::from_yaml(&descriptor_path) {
                Ok(desc) => desc.flatten(id, global_configuration).await,
                Err(e) => {
                    log::warn!("Unable to flatten {}, error {}", self.id, e);
                    Err(e)
                }
            },
        }

        // Err(ZFError::Unimplemented)
    }

    /// Loads the source from the `NodeDescriptor`
    ///
    ///  # Errors
    /// A variant error is returned if loading source fails. Or if the
    ///  node does not contains an source
    pub async fn load_source(
        self,
        global_configuration: Option<Configuration>,
    ) -> ZFResult<SourceDescriptor> {
        let descriptor_path = async_std::fs::read_to_string(&self.descriptor).await?;
        log::trace!("Loading source {}", self.descriptor);
        // We try to load the descriptor, first we try as simple one, if it fails
        // we try as a composite one, if that also fails it is malformed.
        match SourceDescriptor::from_yaml(&descriptor_path) {
            Ok(mut desc) => {
                desc.id = self.id;
                desc.configuration = merge_configurations(global_configuration, desc.configuration);
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
    ) -> ZFResult<SinkDescriptor> {
        let descriptor_path = async_std::fs::read_to_string(&self.descriptor).await?;
        log::trace!("Loading sink {}", self.descriptor);

        // We try to load the descriptor, first we try as simple one, if it fails
        // we try as a composite one, if that also fails it is malformed.
        match SinkDescriptor::from_yaml(&descriptor_path) {
            Ok(mut desc) => {
                desc.id = self.id;
                desc.configuration = merge_configurations(global_configuration, desc.configuration);
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

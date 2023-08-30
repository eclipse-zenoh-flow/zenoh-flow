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

use crate::model::descriptor::validator::DataFlowValidator;
use crate::model::descriptor::Vars;
use crate::model::descriptor::{
    LinkDescriptor, NodeDescriptor, OperatorDescriptor, SinkDescriptor, SourceDescriptor,
};
use crate::types::configuration::Merge;
use crate::types::{Configuration, NodeId, RuntimeId};
use crate::zfresult::ErrorKind;
use crate::{zferror, Result};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::hash::{Hash, Hasher};

/// The description of a data flow graph.
/// It contains all the information needed to instantiate a data flow graph.
///
/// Example:
/// ```yaml
/// flow: SimplePipeline
/// operators:
///   - id : SumOperator
///     descriptor: file://./target/release/sum_and_send.yaml
/// sources:
///   - id : Counter
///     descriptor: file://./target/release/counter_source.yaml
/// sinks:
///   - id : PrintSink
///     descriptor: file://./target/release/generic_sink.yaml
///
/// links:
/// - from:
///     node : Counter
///     output : Counter
///   to:
///     node : SumOperator
///     input : Number
/// - from:
///     node : SumOperator
///     output : Sum
///   to:
///     node : PrintSink
///     input : Data
///
/// mapping:
///   - id: SumOperator
///     runtime: runtime1
///   - id: Counter
///     runtime: runtime0
///   - id: PrintSink
///     runtime: runtime0
/// ```
///
///
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataFlowDescriptor {
    pub flow: String,
    pub operators: Vec<NodeDescriptor>,
    pub sources: Vec<NodeDescriptor>,
    pub sinks: Vec<NodeDescriptor>,
    pub links: Vec<LinkDescriptor>,
    pub mapping: Option<HashMap<NodeId, RuntimeId>>,
    #[serde(alias = "configuration")]
    pub global_configuration: Option<Configuration>,
}

impl DataFlowDescriptor {
    /// Creates a new `DataFlowDescriptor` from its YAML representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_yaml(data: &str) -> Result<Self> {
        let descriptor = Vars::expand_mustache_yaml(data)?;
        let dataflow_descriptor = serde_yaml::from_str::<DataFlowDescriptor>(&descriptor)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    /// Creates a new `DataFlowDescriptor` from its JSON representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_json(data: &str) -> Result<Self> {
        let descriptor = Vars::expand_mustache_json(data)?;
        let dataflow_descriptor = serde_json::from_str::<DataFlowDescriptor>(&descriptor)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        Ok(dataflow_descriptor)
    }

    /// Returns the JSON representation of the `DataFlowDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }

    /// Returns the YAML representation of the `DataFlowDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_yaml(&self) -> Result<String> {
        serde_yaml::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }

    /// Gets all the `RuntimeId` mapped to nodes of this `DataFlowDescriptor`.
    pub fn get_runtimes(&self) -> Vec<RuntimeId> {
        match &self.mapping {
            Some(mapping) => mapping.values().cloned().unique().collect(),
            None => vec![],
        }
    }

    /// Flattens the `DataFlowDescriptor` by loading all the composite operators
    /// returns the [`FlattenDataFlowDescriptor`](`FlattenDataFlowDescriptor`)
    ///
    ///  # Errors
    /// A variant error is returned if loading operators fails.
    pub async fn flatten(self) -> Result<FlattenDataFlowDescriptor> {
        let Self {
            flow,
            operators,
            sources,
            sinks,
            mut links,
            mapping,
            global_configuration,
        } = self;

        let mut flattened_sources = Vec::with_capacity(sources.len());
        for source in sources {
            let config = global_configuration
                .clone()
                .merge_overwrite(source.configuration.clone());
            flattened_sources.push(source.load_source(config).await?);
        }

        let mut flattened_sinks = Vec::with_capacity(sinks.len());
        for sink in sinks {
            let config = global_configuration
                .clone()
                .merge_overwrite(sink.configuration.clone());
            flattened_sinks.push(sink.load_sink(config).await?);
        }

        let mut flattened_operators = Vec::new();
        for operator in operators {
            let config = global_configuration
                .clone()
                .merge_overwrite(operator.configuration.clone());

            let id = operator.id.clone();
            let mut flattened = operator
                .flatten(id, &mut links, config, &mut Vec::new())
                .await?;
            flattened_operators.append(&mut flattened);
        }

        Ok(FlattenDataFlowDescriptor {
            flow,
            sources: flattened_sources,
            sinks: flattened_sinks,
            operators: flattened_operators,
            links,
            mapping,
            global_configuration,
        })
    }
}

impl Hash for DataFlowDescriptor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.flow.hash(state);
    }
}

impl PartialEq for DataFlowDescriptor {
    fn eq(&self, other: &DataFlowDescriptor) -> bool {
        self.flow == other.flow
    }
}

impl Eq for DataFlowDescriptor {}

/// The flatten description of a data flow graph.
/// A flatten descriptor does not contain any composite operator
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FlattenDataFlowDescriptor {
    pub flow: String,
    pub operators: Vec<OperatorDescriptor>,
    pub sources: Vec<SourceDescriptor>,
    pub sinks: Vec<SinkDescriptor>,
    pub links: Vec<LinkDescriptor>,
    pub mapping: Option<HashMap<NodeId, RuntimeId>>,
    #[serde(alias = "configuration")]
    pub global_configuration: Option<Configuration>,
}

impl FlattenDataFlowDescriptor {
    /// Creates a new `FlattenDataFlowDescriptor` from its YAML representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_yaml(data: &str) -> Result<FlattenDataFlowDescriptor> {
        let dataflow_descriptor = serde_yaml::from_str::<FlattenDataFlowDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        dataflow_descriptor.validate()?;
        Ok(dataflow_descriptor)
    }

    /// Creates a new `FlattenDataFlowDescriptor` from its JSON representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_json(data: &str) -> Result<FlattenDataFlowDescriptor> {
        let dataflow_descriptor = serde_json::from_str::<FlattenDataFlowDescriptor>(data)
            .map_err(|e| zferror!(ErrorKind::ParsingError, e))?;
        dataflow_descriptor.validate()?;
        Ok(dataflow_descriptor)
    }

    /// Returns the JSON representation of the `FlattenDataFlowDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }

    /// Returns the YAML representation of the `FlattenDataFlowDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_yaml(&self) -> Result<String> {
        serde_yaml::to_string(&self).map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
    }

    /// Gets all the `RuntimeId` mapped to nodes of this `FlattenDataFlowDescriptor`.
    pub fn get_runtimes(&self) -> Vec<RuntimeId> {
        match &self.mapping {
            Some(mapping) => mapping.values().cloned().unique().collect(),
            None => vec![],
        }
    }

    /// This method checks that the dataflow graph is correct.
    ///
    /// In particular it verifies that:
    /// - each node has a unique id,
    /// - each port (input and output) is connected,
    /// - an input port is connected only once (i.e. it receives data from a single output port),
    /// - connected ports are declared with the same type,
    /// - the dataflow, without the loops, is a DAG,
    /// - the end-to-end deadlines are correct,
    /// - the loops are valid.
    ///
    ///  # Errors
    /// A variant error is returned if validation fails.
    pub fn validate(&self) -> Result<()> {
        let validator = DataFlowValidator::try_from(self)?;
        validator.validate_ports()?;
        Ok(())
    }
}

impl Hash for FlattenDataFlowDescriptor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.flow.hash(state);
    }
}

impl PartialEq for FlattenDataFlowDescriptor {
    fn eq(&self, other: &FlattenDataFlowDescriptor) -> bool {
        self.flow == other.flow
    }
}

impl Eq for FlattenDataFlowDescriptor {}

#[cfg(test)]
#[path = "./tests/flatten-descriptor.rs"]
mod tests;

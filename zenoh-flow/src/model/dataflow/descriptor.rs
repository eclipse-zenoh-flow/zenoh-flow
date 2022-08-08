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

use crate::model::dataflow::flag::Flag;
use crate::model::dataflow::validator::DataflowValidator;
use crate::model::link::LinkDescriptor;
use crate::model::node::{
    NodeDescriptor, SimpleOperatorDescriptor, SinkDescriptor, SourceDescriptor,
};
use crate::serde::{Deserialize, Serialize};
use crate::types::{NodeId, RuntimeId, ZFError, ZFResult};
use crate::Configuration;
use itertools::Itertools;
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
    pub flags: Option<Vec<Flag>>,
}

impl DataFlowDescriptor {
    /// Creates a new `DataFlowDescriptor` from its YAML representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_yaml(data: &str) -> ZFResult<Self> {
        let dataflow_descriptor = serde_yaml::from_str::<DataFlowDescriptor>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
        // dataflow_descriptor.validate()?;
        Ok(dataflow_descriptor)
    }

    /// Creates a new `DataFlowDescriptor` from its JSON representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_json(data: &str) -> ZFResult<Self> {
        let dataflow_descriptor = serde_json::from_str::<DataFlowDescriptor>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
        // dataflow_descriptor.validate()?;
        Ok(dataflow_descriptor)
    }

    /// Returns the JSON representation of the `DataFlowDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_json(&self) -> ZFResult<String> {
        serde_json::to_string(&self).map_err(|_| ZFError::SerializationError)
    }

    /// Returns the YAML representation of the `DataFlowDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_yaml(&self) -> ZFResult<String> {
        serde_yaml::to_string(&self).map_err(|_| ZFError::SerializationError)
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
    // /// A variant error is returned if loading operators fails.
    pub async fn flatten(self) -> ZFResult<FlattenDataFlowDescriptor> {
        let mut sources = vec![];
        let mut sinks = vec![];
        let mut operators = vec![];
        let mut links = vec![];

        // first adding back all the links
        for l in self.links {
            links.push(l);
        }

        // loading sources
        for s in self.sources {
            sources.push(s.load_source(self.global_configuration.clone()).await?);
        }

        // loading sinks
        for s in self.sinks {
            sinks.push(s.load_sink(self.global_configuration.clone()).await?);
        }

        // loading operators
        for o in self.operators {
            let oid = o.id.clone();
            let (ops, lnks, ins, outs) = o
                .flatten(oid.clone(), self.global_configuration.clone())
                .await?;

            operators.extend(ops);
            links.extend(lnks);

            // Updating the links
            for l in &mut links {
                if l.to.node == oid {
                    let matching_input = ins
                        .iter()
                        .find(|x| x.node.starts_with(&*oid))
                        .ok_or_else(|| ZFError::NodeNotFound(oid.clone()))?;
                    l.to.node = matching_input.node.clone();
                }

                if l.from.node == oid {
                    let matching_output = outs
                        .iter()
                        .find(|x| x.node.starts_with(&*oid))
                        .ok_or_else(|| ZFError::NodeNotFound(oid.clone()))?;
                    l.from.node = matching_output.node.clone();
                }
            }
        }

        Ok(FlattenDataFlowDescriptor {
            flow: self.flow,
            operators,
            sources,
            sinks,
            links,
            mapping: None,
            global_configuration: None,
            flags: None,
        })
    }

    // }
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
    pub operators: Vec<SimpleOperatorDescriptor>,
    pub sources: Vec<SourceDescriptor>,
    pub sinks: Vec<SinkDescriptor>,
    pub links: Vec<LinkDescriptor>,
    pub mapping: Option<HashMap<NodeId, RuntimeId>>,
    #[serde(alias = "configuration")]
    pub global_configuration: Option<Configuration>,
    pub flags: Option<Vec<Flag>>,
}

impl FlattenDataFlowDescriptor {
    /// Creates a new `FlattenDataFlowDescriptor` from its YAML representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_yaml(data: &str) -> ZFResult<Self> {
        let dataflow_descriptor = serde_yaml::from_str::<FlattenDataFlowDescriptor>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
        dataflow_descriptor.validate()?;
        Ok(dataflow_descriptor)
    }

    /// Creates a new `FlattenDataFlowDescriptor` from its JSON representation.
    ///
    ///  # Errors
    /// A variant error is returned if deserialization fails.
    pub fn from_json(data: &str) -> ZFResult<Self> {
        let dataflow_descriptor = serde_json::from_str::<FlattenDataFlowDescriptor>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
        dataflow_descriptor.validate()?;
        Ok(dataflow_descriptor)
    }

    /// Returns the JSON representation of the `FlattenDataFlowDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_json(&self) -> ZFResult<String> {
        serde_json::to_string(&self).map_err(|_| ZFError::SerializationError)
    }

    /// Returns the YAML representation of the `FlattenDataFlowDescriptor`.
    ///
    ///  # Errors
    /// A variant error is returned if serialization fails.
    pub fn to_yaml(&self) -> ZFResult<String> {
        serde_yaml::to_string(&self).map_err(|_| ZFError::SerializationError)
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
    pub fn validate(&self) -> ZFResult<()> {
        let validator = DataflowValidator::try_from(self)?;

        validator.validate_ports()?;

        validator.validate_dag()?;

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

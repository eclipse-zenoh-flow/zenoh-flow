//
// Copyright (c) 2017, 2021 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//

use crate::model::link::LinkDescriptor;
use crate::model::node::{OperatorDescriptor, SinkDescriptor, SourceDescriptor};
use crate::serde::{Deserialize, Serialize};
use crate::types::{NodeId, RuntimeId, ZFError, ZFResult};
use crate::{PortId, PortType};
use petgraph::graph::NodeIndex;
use petgraph::Graph;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::hash::{Hash, Hasher};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Mapping {
    pub id: NodeId,
    pub runtime: RuntimeId,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataFlowDescriptor {
    pub flow: String,
    pub operators: Vec<OperatorDescriptor>,
    pub sources: Vec<SourceDescriptor>,
    pub sinks: Vec<SinkDescriptor>,
    pub links: Vec<LinkDescriptor>,
    pub mapping: Option<Vec<Mapping>>,
}

impl DataFlowDescriptor {
    pub fn from_yaml(data: &str) -> ZFResult<Self> {
        let dataflow_descriptor = serde_yaml::from_str::<DataFlowDescriptor>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
        dataflow_descriptor.validate()?;
        Ok(dataflow_descriptor)
    }

    pub fn from_json(data: &str) -> ZFResult<Self> {
        let dataflow_descriptor = serde_json::from_str::<DataFlowDescriptor>(data)
            .map_err(|e| ZFError::ParsingError(format!("{}", e)))?;
        dataflow_descriptor.validate()?;
        Ok(dataflow_descriptor)
    }
    pub fn to_json(&self) -> ZFResult<String> {
        serde_json::to_string(&self).map_err(|_| ZFError::SerializationError)
    }

    pub fn to_yaml(&self) -> ZFResult<String> {
        serde_yaml::to_string(&self).map_err(|_| ZFError::SerializationError)
    }

    pub fn get_mapping(&self, id: &str) -> Option<RuntimeId> {
        match &self.mapping {
            Some(mapping) => mapping
                .iter()
                .find(|&o| o.id.as_ref() == id)
                .map(|m| m.runtime.clone()),
            None => None,
        }
    }

    pub fn add_mapping(&mut self, mapping: Mapping) {
        match self.mapping.as_mut() {
            Some(m) => m.push(mapping),
            None => self.mapping = Some(vec![mapping]),
        }
    }

    pub fn get_runtimes(&self) -> Vec<RuntimeId> {
        let mut runtimes = HashSet::new();

        match &self.mapping {
            Some(mapping) => {
                for node_mapping in mapping.iter() {
                    runtimes.insert(node_mapping.runtime.clone());
                }
            }
            None => (),
        }
        runtimes.into_iter().collect()
    }

    // This method checks that the dataflow graph is correct.
    //
    // In particular it verifies that:
    // - each node has a unique id,
    // - each port (input and output) is connected,
    // - an input port is connected only once (i.e. it receives data from a single output port),
    // - connected ports are declared with the same type.
    fn validate(&self) -> ZFResult<()> {
        let validator = DataflowValidator::try_from(self)?;
        validator.validate()
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

/// `DataflowValidator` performs verifications on the graph.
///
/// In particular it verifies that:
/// - each node has a unique id,
/// - each port (input and output) is connected,
/// - an input port is connected only once (i.e. it receives data from a single output port),
/// - connected ports are declared with the same type.
///
/// To perform these verifications, two directed `petgraph` graphs are created: `node_checker` and
/// `graph_checker`.
///
/// `graph_checker` is an accurate representation of the dataflow. We use it to apply well-known
/// graph algorithms.
///
/// `node_checker, is used to check that all ports are connected and that input ports do not have
/// more than one link. In `node_checker` we view a pair `(NodeId, PortId)` as a "node". For example,
/// an Operator with one input and one output will be viewed as two nodes: (Operator, input) and
/// (Operator, output).
///
/// We store additional information in dedicated structures:
/// - `input_indexes` stores the `node_checker` indexes of inputs (i.e. for Operators and Sinks),
/// - `output_indexes` does the same for all outputs (i.e. for Operators and Sources),
/// - `map_id_to_node_checker_idx` maps the `(NodeId, PortId)` to the indexes in `node_checker`,
/// - `map_id_to_type` maps the `(NodeId, PortId)` to the type declared in the YAML file,
/// - `map_id_to_graph_checker_idx` maps the `NodeId` to the indexes in `graph_checker`.
///
/// For each source we store:
/// - its `(node_id, output.port_id)` in `output_indexes`,
/// - the type of its output in the `map_id_to_type`,
/// - its `node_id` in `graph_checker`,
/// - its `(node_id, output.port_id)` in `node_checker`.
///
/// For each operator we store:
/// - its `node_id` in `graph_checker`,
/// - for each output:
///     - its `(node_id, output.port_id)` in `output_indexes`,
///     - the type of the output in the `map_id_to_type`,
///     - its `(node_id, output.port_id)` in `node_checker`.
/// - for each input:
///     - store its `(node_id, input.port_id)` in `input_indexes`,
///     - the type of the input in the `map_id_to_type`,
///     - its `(node_id, input.port_id)` in `node_check`.
///
/// For each sink we store:
/// - its `(node_id, input.port_id)` in `input_indexes`,
/// - the type of its input in the `map_id_to_type`,
/// - its `node_id` in `graph_checker`,
/// - its `(node_id, input.port_id)` in `node_checker`.
struct DataflowValidator {
    graph_checker: Graph<NodeId, (PortId, PortId)>,
    node_checker: Graph<(NodeId, PortId), PortType>,
    input_indexes: HashSet<NodeIndex>,
    output_indexes: HashSet<NodeIndex>,
    map_id_to_node_checker_idx: HashMap<(NodeId, PortId), NodeIndex>,
    map_id_to_type: HashMap<(NodeId, PortId), PortType>,
    map_id_to_graph_checker_idx: HashMap<NodeId, NodeIndex>,
}

impl TryFrom<&DataFlowDescriptor> for DataflowValidator {
    type Error = ZFError;

    fn try_from(descriptor: &DataFlowDescriptor) -> Result<Self, Self::Error> {
        let mut validator = DataflowValidator::new();
        descriptor
            .sources
            .iter()
            .try_for_each(|source| validator.try_add_source(source))?;
        descriptor
            .operators
            .iter()
            .try_for_each(|operator| validator.try_add_operator(operator))?;
        descriptor
            .sinks
            .iter()
            .try_for_each(|sink| validator.try_add_sink(sink))?;
        descriptor
            .links
            .iter()
            .try_for_each(|link| validator.try_add_link(link))?;

        Ok(validator)
    }
}

impl DataflowValidator {
    fn new() -> Self {
        Self {
            graph_checker: Graph::new(),
            node_checker: Graph::new(),
            input_indexes: HashSet::new(),
            output_indexes: HashSet::new(),
            map_id_to_node_checker_idx: HashMap::new(),
            map_id_to_type: HashMap::new(),
            map_id_to_graph_checker_idx: HashMap::new(),
        }
    }

    /// `try_add_id` returns an error if two nodes have the same id.
    fn try_add_id(&mut self, node_id: NodeId) -> ZFResult<()> {
        let graph_checker_idx = self.graph_checker.add_node(node_id.clone());
        if self
            .map_id_to_graph_checker_idx
            .insert(node_id.clone(), graph_checker_idx)
            .is_some()
        {
            return Err(ZFError::DuplicatedNodeId(node_id));
        }

        Ok(())
    }

    /// `try_add_node` can fail if two ports (input / output), for the same node, have the same id.
    fn try_add_node(
        &mut self,
        node_id: NodeId,
        port_id: PortId,
        port_type: PortType,
    ) -> ZFResult<NodeIndex> {
        let id = (node_id, port_id);
        let node_checker_idx = self.node_checker.add_node(id.clone());
        if self
            .map_id_to_node_checker_idx
            .insert(id.clone(), node_checker_idx)
            .is_some()
        {
            return Err(ZFError::DuplicatedPort(id));
        }
        self.map_id_to_type.insert(id, port_type);

        Ok(node_checker_idx)
    }

    fn try_add_input(
        &mut self,
        node_id: NodeId,
        port_id: PortId,
        port_type: PortType,
    ) -> ZFResult<()> {
        let node_checker_idx = self.try_add_node(node_id, port_id, port_type)?;
        self.input_indexes.insert(node_checker_idx);
        Ok(())
    }

    /// `try_add_output` can fail when calling `try_add_node`.
    fn try_add_output(
        &mut self,
        node_id: NodeId,
        port_id: PortId,
        port_type: PortType,
    ) -> ZFResult<()> {
        let node_checker_idx = self.try_add_node(node_id, port_id, port_type)?;
        self.output_indexes.insert(node_checker_idx);
        Ok(())
    }

    fn try_add_source(&mut self, source: &SourceDescriptor) -> ZFResult<()> {
        self.try_add_id(source.id.clone())?;

        self.try_add_output(
            source.id.clone(),
            source.output.port_id.clone(),
            source.output.port_type.clone(),
        )
    }
    fn try_add_sink(&mut self, sink: &SinkDescriptor) -> ZFResult<()> {
        self.try_add_id(sink.id.clone())?;

        self.try_add_input(
            sink.id.clone(),
            sink.input.port_id.clone(),
            sink.input.port_type.clone(),
        )
    }

    fn try_add_operator(&mut self, operator: &OperatorDescriptor) -> ZFResult<()> {
        self.try_add_id(operator.id.clone())?;

        operator.inputs.iter().try_for_each(|input| {
            self.try_add_input(
                operator.id.clone(),
                input.port_id.clone(),
                input.port_type.clone(),
            )
        })?;

        operator.outputs.iter().try_for_each(|output| {
            self.try_add_output(
                operator.id.clone(),
                output.port_id.clone(),
                output.port_type.clone(),
            )
        })
    }

    fn try_add_link(&mut self, link: &LinkDescriptor) -> ZFResult<()> {
        let from_idx = self
            .map_id_to_graph_checker_idx
            .get(&link.from.node)
            .ok_or_else(|| ZFError::NodeNotFound(link.from.node.clone()))?;
        let to_idx = self
            .map_id_to_graph_checker_idx
            .get(&link.to.node)
            .ok_or_else(|| ZFError::NodeNotFound(link.to.node.clone()))?;

        self.graph_checker.add_edge(
            *from_idx,
            *to_idx,
            (link.from.output.clone(), link.to.input.clone()),
        );

        let from_id = (link.from.node.clone(), link.from.output.clone());
        let to_id = (link.to.node.clone(), link.to.input.clone());

        let from_type = self
            .map_id_to_type
            .get(&from_id)
            .ok_or_else(|| ZFError::PortNotFound(from_id.clone()))?;
        let to_type = self
            .map_id_to_type
            .get(&to_id)
            .ok_or_else(|| ZFError::PortNotFound(to_id.clone()))?;
        if from_type != to_type {
            return Err(ZFError::PortTypeNotMatching((
                from_type.clone(),
                to_type.clone(),
            )));
        }

        let from_idx = self.map_id_to_node_checker_idx.get(&from_id).unwrap();
        let to_idx = self.map_id_to_node_checker_idx.get(&to_id).unwrap();

        self.node_checker
            .add_edge(*from_idx, *to_idx, from_type.clone());
        Ok(())
    }

    fn validate(&self) -> ZFResult<()> {
        self.input_indexes.iter().try_for_each(|idx| {
            match self
                .node_checker
                .edges_directed(*idx, petgraph::EdgeDirection::Incoming)
                .count()
            {
                0 => Err(ZFError::PortNotConnected(
                    self.node_checker.node_weight(*idx).unwrap().clone(),
                )),
                1 => Ok(()),
                _ => Err(ZFError::MultipleOutputsToInput(
                    self.node_checker.node_weight(*idx).unwrap().clone(),
                )),
            }
        })?;

        self.output_indexes.iter().try_for_each(|idx| {
            match self
                .node_checker
                .edges_directed(*idx, petgraph::EdgeDirection::Outgoing)
                .count()
            {
                0 => Err(ZFError::PortNotConnected(
                    self.node_checker.node_weight(*idx).unwrap().clone(),
                )),
                _ => Ok(()),
            }
        })
    }
}

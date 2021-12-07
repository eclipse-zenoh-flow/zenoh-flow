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

use crate::model::dataflow::descriptor::DataFlowDescriptor;
use crate::model::link::PortDescriptor;
use crate::model::{InputDescriptor, OutputDescriptor};
use crate::types::{NodeId, ZFError, ZFResult};
use crate::{PortId, PortType};
use petgraph::graph::{EdgeIndex, NodeIndex};
use petgraph::Graph;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;

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
pub(crate) struct DataflowValidator {
    graph_checker: Graph<NodeId, (PortId, PortId, EdgeIndex)>,
    node_checker: Graph<PortUniqueId, PortType>,
    input_indexes: HashSet<NodeIndex>,
    output_indexes: HashSet<NodeIndex>,
    map_id_to_node_checker_idx: HashMap<PortUniqueId, NodeIndex>,
    map_id_to_type: HashMap<PortUniqueId, PortType>,
    map_id_to_graph_checker_idx: HashMap<NodeId, NodeIndex>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum PortKind {
    Input,
    Output,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct PortUniqueId {
    node_id: NodeId,
    port_id: PortId,
    kind: PortKind,
}

impl TryFrom<&DataFlowDescriptor> for DataflowValidator {
    type Error = ZFError;

    fn try_from(descriptor: &DataFlowDescriptor) -> Result<Self, Self::Error> {
        let mut validator = DataflowValidator::new();
        descriptor.sources.iter().try_for_each(|source| {
            validator.try_add_source(source.id.clone(), source.output.clone())
        })?;
        descriptor.operators.iter().try_for_each(|operator| {
            validator.try_add_operator(operator.id.clone(), &operator.inputs, &operator.outputs)
        })?;

        descriptor
            .sinks
            .iter()
            .try_for_each(|sink| validator.try_add_sink(sink.id.clone(), sink.input.clone()))?;
        descriptor
            .links
            .iter()
            .try_for_each(|link| validator.try_add_link(&link.from, &link.to))?;

        Ok(validator)
    }
}

impl DataflowValidator {
    pub(crate) fn new() -> Self {
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
        port: PortDescriptor,
        port_kind: PortKind,
    ) -> ZFResult<NodeIndex> {
        let id = PortUniqueId {
            node_id: node_id.clone(),
            port_id: port.port_id.clone(),
            kind: port_kind,
        };
        let node_checker_idx = self.node_checker.add_node(id.clone());
        if self
            .map_id_to_node_checker_idx
            .insert(id.clone(), node_checker_idx)
            .is_some()
        {
            return Err(ZFError::DuplicatedPort((node_id, port.port_id)));
        }
        self.map_id_to_type.insert(id, port.port_type);

        Ok(node_checker_idx)
    }

    fn try_add_input(&mut self, node_id: NodeId, input: PortDescriptor) -> ZFResult<()> {
        let node_checker_idx = self.try_add_node(node_id, input, PortKind::Input)?;
        self.input_indexes.insert(node_checker_idx);
        Ok(())
    }

    /// `try_add_output` can fail when calling `try_add_node`.
    fn try_add_output(&mut self, node_id: NodeId, output: PortDescriptor) -> ZFResult<()> {
        let node_checker_idx = self.try_add_node(node_id, output, PortKind::Output)?;
        self.output_indexes.insert(node_checker_idx);
        Ok(())
    }

    pub(crate) fn try_add_source(
        &mut self,
        node_id: NodeId,
        output: PortDescriptor,
    ) -> ZFResult<()> {
        self.try_add_id(node_id.clone())?;

        self.try_add_output(node_id, output)
    }

    pub(crate) fn try_add_sink(&mut self, node_id: NodeId, input: PortDescriptor) -> ZFResult<()> {
        self.try_add_id(node_id.clone())?;
        self.try_add_input(node_id, input)
    }

    pub(crate) fn try_add_operator(
        &mut self,
        node_id: NodeId,
        inputs: &[PortDescriptor],
        outputs: &[PortDescriptor],
    ) -> ZFResult<()> {
        self.try_add_id(node_id.clone())?;

        inputs
            .iter()
            .try_for_each(|input| self.try_add_input(node_id.clone(), input.clone()))?;

        outputs
            .iter()
            .try_for_each(|output| self.try_add_output(node_id.clone(), output.clone()))
    }

    pub(crate) fn try_add_link(
        &mut self,
        from: &OutputDescriptor,
        to: &InputDescriptor,
    ) -> ZFResult<()> {
        let from_graph_checker_idx = self
            .map_id_to_graph_checker_idx
            .get(&from.node)
            .ok_or_else(|| ZFError::NodeNotFound(from.node.clone()))?;
        let to_graph_checker_idx = self
            .map_id_to_graph_checker_idx
            .get(&to.node)
            .ok_or_else(|| ZFError::NodeNotFound(to.node.clone()))?;

        let from_id = PortUniqueId {
            node_id: from.node.clone(),
            port_id: from.output.clone(),
            kind: PortKind::Output,
        };
        let to_id = PortUniqueId {
            node_id: to.node.clone(),
            port_id: to.input.clone(),
            kind: PortKind::Input,
        };

        let from_type = self
            .map_id_to_type
            .get(&from_id)
            .ok_or_else(|| ZFError::PortNotFound((from.node.clone(), from.output.clone())))?;
        let to_type = self
            .map_id_to_type
            .get(&to_id)
            .ok_or_else(|| ZFError::PortNotFound((to.node.clone(), to.input.clone())))?;
        if from_type != to_type {
            return Err(ZFError::PortTypeNotMatching((
                from_type.clone(),
                to_type.clone(),
            )));
        }

        let edge_idx = self.graph_checker.add_edge(
            *from_graph_checker_idx,
            *to_graph_checker_idx,
            (from.output.clone(), to.input.clone(), EdgeIndex::default()),
        );
        // NOTE: We can "safely" unwrap as we just added that edge to the graph.
        let mut edge_weight = self.graph_checker.edge_weight_mut(edge_idx).unwrap();
        edge_weight.2 = edge_idx;

        let from_node_checker_idx = self.map_id_to_node_checker_idx.get(&from_id).unwrap();
        let to_node_checker_idx = self.map_id_to_node_checker_idx.get(&to_id).unwrap();

        self.node_checker.add_edge(
            *from_node_checker_idx,
            *to_node_checker_idx,
            from_type.clone(),
        );
        Ok(())
    }

    /// Validate that all ports respect the constraints.
    ///
    /// - an input port has one and only one incoming link,
    /// - an output port has at least one outgoing link.
    ///
    /// A link is represented by an "edge" in Petgraph vocabulary.
    pub(crate) fn validate_ports(&self) -> ZFResult<()> {
        self.input_indexes.iter().try_for_each(|idx| {
            match self
                .node_checker
                .edges_directed(*idx, petgraph::EdgeDirection::Incoming)
                .count()
            {
                0 => {
                    let port = self.node_checker.node_weight(*idx).unwrap();
                    Err(ZFError::PortNotConnected((
                        port.node_id.clone(),
                        port.port_id.clone(),
                    )))
                }
                1 => Ok(()),
                _ => {
                    let port = self.node_checker.node_weight(*idx).unwrap();
                    Err(ZFError::MultipleOutputsToInput((
                        port.node_id.clone(),
                        port.port_id.clone(),
                    )))
                }
            }
        })?;

        self.output_indexes.iter().try_for_each(|idx| {
            match self
                .node_checker
                .edges_directed(*idx, petgraph::EdgeDirection::Outgoing)
                .count()
            {
                0 => {
                    let port = self.node_checker.node_weight(*idx).unwrap();
                    Err(ZFError::PortNotConnected((
                        port.node_id.clone(),
                        port.port_id.clone(),
                    )))
                }
                _ => Ok(()),
            }
        })
    }

    /// Validate that the deadline can be enforced.
    ///
    /// In particular, we check that there is a path between the "from.output" and "to.input" nodes.
    /// we cannot just check that there is a path between "from" and "to" as we have to take
    /// into consideration the ports the user provided
    ///
    /// To perform that check we:
    /// 1) Isolate the incoming edge (singular!) that ends at the "to" node,
    /// 2) Get the index, in `graph_checker`, of the node that is at the origin of edge that we
    ///    isolated in step 1). This node is the node *before* the "to" node.
    ///
    /// 3) Isolate the outgoing edges that start at the "from" node.
    /// 4) For all the nodes that are at the end of all the edges isolated in step 3) (i.e. all
    ///    nodes that are *after* the "from" node for the provided port) check that there is a path
    ///    connecting them. If there is a path then the deadline is valid.
    ///
    /// NOTE: . Hence the extra steps.
    pub(crate) fn validate_deadline(
        &self,
        from: &OutputDescriptor,
        to: &InputDescriptor,
    ) -> ZFResult<()> {
        let to_idx = self
            .map_id_to_graph_checker_idx
            .get(&to.node)
            .ok_or_else(|| ZFError::NodeNotFound(to.node.clone()))?;
        let all_to_edges = self
            .graph_checker
            .edges_directed(*to_idx, petgraph::Incoming);
        let to_edges: Vec<_> = all_to_edges
            .filter(|edge| {
                let (_, input_port, _) = edge.weight();
                *input_port == to.input
            })
            .collect();

        // By construction, there should only be one "incoming edge".
        assert!(to_edges.len() == 1);
        let (_, _, edge_idx) = to_edges[0].weight();
        let (to_prev_idx, _) = self.graph_checker.edge_endpoints(*edge_idx).unwrap();

        let from_idx = self
            .map_id_to_graph_checker_idx
            .get(&from.node)
            .ok_or_else(|| ZFError::NodeNotFound(from.node.clone()))?;
        let all_from_edges = self
            .graph_checker
            .edges_directed(*from_idx, petgraph::Outgoing);
        let from_edges: Vec<_> = all_from_edges
            .filter(|edge| {
                let (output_port, _, _) = edge.weight();
                *output_port == from.output
            })
            .collect();

        let mut has_path = false;

        // `for` instead of `for_each` to be able to break early.
        for edge in from_edges.iter() {
            let (_, _, edge_idx) = edge.weight();
            let (_, from_next_idx) = self.graph_checker.edge_endpoints(*edge_idx).unwrap();

            if petgraph::algo::has_path_connecting(
                &self.graph_checker,
                from_next_idx,
                to_prev_idx,
                None,
            ) {
                has_path = true;
                break;
            }
        }

        if has_path {
            Ok(())
        } else {
            Err(ZFError::NoPathBetweenNodes((
                (from.node.clone(), from.output.clone()),
                (to.node.clone(), to.input.clone()),
            )))
        }
    }
}

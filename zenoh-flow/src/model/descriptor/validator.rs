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

use crate::model::descriptor::{FlattenDataFlowDescriptor, InputDescriptor, OutputDescriptor};
use crate::types::{NodeId, PortId};
use crate::zferror;
use crate::zfresult::ErrorKind;
use crate::Result as ZFResult;
use petgraph::graph::{EdgeIndex, NodeIndex};
use petgraph::Graph;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::sync::Arc;

/// `DataflowValidator` performs and allows performing verifications on the Dataflow graph.
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
/// `node_checker` is used to check that all ports are connected.
///  In `node_checker` we view a triplet `(NodeId, PortId, PortKind)` as a
/// "node". For example, an Operator with one input and one output will be viewed as two nodes:
/// (Operator, input, PortKind::Input) and (Operator, output, PortKind::Output).
/// These triplets are used to check that no two ports of the same kind and for the same node have
/// the same name. Ports of different kind for the same Operator can share the same name.
///
/// We store additional information in dedicated structures:
/// - `input_indexes` stores the `node_checker` indexes of inputs (i.e. for Operators and Sinks),
/// - `output_indexes` does the same for all outputs (i.e. for Operators and Sources),
/// - `map_id_to_node_checker_idx` maps the `(NodeId, PortId, PortKind)` to the indexes in
///   `node_checker`,
/// - `map_id_to_graph_checker_idx` maps the `NodeId` to the indexes in `graph_checker`,
/// - `loops_node_ids` stores the ids of the nodes involved in loops (ingress and egress).
///
/// Additional verifications are performed calling:
/// - `validate_ports`
/// - `validate_dag`
/// - `validate_deadline`
/// - `validate_loop`
pub(crate) struct DataFlowValidator {
    graph_checker: Graph<(NodeId, NodeKind), (PortId, PortId, EdgeIndex)>,
    node_checker: Graph<PortUniqueId, ()>,
    input_indexes: HashSet<NodeIndex>,
    output_indexes: HashSet<NodeIndex>,
    map_id_to_node_checker_idx: HashMap<PortUniqueId, NodeIndex>,
    map_id_to_graph_checker_idx: HashMap<NodeId, (NodeKind, NodeIndex)>,
}

/// Type of a Port, either Input or Output.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum PortKind {
    Input,
    Output,
}

/// The type of a Node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum NodeKind {
    Source,
    Sink,
    Operator,
}

/// Unique tuple that identifies a port.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct PortUniqueId {
    node_id: NodeId,
    port_id: PortId,
    kind: PortKind,
}

impl TryFrom<&FlattenDataFlowDescriptor> for DataFlowValidator {
    type Error = crate::zfresult::Error;

    fn try_from(descriptor: &FlattenDataFlowDescriptor) -> Result<Self, Self::Error> {
        let mut validator = DataFlowValidator::new();

        let mut all_node_ids = vec![];
        all_node_ids.extend(descriptor.sources.iter().map(|s| s.id.clone()));
        all_node_ids.extend(descriptor.operators.iter().map(|s| s.id.clone()));
        all_node_ids.extend(descriptor.sinks.iter().map(|s| s.id.clone()));

        descriptor
            .sources
            .iter()
            .try_for_each(|source| validator.try_add_source(source.id.clone(), &source.outputs))?;

        descriptor.operators.iter().try_for_each(|operator| {
            validator.try_add_operator(operator.id.clone(), &operator.inputs, &operator.outputs)
        })?;

        descriptor
            .sinks
            .iter()
            .try_for_each(|sink| validator.try_add_sink(sink.id.clone(), &sink.inputs))?;

        descriptor
            .links
            .iter()
            .try_for_each(|link| validator.try_add_link(&link.from, &link.to))?;

        Ok(validator)
    }
}

impl DataFlowValidator {
    /// Creates an empty `DataflowValidator`/
    pub(crate) fn new() -> Self {
        Self {
            graph_checker: Graph::new(),
            input_indexes: HashSet::new(),
            output_indexes: HashSet::new(),
            map_id_to_node_checker_idx: HashMap::new(),
            map_id_to_graph_checker_idx: HashMap::new(),
            node_checker: Graph::new(),
        }
    }

    /// Adds a new node id
    ///
    /// # Errors
    /// An error variant is returned if two nodes have the same id.
    fn try_add_id(&mut self, node_kind: NodeKind, node_id: NodeId) -> ZFResult<()> {
        let graph_checker_idx = self.graph_checker.add_node((node_id.clone(), node_kind));
        if self
            .map_id_to_graph_checker_idx
            .insert(node_id.clone(), (node_kind, graph_checker_idx))
            .is_some()
        {
            return Err(zferror!(ErrorKind::DuplicatedNodeId(node_id), "Duplicate node").into());
        }

        Ok(())
    }

    /// Adds a node
    ///
    /// # Errors
    /// It can fail if two ports (input / output), for the same node,
    ///  have the same id.
    fn try_add_node(
        &mut self,
        node_id: NodeId,
        port: PortId,
        port_kind: PortKind,
    ) -> ZFResult<NodeIndex> {
        let id = PortUniqueId {
            node_id: node_id.clone(),
            port_id: port.clone(),
            kind: port_kind,
        };
        let node_checker_idx = self.node_checker.add_node(id.clone());
        if self
            .map_id_to_node_checker_idx
            .insert(id, node_checker_idx)
            .is_some()
        {
            // NOTE `Arc::clone` for two reasons:
            // - Clippy considers calls to `.clone()` as redundant but rustc complains if there is
            //   no clone,
            // - this is just a temporary fix as we will introduce proper types that will implement
            //   Clone and (under the hood) use Arc::clone
            return Err(zferror!(ErrorKind::DuplicatedPort((
                Arc::clone(&node_id),
                Arc::clone(&port)
            )))
            .into());
        }

        Ok(node_checker_idx)
    }

    /// Adds an input
    ///
    /// # Errors
    /// It can fail when calling `try_add_node`.
    pub(crate) fn try_add_input(&mut self, node_id: NodeId, input: PortId) -> ZFResult<()> {
        let node_checker_idx = self.try_add_node(node_id, input, PortKind::Input)?;
        self.input_indexes.insert(node_checker_idx);
        Ok(())
    }

    /// Adds an output
    ///
    /// # Errors
    /// It can fail when calling `try_add_node`.
    pub(crate) fn try_add_output(&mut self, node_id: NodeId, output: PortId) -> ZFResult<()> {
        let node_checker_idx = self.try_add_node(node_id, output, PortKind::Output)?;
        self.output_indexes.insert(node_checker_idx);
        Ok(())
    }

    /// Adds a source
    ///
    /// #Errors
    /// It can fail when calling `try_add_output` or `try_add_id`.
    pub(crate) fn try_add_source(&mut self, node_id: NodeId, outputs: &[PortId]) -> ZFResult<()> {
        self.try_add_id(NodeKind::Source, node_id.clone())?;

        outputs
            .iter()
            .try_for_each(|output| self.try_add_output(node_id.clone(), output.clone()))
    }

    /// Adds a sink
    ///
    /// # Errors
    /// It can fail when calling `try_add_output` or `try_add_id`.
    pub(crate) fn try_add_sink(&mut self, node_id: NodeId, inputs: &[PortId]) -> ZFResult<()> {
        self.try_add_id(NodeKind::Sink, node_id.clone())?;

        inputs
            .iter()
            .try_for_each(|input| self.try_add_input(node_id.clone(), input.clone()))
    }

    /// Adds an operator
    ///
    /// # Errors
    /// It can fail when calling `try_add_output`, `try_add_id`
    /// or `try_add_input`.
    pub(crate) fn try_add_operator(
        &mut self,
        node_id: NodeId,
        inputs: &[PortId],
        outputs: &[PortId],
    ) -> ZFResult<()> {
        self.try_add_id(NodeKind::Operator, node_id.clone())?;

        inputs
            .iter()
            .try_for_each(|input| self.try_add_input(node_id.clone(), input.clone()))?;

        outputs
            .iter()
            .try_for_each(|output| self.try_add_output(node_id.clone(), output.clone()))
    }

    /// Adds a link, can fail if it does not find the ports.
    ///
    /// # Errors
    /// An error variant is returned if validation fails.
    pub(crate) fn try_add_link(
        &mut self,
        from: &OutputDescriptor,
        to: &InputDescriptor,
    ) -> ZFResult<()> {
        log::debug!("Looking for node < {} >…", &from.node);
        let (_, from_graph_checker_idx) = self
            .map_id_to_graph_checker_idx
            .get(&from.node)
            .ok_or_else(|| zferror!(ErrorKind::NodeNotFound(from.node.clone())))?;
        log::debug!("Looking for node < {} >… OK.", &from.node);
        log::debug!("Looking for node < {} >…", &to.node);
        let (_, to_graph_checker_idx) = self
            .map_id_to_graph_checker_idx
            .get(&to.node)
            .ok_or_else(|| zferror!(ErrorKind::NodeNotFound(to.node.clone())))?;
        log::debug!("Looking for node < {} >… OK.", &to.node);

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

        let edge_idx = self.graph_checker.add_edge(
            *from_graph_checker_idx,
            *to_graph_checker_idx,
            (from.output.clone(), to.input.clone(), EdgeIndex::default()),
        );
        log::trace!("FromId {from_id:?} ToId: {to_id:?}");
        let mut edge_weight = self
            .graph_checker
            .edge_weight_mut(edge_idx)
            .ok_or_else(|| {
                zferror!(
                    ErrorKind::NotFound,
                    "Link with id {edge_idx:?}, between {from:?} => {to:?} not found"
                )
            })?;
        edge_weight.2 = edge_idx;

        let from_node_checker_idx =
            self.map_id_to_node_checker_idx
                .get(&from_id)
                .ok_or_else(|| {
                    zferror!(ErrorKind::PortNotFound((
                        from.node.clone(),
                        from.output.clone()
                    )))
                })?;
        let to_node_checker_idx = self.map_id_to_node_checker_idx.get(&to_id).ok_or_else(|| {
            zferror!(ErrorKind::PortNotFound((to.node.clone(), to.input.clone())))
        })?;

        self.node_checker
            .add_edge(*from_node_checker_idx, *to_node_checker_idx, ());
        Ok(())
    }

    /// Validate that all ports respect the constraints.
    ///
    /// - an input port has at least one incoming link,
    /// - an output port has at least one outgoing link.
    ///
    /// A link is represented by an "edge" in Petgraph vocabulary.
    ///
    /// # Errors
    /// A variant error is returned if validation fails.
    pub(crate) fn validate_ports(&self) -> ZFResult<()> {
        self.input_indexes.iter().try_for_each(|idx| {
            match self
                .node_checker
                .edges_directed(*idx, petgraph::EdgeDirection::Incoming)
                .count()
            {
                0 => {
                    let port = self.node_checker.node_weight(*idx).unwrap();
                    Err(zferror!(ErrorKind::PortNotConnected((
                        port.node_id.clone(),
                        port.port_id.clone(),
                    ))))
                }
                _ => Ok(()),
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
                    Err(zferror!(ErrorKind::PortNotConnected((
                        port.node_id.clone(),
                        port.port_id.clone()
                    )))
                    .into())
                }
                _ => Ok(()),
            }
        })
    }
}

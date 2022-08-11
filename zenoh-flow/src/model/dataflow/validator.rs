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
use crate::model::dataflow::descriptor::FlattenDataFlowDescriptor;
use crate::model::dataflow::flag::{get_nodes_to_remove, Flag};
use crate::model::link::PortDescriptor;
use crate::model::{InputDescriptor, OutputDescriptor};
use crate::types::{NodeId, PortId, PortType, ZFResult, PORT_TYPE_ANY};
use petgraph::graph::{EdgeIndex, NodeIndex};
use petgraph::Graph;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;

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
/// `node_checker` is used to check that all ports are connected and that input ports do not have
/// more than one link. In `node_checker` we view a triplet `(NodeId, PortId, PortKind)` as a
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
/// - `map_id_to_type` maps the `(NodeId, PortId, PortKind)` to the type declared in the YAML file,
/// - `map_id_to_graph_checker_idx` maps the `NodeId` to the indexes in `graph_checker`,
/// - `loops_node_ids` stores the ids of the nodes involved in loops (ingress and egress).
///
/// Additional verifications are performed calling:
/// - `validate_ports`
/// - `validate_dag`
/// - `validate_deadline`
/// - `validate_loop`
pub(crate) struct DataflowValidator {
    graph_checker: Graph<(NodeId, NodeKind), (PortId, PortId, EdgeIndex)>,
    node_checker: Graph<PortUniqueId, PortType>,
    input_indexes: HashSet<NodeIndex>,
    output_indexes: HashSet<NodeIndex>,
    map_id_to_node_checker_idx: HashMap<PortUniqueId, NodeIndex>,
    map_id_to_type: HashMap<PortUniqueId, PortType>,
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

impl TryFrom<&FlattenDataFlowDescriptor> for DataflowValidator {
    type Error = ZFError;

    fn try_from(descriptor: &FlattenDataFlowDescriptor) -> Result<Self, Self::Error> {
        let mut validator = DataflowValidator::new();

        let mut all_node_ids = vec![];
        all_node_ids.extend(descriptor.sources.iter().map(|s| s.id.clone()));
        all_node_ids.extend(descriptor.operators.iter().map(|s| s.id.clone()));
        all_node_ids.extend(descriptor.sinks.iter().map(|s| s.id.clone()));

        let mut nodes_to_remove = HashSet::new();
        if let Some(flags) = &descriptor.flags {
            flags
                .iter()
                .try_for_each(|flag| validator.validate_flag(&all_node_ids, flag))?;

            nodes_to_remove = get_nodes_to_remove(flags);
        }

        descriptor
            .sources
            .iter()
            .filter(|&s| !nodes_to_remove.contains(&s.id))
            .try_for_each(|source| validator.try_add_source(source.id.clone(), &source.outputs))?;

        descriptor
            .operators
            .iter()
            .filter(|&o| !nodes_to_remove.contains(&o.id))
            .try_for_each(|operator| {
                validator.try_add_operator(operator.id.clone(), &operator.inputs, &operator.outputs)
            })?;

        descriptor
            .sinks
            .iter()
            .filter(|&s| !nodes_to_remove.contains(&s.id))
            .try_for_each(|sink| validator.try_add_sink(sink.id.clone(), &sink.inputs))?;

        descriptor
            .links
            .iter()
            .filter(|&l| {
                !nodes_to_remove.contains(&l.from.node) && !nodes_to_remove.contains(&l.to.node)
            })
            .try_for_each(|link| validator.try_add_link(&link.from, &link.to))?;

        Ok(validator)
    }
}

impl DataflowValidator {
    /// Creates an empty `DataflowValidator`/
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
            return Err(ZFError::DuplicatedNodeId(node_id));
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

    /// Adds an input
    ///
    /// # Errors
    /// It can fail when calling `try_add_node`.
    pub(crate) fn try_add_input(&mut self, node_id: NodeId, input: PortDescriptor) -> ZFResult<()> {
        let node_checker_idx = self.try_add_node(node_id, input, PortKind::Input)?;
        self.input_indexes.insert(node_checker_idx);
        Ok(())
    }

    /// Adds an output
    ///
    /// # Errors
    /// It can fail when calling `try_add_node`.
    pub(crate) fn try_add_output(
        &mut self,
        node_id: NodeId,
        output: PortDescriptor,
    ) -> ZFResult<()> {
        let node_checker_idx = self.try_add_node(node_id, output, PortKind::Output)?;
        self.output_indexes.insert(node_checker_idx);
        Ok(())
    }

    /// Adds a source
    ///
    /// #Errors
    /// It can fail when calling `try_add_output` or `try_add_id`.
    pub(crate) fn try_add_source(
        &mut self,
        node_id: NodeId,
        outputs: &[PortDescriptor],
    ) -> ZFResult<()> {
        self.try_add_id(NodeKind::Source, node_id.clone())?;

        outputs
            .iter()
            .try_for_each(|output| self.try_add_output(node_id.clone(), output.clone()))
    }

    /// Adds a sink
    ///
    /// # Errors
    /// It can fail when calling `try_add_output` or `try_add_id`.
    pub(crate) fn try_add_sink(
        &mut self,
        node_id: NodeId,
        inputs: &[PortDescriptor],
    ) -> ZFResult<()> {
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
        inputs: &[PortDescriptor],
        outputs: &[PortDescriptor],
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
            .ok_or_else(|| ZFError::NodeNotFound(from.node.clone()))?;
        log::debug!("Looking for node < {} >… OK.", &from.node);
        log::debug!("Looking for node < {} >…", &to.node);
        let (_, to_graph_checker_idx) = self
            .map_id_to_graph_checker_idx
            .get(&to.node)
            .ok_or_else(|| ZFError::NodeNotFound(to.node.clone()))?;
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

        log::debug!("Looking for port type of < {:?} >…", &from_id);
        let from_type = self
            .map_id_to_type
            .get(&from_id)
            .ok_or_else(|| ZFError::PortNotFound((from.node.clone(), from.output.clone())))?;
        log::debug!("Looking for port type of < {:?} >… OK.", &from_id);
        log::debug!("Looking for port type of < {:?} >…", &to_id);
        let to_type = self
            .map_id_to_type
            .get(&to_id)
            .ok_or_else(|| ZFError::PortNotFound((to.node.clone(), to.input.clone())))?;
        log::debug!("Looking for port type of < {:?} >… OK.", &to_id);
        log::debug!("Port types are identical…");
        if from_type != to_type
            && from_type.as_ref() != PORT_TYPE_ANY
            && to_type.as_ref() != PORT_TYPE_ANY
        {
            return Err(ZFError::PortTypeNotMatching((
                from_type.clone(),
                to_type.clone(),
            )));
        }
        log::debug!("Port types are identical… OK.");

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

    /// Validates that, without the Loops, the Dataflow is a Directed Acyclic Graph.
    ///
    /// # Errors
    /// A variant error is returned if validation fails.
    pub(crate) fn validate_dag(&self) -> ZFResult<()> {
        match petgraph::algo::is_cyclic_directed(&self.graph_checker) {
            true => Err(ZFError::InvalidData(
                "The dataflow contains a cycle, please use the \
                 `Loops` section to express this behavior."
                    .into(),
            )),
            false => Ok(()),
        }
    }

    /// Validate flag.
    ///
    /// A flag is valid if and only if all the nodes it references exist. We return an error if the
    /// flag is toggled, if not we simply emit a warning.
    pub(crate) fn validate_flag(&self, node_ids: &[NodeId], flag: &Flag) -> ZFResult<()> {
        for node in &flag.nodes {
            if !node_ids.contains(node) {
                if flag.toggle {
                    log::error!("Flag < {} >: unknown node {}", flag.id, node);
                    return Err(ZFError::NodeNotFound(node.clone()));
                }

                log::warn!("Flag < {} >: unknown node {}", flag.id, node);
            }
        }

        Ok(())
    }
}

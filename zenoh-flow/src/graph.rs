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

use async_std::sync::{Arc, Mutex};
use petgraph::dot::{Config, Dot};
use petgraph::graph::{EdgeIndex, NodeIndex};
use petgraph::stable_graph::StableGraph;
use petgraph::Direction;
use std::collections::HashMap;

use crate::link::{link, ZFLinkReceiver, ZFLinkSender};
use crate::loader::{
    load_operator, load_sink, load_source, ZFOperatorRunner, ZFSinkRunner, ZFSourceRunner,
};
use crate::message::ZFMessage;
use crate::serde::{Deserialize, Serialize};
use crate::types::{ZFLinkId, ZFResult, ZFError};
use crate::{
    ZFConnection, ZFOperatorDescription, ZFOperatorId, ZFSinkDescription, ZFSourceDescription,
};

pub enum Runner {
    Operator(ZFOperatorRunner),
    Source(ZFSourceRunner),
    Sink(ZFSinkRunner),
}

impl Runner {
    pub async fn run(&mut self) -> ZFResult<()> {
        match self {
            Runner::Operator(runner) => runner.run().await,
            Runner::Source(runner) => runner.run().await,
            Runner::Sink(runner) => runner.run().await,
        }
    }

    pub fn add_input(&mut self, input: ZFLinkReceiver<ZFMessage>) {
        match self {
            Runner::Operator(runner) => runner.add_input(input),
            Runner::Source(_runner) => panic!("Sources does not have inputs!"),
            Runner::Sink(runner) => runner.add_input(input),
        }
    }

    pub fn add_output(&mut self, output: ZFLinkSender<ZFMessage>) {
        match self {
            Runner::Operator(runner) => runner.add_output(output),
            Runner::Source(runner) => runner.add_output(output),
            Runner::Sink(_runner) => panic!("Sinks does not have output!"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataFlowNode {
    Operator(ZFOperatorDescription),
    Source(ZFSourceDescription),
    Sink(ZFSinkDescription),
}

impl std::fmt::Display for DataFlowNode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DataFlowNode::Operator(inner) => write!(f, "{}", inner),
            DataFlowNode::Source(inner) => write!(f, "{}", inner),
            DataFlowNode::Sink(inner) => write!(f, "{}", inner),
        }
    }
}

impl DataFlowNode {
    pub fn has_input(&self, id: ZFLinkId) -> bool {
        match self {
            DataFlowNode::Operator(op) => match op.inputs.iter().find(|&lid| *lid == id) {
                Some(_lid) => true,
                None => false,
            },
            DataFlowNode::Source(_) => false,
            DataFlowNode::Sink(sink) => match sink.inputs.iter().find(|&lid| *lid == id) {
                Some(_lid) => true,
                None => false,
            },
        }
    }

    pub fn has_output(&self, id: ZFLinkId) -> bool {
        match self {
            DataFlowNode::Operator(op) => match op.outputs.iter().find(|&lid| *lid == id) {
                Some(_lid) => true,
                None => false,
            },
            DataFlowNode::Sink(_) => false,
            DataFlowNode::Source(source) => match source.outputs.iter().find(|&lid| *lid == id) {
                Some(_lid) => true,
                None => false,
            },
        }
    }

    pub fn get_id(&self) -> ZFOperatorId {
        match self {
            DataFlowNode::Operator(op) => op.id.clone(),
            DataFlowNode::Sink(s) => s.id.clone(),
            DataFlowNode::Source(s) => s.id.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataFlowDescription {
    pub operators: Vec<ZFOperatorDescription>,
    pub sources: Vec<ZFSourceDescription>,
    pub sinks: Vec<ZFSinkDescription>,
    pub connections: Vec<ZFConnection>,
}

impl DataFlowDescription {
    pub fn from_yaml(data: String) -> Self {
        serde_yaml::from_str::<DataFlowDescription>(&data).unwrap()
    }

    pub fn find_node(&self, id: ZFOperatorId) -> Option<DataFlowNode> {
        match self.get_operator(id.clone()) {
            Some(o) => Some(DataFlowNode::Operator(o)),
            None => match self.get_source(id.clone()) {
                Some(s) => Some(DataFlowNode::Source(s)),
                None => match self.get_sink(id) {
                    Some(s) => Some(DataFlowNode::Sink(s)),
                    None => None,
                },
            },
        }
    }

    fn get_operator(&self, id: ZFOperatorId) -> Option<ZFOperatorDescription> {
        match self.operators.iter().find(|&o| o.id == id) {
            Some(o) => Some(o.clone()),
            None => None,
        }
    }

    fn get_source(&self, id: ZFOperatorId) -> Option<ZFSourceDescription> {
        match self.sources.iter().find(|&o| o.id == id) {
            Some(s) => Some(s.clone()),
            None => None,
        }
    }

    fn get_sink(&self, id: ZFOperatorId) -> Option<ZFSinkDescription> {
        match self.sinks.iter().find(|&o| o.id == id) {
            Some(s) => Some(s.clone()),
            None => None,
        }
    }
}

pub struct DataFlowGraph {
    pub operators: Vec<(NodeIndex, DataFlowNode)>,
    pub connections: Vec<(EdgeIndex, ZFConnection)>,
    pub graph: StableGraph<DataFlowNode, ZFLinkId>,
    pub operators_runners: HashMap<ZFOperatorId, Arc<Mutex<Runner>>>,
}

pub fn deserialize_dataflow_description(data: String) -> DataFlowDescription {
    serde_yaml::from_str::<DataFlowDescription>(&data).unwrap()
}

impl DataFlowGraph {
    pub fn new(df: Option<DataFlowDescription>) -> Self {
        match df {
            Some(df) => {
                let mut graph = StableGraph::<DataFlowNode, ZFLinkId>::new();
                let mut operators = Vec::new();
                let mut connections = Vec::new();
                for o in df.operators {
                    operators.push((
                        graph.add_node(DataFlowNode::Operator(o.clone())),
                        DataFlowNode::Operator(o),
                    ));
                }

                for o in df.sources {
                    operators.push((
                        graph.add_node(DataFlowNode::Source(o.clone())),
                        DataFlowNode::Source(o),
                    ));
                }

                for o in df.sinks {
                    operators.push((
                        graph.add_node(DataFlowNode::Sink(o.clone())),
                        DataFlowNode::Sink(o),
                    ));
                }

                for l in df.connections {
                    // First check if the LinkId are the same
                    if l.from.1 == l.to.1 {
                        let from_index =
                            match operators.iter().find(|&(_, o)| o.get_id() == l.from.0) {
                                Some((idx, op)) => match op.has_output(l.from.1) {
                                    true => idx,
                                    false => panic!("Link id {:?} not found in {:?}", l.from.1, op),
                                },
                                None => panic!("Not not found"),
                            };

                        let to_index = match operators.iter().find(|&(_, o)| o.get_id() == l.to.0) {
                            Some((idx, op)) => match op.has_input(l.to.1) {
                                true => idx,
                                false => panic!("Link id {:?} not found in {:?}", l.to.1, op),
                            },
                            None => panic!("Not not found"),
                        };

                        connections.push((graph.add_edge(*from_index, *to_index, 1), l));
                    } else {
                        panic!("Ports ID does not match")
                    }
                }

                Self {
                    operators,
                    connections,
                    graph,
                    operators_runners: HashMap::new(),
                }
            }
            None => Self {
                operators: Vec::new(),
                connections: Vec::new(),
                graph: StableGraph::<DataFlowNode, ZFLinkId>::new(),
                operators_runners: HashMap::new(),
            },
        }
    }

    pub fn to_dot_notation(&self) -> String {
        String::from(format!(
            "{}",
            Dot::with_config(&self.graph, &[Config::EdgeNoLabel])
        ))
    }


    pub fn add_static_operator(&mut self, id : ZFOperatorId, name: String, inputs : Vec<ZFLinkId>, outputs : Vec<ZFLinkId>,  operator : Box<dyn crate::operator::OperatorTrait + Send>) -> ZFResult<()> {
        let descriptor = ZFOperatorDescription{
            id : id.clone(),
            name,
            inputs,
            outputs,
            lib : "".to_string()
        };
        self.operators.push((
            self.graph.add_node(DataFlowNode::Operator(descriptor.clone())),
            DataFlowNode::Operator(descriptor),
        ));
        let runner = Runner::Operator(crate::loader::ZFOperatorRunner::new_static(operator));
        self.operators_runners.insert(id, Arc::new(Mutex::new(runner)));
        Ok(())
    }

    pub fn add_static_source(&mut self, id : ZFOperatorId, name: String, outputs : Vec<ZFLinkId>,  source : Box<dyn crate::operator::SourceTrait + Send>) -> ZFResult<()> {
        let descriptor = ZFSourceDescription{
            id : id.clone(),
            name,
            outputs,
            lib : "".to_string()
        };
        self.operators.push((
            self.graph.add_node(DataFlowNode::Source(descriptor.clone())),
            DataFlowNode::Source(descriptor),
        ));
        let runner = Runner::Source(crate::loader::ZFSourceRunner::new_static(source));
        self.operators_runners.insert(id, Arc::new(Mutex::new(runner)));
        Ok(())
    }

    pub fn add_static_sink(&mut self, id : ZFOperatorId, name: String, inputs : Vec<ZFLinkId>, sink : Box<dyn crate::operator::SinkTrait + Send>) -> ZFResult<()> {
        let descriptor = ZFSinkDescription{
            id : id.clone(),
            name,
            inputs,
            lib : "".to_string()
        };
        self.operators.push((
            self.graph.add_node(DataFlowNode::Sink(descriptor.clone())),
            DataFlowNode::Sink(descriptor),
        ));
        let runner = Runner::Sink(crate::loader::ZFSinkRunner::new_static(sink));
        self.operators_runners.insert(id, Arc::new(Mutex::new(runner)));
        Ok(())
    }

    pub fn add_link(&mut self, from : (ZFOperatorId,ZFLinkId), to: (ZFOperatorId, ZFLinkId)) -> ZFResult<()> {
        let connection = ZFConnection {
            from,
            to,
        };

        if connection.from.1 == connection.to.1 {
            let from_index =
                match self.operators.iter().find(|&(_, o)| o.get_id() == connection.from.0) {
                    Some((idx, op)) => match op.has_output(connection.from.1) {
                        true => idx,
                        false => return Err(ZFError::PortNotFound((connection.from.0, connection.from.1))),
                    },
                    None => return Err(ZFError::OperatorNotFound(connection.from.0)),
                };

            let to_index = match self.operators.iter().find(|&(_, o)| o.get_id() == connection.to.0) {
                Some((idx, op)) => match op.has_input(connection.to.1) {
                    true => idx,
                    false => return Err(ZFError::PortNotFound((connection.to.0, connection.to.1))),
                },
                None => return Err(ZFError::OperatorNotFound(connection.to.0)),
            };

            self.connections.push((self.graph.add_edge(*from_index, *to_index, 1), connection));
        } else {
            return Err(ZFError::PortIdNotMatching((connection.from.1, connection.to.1)))
        }


        Ok(())
    }

    pub fn load(&mut self) -> std::io::Result<()> {
        unsafe {
            for (_, op) in &self.operators {
                match op {
                    DataFlowNode::Operator(inner) => {
                        let (operator_id, runner) = load_operator(inner.lib.clone())?;
                        let runner = Runner::Operator(runner);
                        self.operators_runners
                            .insert(operator_id, Arc::new(Mutex::new(runner)));
                    }
                    DataFlowNode::Source(inner) => {
                        let (operator_id, runner) = load_source(inner.lib.clone())?;
                        let runner = Runner::Source(runner);
                        self.operators_runners
                            .insert(operator_id, Arc::new(Mutex::new(runner)));
                    }
                    DataFlowNode::Sink(inner) => {
                        let (operator_id, runner) = load_sink(inner.lib.clone())?;
                        let runner = Runner::Sink(runner);
                        self.operators_runners
                            .insert(operator_id, Arc::new(Mutex::new(runner)));
                    }
                }
            }
            Ok(())
        }
    }

    pub async fn make_connections(&mut self) {
        // Connects the operators via our FIFOs

        for (idx, up_op) in &self.operators {
            let mut up_runner = self
                .operators_runners
                .get(&up_op.get_id())
                .unwrap()
                .lock()
                .await;
            if self.graph.contains_node(*idx) {
                let mut downstreams = self
                    .graph
                    .neighbors_directed(*idx, Direction::Outgoing)
                    .detach();
                while let Some(down) = downstreams.next(&self.graph) {
                    let (_, jdx) = down;
                    let down_op = match self.operators.iter().find(|&(idx, _)| *idx == jdx) {
                        Some((_, op)) => op,
                        None => panic!("To not found"),
                    };
                    let (_, down_link) = match self.graph.find_edge(*idx, jdx) {
                        Some(l_idx) => self
                            .connections
                            .iter()
                            .find(|&(lidx, _)| *lidx == l_idx)
                            .unwrap(),
                        None => panic!("Link between {:?} -> {:?} not found", idx, jdx),
                    };

                    let link_id = down_link.from.1; //The check on the link id matching is done when creating the graph

                    let mut down_runner = self
                        .operators_runners
                        .get(&down_op.get_id())
                        .unwrap()
                        .lock()
                        .await;

                    let (tx, rx) = link::<ZFMessage>(1024, link_id);

                    up_runner.add_output(tx);
                    down_runner.add_input(rx);
                }
            }
        }
    }

    pub fn get_runner(&self, operator_id: &ZFOperatorId) -> Arc<Mutex<Runner>> {
        self.operators_runners.get(operator_id).unwrap().clone()
    }

    pub fn get_runners(&self) -> Vec<Arc<Mutex<Runner>>> {
        let mut runners = vec![];

        for (_, runner) in &self.operators_runners {
            runners.push(runner.clone());
        }
        runners
    }
}

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

pub mod link;
pub mod node;

use async_std::sync::{Arc, Mutex};
use node::DataFlowNode;
use petgraph::dot::{Config, Dot};
use petgraph::graph::{EdgeIndex, NodeIndex};
use petgraph::stable_graph::StableGraph;
use petgraph::Direction;
use std::collections::HashMap;
use uhlc::HLC;
use zenoh::ZFuture;

use crate::model::dataflow::DataFlowRecord;
use crate::runtime::connectors::{ZFZenohReceiver, ZFZenohSender};
use crate::runtime::loader::{load_operator, load_sink, load_source};
use crate::runtime::message::ZFMessage;
use crate::runtime::runner::{Runner, ZFOperatorRunner, ZFSinkRunner, ZFSourceRunner};
use crate::{
    model::connector::ZFConnectorKind,
    model::link::{ZFFromEndpoint, ZFLinkDescriptor, ZFToEndpoint},
    model::operator::{ZFOperatorRecord, ZFSinkRecord, ZFSourceRecord},
    runtime::graph::link::link,
    runtime::graph::node::DataFlowNodeKind,
    types::{ZFError, ZFLinkId, ZFOperatorId, ZFOperatorName, ZFResult},
};
use uuid::Uuid;

pub struct DataFlowGraph {
    pub uuid: Uuid,
    pub flow: String,
    pub operators: Vec<(NodeIndex, DataFlowNode)>,
    pub links: Vec<(EdgeIndex, ZFLinkDescriptor)>,
    pub graph: StableGraph<DataFlowNode, ZFLinkId>,
    pub operators_runners: HashMap<ZFOperatorName, (Arc<Mutex<Runner>>, DataFlowNodeKind)>,
}

impl DataFlowGraph {
    pub fn new() -> Self {
        Self {
            uuid: Uuid::nil(),
            flow: "".to_string(),
            operators: Vec::new(),
            links: Vec::new(),
            graph: StableGraph::<DataFlowNode, ZFLinkId>::new(),
            operators_runners: HashMap::new(),
        }
    }

    pub fn from_dataflow_record(dr: DataFlowRecord) -> ZFResult<Self> {
        let mut graph = StableGraph::<DataFlowNode, ZFLinkId>::new();
        let mut operators = Vec::new();
        let mut links: Vec<(EdgeIndex, ZFLinkDescriptor)> = Vec::new();
        for o in dr.operators {
            operators.push((
                graph.add_node(DataFlowNode::Operator(o.clone())),
                DataFlowNode::Operator(o),
            ));
        }

        for o in dr.sources {
            operators.push((
                graph.add_node(DataFlowNode::Source(o.clone())),
                DataFlowNode::Source(o),
            ));
        }

        for o in dr.sinks {
            operators.push((
                graph.add_node(DataFlowNode::Sink(o.clone())),
                DataFlowNode::Sink(o),
            ));
        }

        for o in dr.connectors {
            operators.push((
                graph.add_node(DataFlowNode::Connector(o.clone())),
                DataFlowNode::Connector(o),
            ));
        }

        for l in dr.links {
            // First check if the LinkId are the same
            if l.from.output != l.to.input {
                return Err(ZFError::PortIdNotMatching((
                    l.from.output.clone(),
                    l.to.input,
                )));
            }

            let (from_index, from_runtime) =
                match operators.iter().find(|&(_, o)| o.get_id() == l.from.id) {
                    Some((idx, op)) => match op.has_output(l.from.output.clone()) {
                        true => (idx, op.get_runtime()),
                        false => return Err(ZFError::PortNotFound((l.from.id, l.from.output))),
                    },
                    None => return Err(ZFError::OperatorNotFound(l.from.id)),
                };

            let (to_index, to_runtime) =
                match operators.iter().find(|&(_, o)| o.get_id() == l.to.id) {
                    Some((idx, op)) => match op.has_input(l.to.input.clone()) {
                        true => (idx, op.get_runtime()),
                        false => return Err(ZFError::PortNotFound((l.to.id, l.to.input))),
                    },
                    None => return Err(ZFError::OperatorNotFound(l.to.id)),
                };

            if from_runtime == to_runtime {
                log::debug!("[Graph instantiation] [same runtime] Pushing link: {:?}", l);
                links.push((
                    graph.add_edge(*from_index, *to_index, l.from.output.clone()),
                    l.clone(),
                ));
            } else {
                log::debug!(
                    "[Graph instantiation] Link on different runtime detected: {:?}, this should not happen! :P",
                    l
                );

                // We do nothing in this case... the links are already well created when creating the record, so this should NEVER happen
            }
        }

        Ok(Self {
            uuid: dr.uuid,
            flow: dr.flow,
            operators,
            links,
            graph,
            operators_runners: HashMap::new(),
        })
    }

    pub fn set_name(&mut self, name: String) {
        self.flow = name;
    }

    pub fn to_dot_notation(&self) -> String {
        format!("{}", Dot::with_config(&self.graph, &[Config::EdgeNoLabel]))
    }

    pub fn add_static_operator(
        &mut self,
        hlc: Arc<HLC>,
        id: ZFOperatorId,
        inputs: Vec<ZFLinkId>,
        outputs: Vec<ZFLinkId>,
        operator: Box<dyn crate::OperatorTrait + Send>,
        configuration: Option<HashMap<String, String>>,
    ) -> ZFResult<()> {
        let descriptor = ZFOperatorRecord {
            id: id.clone(),
            inputs,
            outputs,
            uri: None,
            configuration,
            runtime: String::from("self"),
        };
        self.operators.push((
            self.graph
                .add_node(DataFlowNode::Operator(descriptor.clone())),
            DataFlowNode::Operator(descriptor),
        ));
        let runner = Runner::Operator(ZFOperatorRunner::new(hlc, operator, None));
        self.operators_runners.insert(
            id,
            (Arc::new(Mutex::new(runner)), DataFlowNodeKind::Operator),
        );
        Ok(())
    }

    pub fn add_static_source(
        &mut self,
        hlc: Arc<HLC>,
        id: ZFOperatorId,
        output: ZFLinkId,
        source: Box<dyn crate::SourceTrait + Send>,
        configuration: Option<HashMap<String, String>>,
    ) -> ZFResult<()> {
        let descriptor = ZFSourceRecord {
            id: id.clone(),
            output,
            uri: None,
            configuration,
            runtime: String::from("self"),
        };
        self.operators.push((
            self.graph
                .add_node(DataFlowNode::Source(descriptor.clone())),
            DataFlowNode::Source(descriptor),
        ));
        let runner = Runner::Source(ZFSourceRunner::new(hlc, source, None));
        self.operators_runners
            .insert(id, (Arc::new(Mutex::new(runner)), DataFlowNodeKind::Source));
        Ok(())
    }

    pub fn add_static_sink(
        &mut self,
        id: ZFOperatorId,
        input: ZFLinkId,
        sink: Box<dyn crate::SinkTrait + Send>,
        configuration: Option<HashMap<String, String>>,
    ) -> ZFResult<()> {
        let descriptor = ZFSinkRecord {
            id: id.clone(),
            input,
            uri: None,
            configuration,
            runtime: String::from("self"),
        };
        self.operators.push((
            self.graph.add_node(DataFlowNode::Sink(descriptor.clone())),
            DataFlowNode::Sink(descriptor),
        ));
        let runner = Runner::Sink(ZFSinkRunner::new(sink, None));
        self.operators_runners
            .insert(id, (Arc::new(Mutex::new(runner)), DataFlowNodeKind::Sink));
        Ok(())
    }

    pub fn add_link(
        &mut self,
        from: ZFFromEndpoint,
        to: ZFToEndpoint,
        size: Option<usize>,
        queueing_policy: Option<String>,
        priority: Option<usize>,
    ) -> ZFResult<()> {
        let connection = ZFLinkDescriptor {
            from,
            to,
            size,
            queueing_policy,
            priority,
        };

        if connection.from.output == connection.to.input {
            let from_index = match self
                .operators
                .iter()
                .find(|&(_, o)| o.get_id() == connection.from.id.clone())
            {
                Some((idx, op)) => match op.has_output(connection.from.output.clone()) {
                    true => idx,
                    false => {
                        return Err(ZFError::PortNotFound((
                            connection.from.id.clone(),
                            connection.from.output.clone(),
                        )))
                    }
                },
                None => return Err(ZFError::OperatorNotFound(connection.from.id.clone())),
            };

            let to_index = match self
                .operators
                .iter()
                .find(|&(_, o)| o.get_id() == connection.to.id.clone())
            {
                Some((idx, op)) => match op.has_input(connection.to.input.clone()) {
                    true => idx,
                    false => {
                        return Err(ZFError::PortNotFound((
                            connection.to.id.clone(),
                            connection.to.input.clone(),
                        )))
                    }
                },
                None => return Err(ZFError::OperatorNotFound(connection.to.id.clone())),
            };

            self.links.push((
                self.graph
                    .add_edge(*from_index, *to_index, connection.from.output.clone()),
                connection,
            ));
        } else {
            return Err(ZFError::PortIdNotMatching((
                connection.from.output.clone(),
                connection.to.input,
            )));
        }

        Ok(())
    }

    pub fn load(&mut self, runtime: &str) -> ZFResult<()> {
        let session = Arc::new(zenoh::net::open(zenoh::net::config::peer()).wait()?);
        let hlc = Arc::new(uhlc::HLC::default());

        for (_, op) in &self.operators {
            if op.get_runtime() != runtime {
                continue;
            }

            match op {
                DataFlowNode::Operator(inner) => {
                    match &inner.uri {
                        Some(uri) => {
                            let runner = load_operator(
                                hlc.clone(),
                                uri.clone(),
                                inner.configuration.clone(),
                            )?;
                            let runner = Runner::Operator(runner);
                            self.operators_runners.insert(
                                inner.id.clone(),
                                (Arc::new(Mutex::new(runner)), DataFlowNodeKind::Operator),
                            );
                        }
                        None => {
                            // this is a static operator.
                        }
                    }
                }
                DataFlowNode::Source(inner) => {
                    match &inner.uri {
                        Some(uri) => {
                            let runner =
                                load_source(hlc.clone(), uri.clone(), inner.configuration.clone())?;
                            let runner = Runner::Source(runner);
                            self.operators_runners.insert(
                                inner.id.clone(),
                                (Arc::new(Mutex::new(runner)), DataFlowNodeKind::Source),
                            );
                        }
                        None => {
                            // static source
                        }
                    }
                }
                DataFlowNode::Sink(inner) => {
                    match &inner.uri {
                        Some(uri) => {
                            let runner = load_sink(uri.clone(), inner.configuration.clone())?;
                            let runner = Runner::Sink(runner);
                            self.operators_runners.insert(
                                inner.id.clone(),
                                (Arc::new(Mutex::new(runner)), DataFlowNodeKind::Sink),
                            );
                        }
                        None => {
                            //static sink
                        }
                    }
                }
                DataFlowNode::Connector(zc) => match zc.kind {
                    ZFConnectorKind::Sender => {
                        let runner = ZFZenohSender::new(session.clone(), zc.resource.clone(), None);
                        let runner = Runner::Sender(runner);
                        self.operators_runners.insert(
                            zc.id.clone(),
                            (Arc::new(Mutex::new(runner)), DataFlowNodeKind::Connector),
                        );
                    }

                    ZFConnectorKind::Receiver => {
                        let runner =
                            ZFZenohReceiver::new(session.clone(), zc.resource.clone(), None);
                        let runner = Runner::Receiver(runner);
                        self.operators_runners.insert(
                            zc.id.clone(),
                            (Arc::new(Mutex::new(runner)), DataFlowNodeKind::Connector),
                        );
                    }
                },
            }
        }
        Ok(())
    }

    pub async fn make_connections(&mut self, runtime: &str) -> ZFResult<()> {
        // Connects the operators via our FIFOs

        for (idx, up_op) in &self.operators {
            if up_op.get_runtime() != runtime {
                continue;
            }

            log::debug!("Creating links for:\n\t< {:?} > Operator: {:?}", idx, up_op);

            let (up_runner, _) = self
                .operators_runners
                .get(&up_op.get_id())
                .ok_or(ZFError::OperatorNotFound(up_op.get_id().clone()))?;
            let mut up_runner = up_runner.lock().await;

            if self.graph.contains_node(*idx) {
                let mut downstreams = self
                    .graph
                    .neighbors_directed(*idx, Direction::Outgoing)
                    .detach();
                while let Some((down_edge_index, down_node_index)) = downstreams.next(&self.graph) {
                    let (_, down_link) = self
                        .links
                        .iter()
                        .find(|&(edge_index, _)| *edge_index == down_edge_index)
                        .ok_or(ZFError::OperatorNotFound(up_op.get_id().clone()))?;
                    let link_id = down_link.from.output.clone();

                    let down_op = match self
                        .operators
                        .iter()
                        .find(|&(idx, _)| *idx == down_node_index)
                    {
                        Some((_, op)) => op,
                        None => panic!("To not found"),
                    };

                    let (down_runner, _) = self
                        .operators_runners
                        .get(&down_op.get_id())
                        .ok_or(ZFError::OperatorNotFound(down_op.get_id().clone()))?;
                    let mut down_runner = down_runner.lock().await;

                    log::debug!(
                        "\t Creating link between {:?} -> {:?}: {:?}",
                        idx,
                        down_node_index,
                        link_id
                    );
                    let (tx, rx) = link::<ZFMessage>(None, link_id);

                    up_runner.add_output(tx);
                    down_runner.add_input(rx);
                }
            }
        }
        Ok(())
    }

    pub fn get_runner(&self, operator_id: &ZFOperatorId) -> Option<Arc<Mutex<Runner>>> {
        self.operators_runners
            .get(operator_id)
            .map(|(r, _)| r.clone())
    }

    pub fn get_runners(&self) -> Vec<Arc<Mutex<Runner>>> {
        let mut runners = vec![];

        for (runner, _) in self.operators_runners.values() {
            runners.push(runner.clone());
        }
        runners
    }

    pub fn get_sources(&self) -> Vec<Arc<Mutex<Runner>>> {
        let mut runners = vec![];

        for (runner, kind) in self.operators_runners.values() {
            if let DataFlowNodeKind::Source = kind {
                runners.push(runner.clone());
            }
        }
        runners
    }

    pub fn get_sinks(&self) -> Vec<Arc<Mutex<Runner>>> {
        let mut runners = vec![];

        for (runner, kind) in self.operators_runners.values() {
            if let DataFlowNodeKind::Sink = kind {
                runners.push(runner.clone());
            }
        }
        runners
    }

    pub fn get_operators(&self) -> Vec<Arc<Mutex<Runner>>> {
        let mut runners = vec![];

        for (runner, kind) in self.operators_runners.values() {
            if let DataFlowNodeKind::Operator = kind {
                runners.push(runner.clone());
            }
        }
        runners
    }

    pub fn get_connectors(&self) -> Vec<Arc<Mutex<Runner>>> {
        let mut runners = vec![];

        for (runner, kind) in self.operators_runners.values() {
            if let DataFlowNodeKind::Connector = kind {
                runners.push(runner.clone());
            }
        }
        runners
    }
}

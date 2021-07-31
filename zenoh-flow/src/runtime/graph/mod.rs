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

use crate::runtime::connectors::{ZFZenohReceiver, ZFZenohSender};
use crate::runtime::loader::{load_operator, load_sink, load_source};
use crate::runtime::message::ZFMessage;
use crate::runtime::runner::{Runner, ZFOperatorRunner, ZFSinkRunner, ZFSourceRunner};
use crate::{
    model::connector::ZFConnectorKind,
    model::dataflow::DataFlowRecord,
    model::link::{ZFLinkDescriptor, ZFLinkFromDescriptor, ZFLinkToDescriptor, ZFPortDescriptor},
    model::operator::{ZFOperatorRecord, ZFSinkRecord, ZFSourceRecord},
    runtime::graph::link::link,
    runtime::graph::node::DataFlowNodeKind,
    types::{ZFError, ZFOperatorId, ZFOperatorName, ZFResult},
    utils::hlc::PeriodicHLC,
};
use uuid::Uuid;

pub struct DataFlowGraph {
    pub uuid: Uuid,
    pub flow: String,
    pub operators: Vec<(NodeIndex, DataFlowNode)>,
    pub links: Vec<(EdgeIndex, ZFLinkDescriptor)>,
    pub graph: StableGraph<DataFlowNode, (String, String)>,
    pub operators_runners: HashMap<ZFOperatorName, (Arc<Mutex<Runner>>, DataFlowNodeKind)>,
}

impl Default for DataFlowGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl DataFlowGraph {
    pub fn new() -> Self {
        Self {
            uuid: Uuid::nil(),
            flow: "".to_string(),
            operators: Vec::new(),
            links: Vec::new(),
            graph: StableGraph::<DataFlowNode, (String, String)>::new(),
            operators_runners: HashMap::new(),
        }
    }

    pub fn from_dataflow_record(dr: DataFlowRecord) -> ZFResult<Self> {
        let mut graph = StableGraph::<DataFlowNode, (String, String)>::new();
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
            // if l.from.output != l.to.input {
            //     return Err(ZFError::PortIdNotMatching((
            //         l.from.output.clone(),
            //         l.to.input,
            //     )));
            // }

            let (from_index, from_runtime, from_type) = match operators
                .iter()
                .find(|&(_, o)| o.get_id() == l.from.component_id)
            {
                Some((idx, op)) => match op.has_output(l.from.output_id.clone()) {
                    true => (
                        idx,
                        op.get_runtime(),
                        op.get_output_type(l.from.output_id.clone())?,
                    ),
                    false => {
                        return Err(ZFError::PortNotFound((
                            l.from.component_id,
                            l.from.output_id.clone(),
                        )))
                    }
                },
                None => return Err(ZFError::OperatorNotFound(l.from.component_id)),
            };

            let (to_index, to_runtime, to_type) = match operators
                .iter()
                .find(|&(_, o)| o.get_id() == l.to.component_id)
            {
                Some((idx, op)) => match op.has_input(l.to.input_id.clone()) {
                    true => (
                        idx,
                        op.get_runtime(),
                        op.get_input_type(l.to.input_id.clone())?,
                    ),
                    false => {
                        return Err(ZFError::PortNotFound((
                            l.to.component_id,
                            l.to.input_id.clone(),
                        )))
                    }
                },
                None => return Err(ZFError::OperatorNotFound(l.to.component_id)),
            };

            if to_type != from_type {
                return Err(ZFError::PortTypeNotMatching((
                    to_type.to_string(),
                    from_type.to_string(),
                )));
            }

            if from_runtime == to_runtime {
                log::debug!("[Graph instantiation] [same runtime] Pushing link: {:?}", l);
                links.push((
                    graph.add_edge(
                        *from_index,
                        *to_index,
                        (l.from.output_id.clone(), l.to.input_id.clone()),
                    ),
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
        format!(
            "{:?}",
            Dot::with_config(&self.graph, &[Config::EdgeNoLabel])
        )
    }

    pub fn add_static_operator(
        &mut self,
        hlc: Arc<HLC>,
        id: ZFOperatorId,
        inputs: Vec<ZFPortDescriptor>,
        outputs: Vec<ZFPortDescriptor>,
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
        output: ZFPortDescriptor,
        source: Box<dyn crate::SourceTrait + Send>,
        configuration: Option<HashMap<String, String>>,
    ) -> ZFResult<()> {
        let descriptor = ZFSourceRecord {
            id: id.clone(),
            output,
            period: None,
            uri: None,
            configuration,
            runtime: String::from("self"),
        };
        self.operators.push((
            self.graph
                .add_node(DataFlowNode::Source(descriptor.clone())),
            DataFlowNode::Source(descriptor),
        ));
        let non_periodic_hlc = PeriodicHLC::new(hlc, None);
        let runner = Runner::Source(ZFSourceRunner::new(non_periodic_hlc, source, None));
        self.operators_runners
            .insert(id, (Arc::new(Mutex::new(runner)), DataFlowNodeKind::Source));
        Ok(())
    }

    pub fn add_static_sink(
        &mut self,
        id: ZFOperatorId,
        input: ZFPortDescriptor,
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
        from: ZFLinkFromDescriptor,
        to: ZFLinkToDescriptor,
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

        let (from_index, from_type) = match self
            .operators
            .iter()
            .find(|&(_, o)| o.get_id() == connection.from.component_id.clone())
        {
            Some((idx, op)) => match op.has_output(connection.from.output_id.clone()) {
                true => (idx, op.get_output_type(connection.from.output_id.clone())?),
                false => {
                    return Err(ZFError::PortNotFound((
                        connection.from.component_id.clone(),
                        connection.from.output_id.clone(),
                    )))
                }
            },
            None => {
                return Err(ZFError::OperatorNotFound(
                    connection.from.component_id.clone(),
                ))
            }
        };

        let (to_index, to_type) = match self
            .operators
            .iter()
            .find(|&(_, o)| o.get_id() == connection.to.component_id.clone())
        {
            Some((idx, op)) => match op.has_input(connection.to.input_id.clone()) {
                true => (idx, op.get_input_type(connection.to.input_id.clone())?),
                false => {
                    return Err(ZFError::PortNotFound((
                        connection.to.component_id.clone(),
                        connection.to.input_id.clone(),
                    )))
                }
            },
            None => {
                return Err(ZFError::OperatorNotFound(
                    connection.to.component_id.clone(),
                ))
            }
        };

        if from_type == to_type {
            self.links.push((
                self.graph.add_edge(
                    *from_index,
                    *to_index,
                    (
                        connection.from.output_id.clone(),
                        connection.to.input_id.clone(),
                    ),
                ),
                connection,
            ));
            return Ok(());
        }

        Err(ZFError::PortTypeNotMatching((
            String::from(from_type),
            String::from(to_type),
        )))
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
                            let runner = load_source(
                                PeriodicHLC::new(hlc.clone(), inner.period.clone()),
                                uri.clone(),
                                inner.configuration.clone(),
                            )?;
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
                .ok_or_else(|| ZFError::OperatorNotFound(up_op.get_id()))?;
            let mut up_runner = up_runner.lock().await;

            if self.graph.contains_node(*idx) {
                let mut downstreams = self
                    .graph
                    .neighbors_directed(*idx, Direction::Outgoing)
                    .detach();
                while let Some((down_edge_index, down_node_index)) = downstreams.next(&self.graph) {
                    let (_link_index, _down_link) = self
                        .links
                        .iter()
                        .find(|&(edge_index, _)| *edge_index == down_edge_index)
                        .ok_or_else(|| ZFError::OperatorNotFound(up_op.get_id()))?;

                    let (link_id_from, link_id_to) = match self.graph.edge_weight(down_edge_index) {
                        Some(w) => w,
                        None => return Err(ZFError::GenericError),
                    };

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
                        .ok_or_else(|| ZFError::OperatorNotFound(down_op.get_id()))?;
                    let mut down_runner = down_runner.lock().await;

                    log::debug!(
                        "\t Creating link between {:?} -> {:?}: {:?} -> {:?}",
                        idx,
                        down_node_index,
                        link_id_from,
                        link_id_to,
                    );
                    let (tx, rx) = link::<ZFMessage>(
                        None,
                        String::from(link_id_from),
                        String::from(link_id_to),
                    );

                    up_runner.add_output(tx);
                    down_runner.add_input(rx);
                }
            }
        }
        Ok(())
    }

    pub fn get_runner(&self, operator_id: &str) -> Option<Arc<Mutex<Runner>>> {
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

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
use zenoh::ZFuture;

use crate::model::dataflow::DataFlowDescriptor;
use crate::runtime::connectors::{
    ZFZenohConnectorDescriptor, ZFZenohConnectorInfo, ZFZenohReceiver, ZFZenohSender,
};
use crate::runtime::loader::{load_operator, load_sink, load_source};
use crate::runtime::message::ZFMessage;
use crate::runtime::runner::{Runner, ZFOperatorRunner, ZFSinkRunner, ZFSourceRunner};
use crate::{
    model::link::{ZFFromEndpoint, ZFLinkDescriptor, ZFToEndpoint},
    model::operator::{ZFOperatorDescriptor, ZFSinkDescriptor, ZFSourceDescriptor},
    runtime::graph::link::link,
    types::{ZFError, ZFLinkId, ZFOperatorId, ZFOperatorName, ZFResult},
};

pub struct DataFlowGraph {
    pub flow: String,
    pub operators: Vec<(NodeIndex, DataFlowNode)>,
    pub links: Vec<(EdgeIndex, ZFLinkDescriptor)>,
    pub graph: StableGraph<DataFlowNode, ZFLinkId>,
    pub operators_runners: HashMap<ZFOperatorName, Arc<Mutex<Runner>>>,
}

pub fn deserialize_dataflow_description(data: String) -> DataFlowDescriptor {
    serde_yaml::from_str::<DataFlowDescriptor>(&data).unwrap()
}

impl DataFlowGraph {
    pub fn new(df: Option<DataFlowDescriptor>) -> Self {
        match df {
            Some(df) => {
                let mut graph = StableGraph::<DataFlowNode, ZFLinkId>::new();
                let mut operators = Vec::new();
                let mut links: Vec<(EdgeIndex, ZFLinkDescriptor)> = Vec::new();
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

                for l in df.links {
                    // First check if the LinkId are the same
                    if l.from.output != l.to.input {
                        panic!("Ports ID does not match");
                    }

                    let (from_index, from_runtime) =
                        match operators.iter().find(|&(_, o)| o.get_name() == l.from.name) {
                            Some((idx, op)) => match op.has_output(l.from.output.clone()) {
                                true => (idx, op.get_runtime()),
                                false => {
                                    panic!("Link id {:?} not found in {:?}", l.from.output, op)
                                }
                            },
                            None => panic!("Not not found"),
                        };

                    let (to_index, to_runtime) =
                        match operators.iter().find(|&(_, o)| o.get_name() == l.to.name) {
                            Some((idx, op)) => match op.has_input(l.to.input.clone()) {
                                true => (idx, op.get_runtime()),
                                false => panic!("Link id {:?} not found in {:?}", &l.to.input, op),
                            },
                            None => panic!("Not not found"),
                        };

                    if from_runtime == to_runtime {
                        log::debug!("[Graph instantiation] [same runtime] Pushing link: {:?}", l);
                        links.push((
                            graph.add_edge(*from_index, *to_index, l.from.output.clone()),
                            l.clone(),
                        ));
                    } else {
                        log::debug!(
                            "[Graph instantiation] Link on different runtime detected: {:?}",
                            l
                        );

                        // TODO Refactoring: put the following logic in a separate, dedicated
                        // function.
                        let resource = format!(
                            "/zf/{}/{}/{}",
                            df.flow,
                            l.from.name.clone(),
                            l.from.output.clone()
                        );
                        log::debug!("[Graph instantiation] Resource associated: {:?}", resource);

                        let sender_name = format!(
                            "{}-sender-{}-{}",
                            df.flow,
                            l.from.name.clone(),
                            l.from.output.clone()
                        );
                        let sender = ZFZenohConnectorInfo {
                            name: sender_name.clone(),
                            link_id: l.from.output.clone(),
                            resource: resource.clone(),
                            runtime: from_runtime.clone(),
                        };

                        let sender_idx = graph.add_node(DataFlowNode::Connector(
                            ZFZenohConnectorDescriptor::Sender(sender.clone()),
                        ));

                        let link_sender = ZFLinkDescriptor {
                            from: l.from.clone(),
                            to: ZFToEndpoint {
                                name: sender_name,
                                input: sender.link_id.clone(),
                            },
                            size: None,
                            queueing_policy: None,
                            priority: None,
                        };

                        log::debug!("Pushing link: {:?}", link_sender);
                        links.push((
                            graph.add_edge(
                                *from_index,
                                sender_idx,
                                link_sender.from.output.clone(),
                            ),
                            link_sender,
                        ));

                        let receiver_name = format!(
                            "{}-receiver-{}-{}",
                            df.flow,
                            l.to.name.clone(),
                            l.to.input.clone()
                        );
                        let receiver = ZFZenohConnectorInfo {
                            name: receiver_name.clone(),
                            link_id: l.to.input.clone(),
                            resource,
                            runtime: to_runtime.clone(),
                        };

                        let recv_idx = graph.add_node(DataFlowNode::Connector(
                            ZFZenohConnectorDescriptor::Receiver(receiver.clone()),
                        ));
                        let link_receiver = ZFLinkDescriptor {
                            from: ZFFromEndpoint {
                                name: receiver_name,
                                output: receiver.link_id.clone(),
                            },
                            to: l.to.clone(),
                            size: None,
                            queueing_policy: None,
                            priority: None,
                        };
                        links.push((
                            graph.add_edge(recv_idx, *to_index, link_receiver.to.input.clone()),
                            link_receiver,
                        ));

                        operators.push((
                            sender_idx,
                            DataFlowNode::Connector(ZFZenohConnectorDescriptor::Sender(sender)),
                        ));
                        operators.push((
                            recv_idx,
                            DataFlowNode::Connector(ZFZenohConnectorDescriptor::Receiver(receiver)),
                        ));
                    }
                }

                Self {
                    flow: df.flow,
                    operators,
                    links,
                    graph,
                    operators_runners: HashMap::new(),
                }
            }
            None => Self {
                flow: "".to_string(),
                operators: Vec::new(),
                links: Vec::new(),
                graph: StableGraph::<DataFlowNode, ZFLinkId>::new(),
                operators_runners: HashMap::new(),
            },
        }
    }

    pub fn set_name(&mut self, name: String) {
        self.flow = name;
    }

    pub fn to_dot_notation(&self) -> String {
        String::from(format!(
            "{}",
            Dot::with_config(&self.graph, &[Config::EdgeNoLabel])
        ))
    }

    pub fn add_static_operator(
        &mut self,
        id: ZFOperatorId,
        name: String,
        inputs: Vec<ZFLinkId>,
        outputs: Vec<ZFLinkId>,
        operator: Box<dyn crate::operator::OperatorTrait + Send>,
        configuration: Option<HashMap<String, String>>,
    ) -> ZFResult<()> {
        let descriptor = ZFOperatorDescriptor {
            id: id,
            name: name.clone(),
            inputs,
            outputs,
            lib: None,
            configuration,
            runtime: None,
        };
        self.operators.push((
            self.graph
                .add_node(DataFlowNode::Operator(descriptor.clone())),
            DataFlowNode::Operator(descriptor),
        ));
        let runner = Runner::Operator(ZFOperatorRunner::new(operator, None));
        self.operators_runners
            .insert(name, Arc::new(Mutex::new(runner)));
        Ok(())
    }

    pub fn add_static_source(
        &mut self,
        id: ZFOperatorId,
        name: String,
        output: ZFLinkId,
        source: Box<dyn crate::operator::SourceTrait + Send>,
        configuration: Option<HashMap<String, String>>,
    ) -> ZFResult<()> {
        let descriptor = ZFSourceDescriptor {
            id: id,
            name: name.clone(),
            output,
            lib: None,
            configuration,
            runtime: None,
        };
        self.operators.push((
            self.graph
                .add_node(DataFlowNode::Source(descriptor.clone())),
            DataFlowNode::Source(descriptor),
        ));
        let runner = Runner::Source(ZFSourceRunner::new(source, None));
        self.operators_runners
            .insert(name, Arc::new(Mutex::new(runner)));
        Ok(())
    }

    pub fn add_static_sink(
        &mut self,
        id: ZFOperatorId,
        name: String,
        input: ZFLinkId,
        sink: Box<dyn crate::operator::SinkTrait + Send>,
        configuration: Option<HashMap<String, String>>,
    ) -> ZFResult<()> {
        let descriptor = ZFSinkDescriptor {
            id: id,
            name: name.clone(),
            input,
            lib: None,
            configuration,
            runtime: None,
        };
        self.operators.push((
            self.graph.add_node(DataFlowNode::Sink(descriptor.clone())),
            DataFlowNode::Sink(descriptor),
        ));
        let runner = Runner::Sink(ZFSinkRunner::new(sink, None));
        self.operators_runners
            .insert(name, Arc::new(Mutex::new(runner)));
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
                .find(|&(_, o)| o.get_name() == connection.from.name.clone())
            {
                Some((idx, op)) => match op.has_output(connection.from.output.clone()) {
                    true => idx,
                    false => {
                        return Err(ZFError::PortNotFound((
                            connection.from.name.clone(),
                            connection.from.output.clone(),
                        )))
                    }
                },
                None => return Err(ZFError::OperatorNotFound(connection.from.name.clone())),
            };

            let to_index = match self
                .operators
                .iter()
                .find(|&(_, o)| o.get_name() == connection.to.name.clone())
            {
                Some((idx, op)) => match op.has_input(connection.to.input.clone()) {
                    true => idx,
                    false => {
                        return Err(ZFError::PortNotFound((
                            connection.to.name.clone(),
                            connection.to.input.clone(),
                        )))
                    }
                },
                None => return Err(ZFError::OperatorNotFound(connection.to.name.clone())),
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
        unsafe {
            let session = Arc::new(zenoh::net::open(zenoh::net::config::peer()).wait().unwrap());
            for (_, op) in &self.operators {
                if let Some(op_runtime) = op.get_runtime() {
                    if op_runtime != runtime {
                        continue;
                    }
                }

                match op {
                    DataFlowNode::Operator(inner) => {
                        match &inner.lib {
                            Some(lib) => {
                                let runner =
                                    load_operator(lib.clone(), inner.configuration.clone())?;
                                let runner = Runner::Operator(runner);
                                self.operators_runners
                                    .insert(inner.name.clone(), Arc::new(Mutex::new(runner)));
                            }
                            None => {
                                // this is a static operator.
                                ()
                            }
                        }
                    }
                    DataFlowNode::Source(inner) => {
                        match &inner.lib {
                            Some(lib) => {
                                let runner = load_source(lib.clone(), inner.configuration.clone())?;
                                let runner = Runner::Source(runner);
                                self.operators_runners
                                    .insert(inner.name.clone(), Arc::new(Mutex::new(runner)));
                            }
                            None => {
                                // static source
                                ()
                            }
                        }
                    }
                    DataFlowNode::Sink(inner) => {
                        match &inner.lib {
                            Some(lib) => {
                                let runner = load_sink(lib.clone(), inner.configuration.clone())?;
                                let runner = Runner::Sink(runner);
                                self.operators_runners
                                    .insert(inner.name.clone(), Arc::new(Mutex::new(runner)));
                            }
                            None => {
                                //static sink
                                ()
                            }
                        }
                    }
                    DataFlowNode::Connector(zc) => match zc {
                        ZFZenohConnectorDescriptor::Sender(tx) => {
                            let runner =
                                ZFZenohSender::new(session.clone(), tx.resource.clone(), None);
                            let runner = Runner::Sender(runner);
                            self.operators_runners
                                .insert(tx.name.clone(), Arc::new(Mutex::new(runner)));
                        }

                        ZFZenohConnectorDescriptor::Receiver(rx) => {
                            let runner =
                                ZFZenohReceiver::new(session.clone(), rx.resource.clone(), None);
                            let runner = Runner::Receiver(runner);
                            self.operators_runners
                                .insert(rx.name.clone(), Arc::new(Mutex::new(runner)));
                        }
                    },
                }
            }
            Ok(())
        }
    }

    pub async fn make_connections(&mut self, runtime: &str) {
        // Connects the operators via our FIFOs

        for (idx, up_op) in &self.operators {
            if let Some(op_runtime) = up_op.get_runtime() {
                if op_runtime != runtime {
                    continue;
                }
            }

            log::debug!("Creating links for:\n\t< {:?} > Operator: {:?}", idx, up_op);

            let mut up_runner = self
                .operators_runners
                .get(&up_op.get_name())
                .unwrap()
                .lock()
                .await;

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
                        .unwrap();
                    let link_id = down_link.from.output.clone();

                    let down_op = match self
                        .operators
                        .iter()
                        .find(|&(idx, _)| *idx == down_node_index)
                    {
                        Some((_, op)) => op,
                        None => panic!("To not found"),
                    };

                    let mut down_runner = self
                        .operators_runners
                        .get(&down_op.get_name())
                        .unwrap()
                        .lock()
                        .await;

                    log::debug!(
                        "\t Creating link between {:?} -> {:?}: {:?}",
                        idx,
                        down_node_index,
                        link_id
                    );
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
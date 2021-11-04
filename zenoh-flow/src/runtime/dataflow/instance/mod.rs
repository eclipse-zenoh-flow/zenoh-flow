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
pub mod runners;

use async_std::sync::Arc;
use std::collections::HashMap;
use uuid::Uuid;

use crate::model::connector::ZFConnectorKind;
use crate::model::link::LinkDescriptor;
use crate::runtime::dataflow::instance::link::link;
use crate::runtime::dataflow::instance::runners::connector::{ZenohReceiver, ZenohSender};
use crate::runtime::dataflow::instance::runners::operator::{OperatorIO, OperatorRunner};
use crate::runtime::dataflow::instance::runners::sink::SinkRunner;
use crate::runtime::dataflow::instance::runners::source::SourceRunner;
use crate::runtime::dataflow::instance::runners::{NodeRunner, RunnerKind};
use crate::runtime::dataflow::Dataflow;
use crate::runtime::RuntimeContext;
use crate::{FlowId, Message, NodeId, ZFError, ZFResult};

pub struct DataflowInstance {
    pub(crate) uuid: Uuid,
    pub(crate) flow_id: FlowId,
    pub(crate) context: RuntimeContext,
    pub(crate) runners: HashMap<NodeId, NodeRunner>,
}

fn create_links(
    nodes: &[NodeId],
    links: &[LinkDescriptor],
) -> ZFResult<HashMap<NodeId, OperatorIO>> {
    let mut io: HashMap<NodeId, OperatorIO> = HashMap::with_capacity(nodes.len());

    for link_desc in links {
        let upstream_node = link_desc.from.node.clone();
        let downstream_node = link_desc.to.node.clone();

        // Nodes have been filtered based on their runtime. If the runtime of either one of the node
        // is not equal to that of the current runtime, the channels should not be created.
        if !nodes.contains(&upstream_node) || !nodes.contains(&downstream_node) {
            continue;
        }

        let (tx, rx) = link::<Message>(
            None,
            link_desc.from.output.clone(),
            link_desc.to.input.clone(),
        );

        match io.get_mut(&upstream_node) {
            Some(operator_io) => operator_io.add_output(tx),
            None => {
                let mut operator_io = OperatorIO::default();
                operator_io.add_output(tx);
                io.insert(upstream_node, operator_io);
            }
        }

        match io.get_mut(&downstream_node) {
            Some(operator_io) => operator_io.try_add_input(rx)?,
            None => {
                let mut operator_io = OperatorIO::default();
                operator_io.try_add_input(rx)?;
                io.insert(downstream_node, operator_io);
            }
        }
    }

    Ok(io)
}

impl DataflowInstance {
    pub fn try_instantiate(dataflow: Dataflow) -> ZFResult<Self> {
        // Gather all node ids to be able to generate (i) the links and (ii) the hash map containing
        // the runners.
        let mut node_ids: Vec<NodeId> = Vec::with_capacity(
            dataflow.sources.len()
                + dataflow.operators.len()
                + dataflow.sinks.len()
                + dataflow.connectors.len(),
        );

        node_ids.append(&mut dataflow.sources.keys().cloned().collect::<Vec<_>>());
        node_ids.append(&mut dataflow.operators.keys().cloned().collect::<Vec<_>>());
        node_ids.append(&mut dataflow.sinks.keys().cloned().collect::<Vec<_>>());
        node_ids.append(&mut dataflow.connectors.keys().cloned().collect::<Vec<_>>());

        let mut links = create_links(&node_ids, &dataflow.links)?;

        // The links were created, we can generate the Runners.
        let mut runners: HashMap<NodeId, NodeRunner> = HashMap::with_capacity(node_ids.len());

        for (id, source) in dataflow.sources.into_iter() {
            let io = links.remove(&id).ok_or_else(|| {
                ZFError::IOError(format!(
                    "Links for Source < {} > were not created.",
                    &source.id
                ))
            })?;
            runners.insert(
                id,
                NodeRunner::new(Arc::new(SourceRunner::try_new(
                    dataflow.context.clone(),
                    source,
                    io,
                )?)),
            );
        }

        for (id, operator) in dataflow.operators.into_iter() {
            let io = links.remove(&operator.id).ok_or_else(|| {
                ZFError::IOError(format!(
                    "Links for Operator < {} > were not created.",
                    &operator.id
                ))
            })?;
            runners.insert(
                id,
                NodeRunner::new(Arc::new(OperatorRunner::try_new(
                    dataflow.context.clone(),
                    operator,
                    io,
                )?)),
            );
        }

        for (id, sink) in dataflow.sinks.into_iter() {
            let io = links.remove(&id).ok_or_else(|| {
                ZFError::IOError(format!("Links for Sink < {} > were not created.", &sink.id))
            })?;
            runners.insert(
                id,
                NodeRunner::new(Arc::new(SinkRunner::try_new(
                    dataflow.context.clone(),
                    sink,
                    io,
                )?)),
            );
        }

        for (id, connector) in dataflow.connectors.into_iter() {
            let io = links.remove(&id).ok_or_else(|| {
                ZFError::IOError(format!(
                    "Links for Connector < {} > were not created.",
                    &connector.id
                ))
            })?;
            match connector.kind {
                ZFConnectorKind::Sender => {
                    runners.insert(
                        id,
                        NodeRunner::new(Arc::new(ZenohSender::try_new(
                            dataflow.context.clone(),
                            connector,
                            io,
                        )?)),
                    );
                }
                ZFConnectorKind::Receiver => {
                    runners.insert(
                        id,
                        NodeRunner::new(Arc::new(ZenohReceiver::try_new(
                            dataflow.context.clone(),
                            connector,
                            io,
                        )?)),
                    );
                }
            }
        }

        Ok(Self {
            uuid: dataflow.uuid,
            flow_id: dataflow.flow_id,
            context: dataflow.context,
            runners,
        })
    }

    pub fn get_uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn get_flow(&self) -> Arc<str> {
        self.flow_id.clone()
    }

    pub fn get_runtime_context(&self) -> RuntimeContext {
        self.context.clone()
    }

    pub fn get_runner(&self, operator_id: &NodeId) -> Option<NodeRunner> {
        self.runners.get(operator_id).cloned()
    }

    pub fn get_runners(&self) -> Vec<NodeRunner> {
        self.runners.values().cloned().collect()
    }

    pub fn get_sources(&self) -> Vec<NodeRunner> {
        self.runners
            .values()
            .filter(|runner| matches!(runner.get_kind(), RunnerKind::Source))
            .cloned()
            .collect()
    }

    pub fn get_sinks(&self) -> Vec<NodeRunner> {
        self.runners
            .values()
            .filter(|runner| matches!(runner.get_kind(), RunnerKind::Sink))
            .cloned()
            .collect()
    }

    pub fn get_operators(&self) -> Vec<NodeRunner> {
        self.runners
            .values()
            .filter(|runner| matches!(runner.get_kind(), RunnerKind::Operator))
            .cloned()
            .collect()
    }

    pub fn get_connectors(&self) -> Vec<NodeRunner> {
        self.runners
            .values()
            .filter(|runner| matches!(runner.get_kind(), RunnerKind::Connector))
            .cloned()
            .collect()
    }
}

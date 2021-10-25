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

use async_std::sync::Arc;
use std::collections::HashMap;

use crate::model::connector::ZFConnectorRecord;
use crate::model::node::{OperatorRecord, SinkRecord, SourceRecord};
use crate::runtime::message::Message;
use crate::runtime::runners::connector::{ZenohReceiver, ZenohSender};
use crate::runtime::runners::{
    operator::OperatorRunner, sink::SinkRunner, source::SourceRunner, Runner,
};
use crate::{
    model::connector::ZFConnectorKind,
    model::dataflow::DataFlowRecord,
    model::link::LinkDescriptor,
    runtime::graph::link::link,
    runtime::RuntimeContext,
    types::{NodeId, ZFResult},
};
use uuid::Uuid;

use super::runners::operator::OperatorIO;

pub struct DataFlowGraph {
    pub uuid: Uuid,
    flow: Arc<str>,
    pub context: RuntimeContext,
    runners: HashMap<NodeId, Runner>,
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

impl DataFlowGraph {
    pub fn try_new(context: RuntimeContext, dataflow_record: DataFlowRecord) -> ZFResult<Self> {
        // Filter all the nodes to only keep the ones that are assigned to the current runtime.
        let sources: Vec<SourceRecord> = dataflow_record
            .sources
            .into_iter()
            .filter(|source| source.runtime == context.runtime_name)
            .collect();

        let operators: Vec<OperatorRecord> = dataflow_record
            .operators
            .into_iter()
            .filter(|operator| operator.runtime == context.runtime_name)
            .collect();

        let sinks: Vec<SinkRecord> = dataflow_record
            .sinks
            .into_iter()
            .filter(|sink| sink.runtime == context.runtime_name)
            .collect();

        let connectors: Vec<ZFConnectorRecord> = dataflow_record
            .connectors
            .into_iter()
            .filter(|connector| connector.runtime == context.runtime_name)
            .collect();

        // Gather all the ids of the nodes assigned to the current runtime to only create the links
        // needed on that runtime.
        let mut nodes: Vec<NodeId> =
            Vec::with_capacity(sources.len() + operators.len() + sinks.len() + connectors.len());
        nodes.extend(
            sources
                .iter()
                .map(|source| source.id.clone())
                .collect::<Vec<_>>(),
        );
        nodes.extend(
            operators
                .iter()
                .map(|operator| operator.id.clone())
                .collect::<Vec<_>>(),
        );
        nodes.extend(sinks.iter().map(|sink| sink.id.clone()).collect::<Vec<_>>());
        nodes.extend(
            connectors
                .iter()
                .map(|connector| connector.id.clone())
                .collect::<Vec<_>>(),
        );

        // Create all the necessary links.
        let mut links = create_links(&nodes, &dataflow_record.links)?;

        // Create all the runners.
        let mut runners = HashMap::with_capacity(nodes.len());

        for source in sources.into_iter() {
            let io = links.remove(&source.id);
            runners.insert(
                source.id.clone(),
                Runner::Source(SourceRunner::try_new(&context, source, io)?),
            );
        }

        for operator in operators.into_iter() {
            let io = links.remove(&operator.id);
            runners.insert(
                operator.id.clone(),
                Runner::Operator(OperatorRunner::try_new(&context, operator, io)?),
            );
        }

        for sink in sinks.into_iter() {
            let io = links.remove(&sink.id);
            runners.insert(
                sink.id.clone(),
                Runner::Sink(SinkRunner::try_new(&context, sink, io)?),
            );
        }

        for connector in connectors.into_iter() {
            let io = links.remove(&connector.id);
            match connector.kind {
                ZFConnectorKind::Sender => {
                    runners.insert(
                        connector.id.clone(),
                        Runner::Sender(ZenohSender::try_new(&context, connector, io)?),
                    );
                }
                ZFConnectorKind::Receiver => {
                    runners.insert(
                        connector.id.clone(),
                        Runner::Receiver(ZenohReceiver::try_new(&context, connector, io)?),
                    );
                }
            }
        }

        Ok(Self {
            uuid: Uuid::new_v4(),
            flow: dataflow_record.flow.into(),
            context,
            runners,
        })
    }

    pub fn set_name(&mut self, name: String) {
        self.flow = name.into();
    }

    // pub fn add_static_operator(
    //     &mut self,
    //     id: NodeId,
    //     inputs: Vec<PortDescriptor>,
    //     outputs: Vec<PortDescriptor>,
    //     operator: Arc<dyn Operator>,
    //     configuration: Option<HashMap<String, String>>,
    // ) -> ZFResult<()> {
    //     let record = OperatorRecord {
    //         id: id.clone(),
    //         inputs,
    //         outputs,
    //         uri: None,
    //         configuration,
    //         runtime: self.ctx.runtime_name.clone(),
    //     };
    //     let dfn = DataFlowNode::Operator(record.clone());
    //     self.operators.insert(dfn.get_id(), dfn);
    //     let runner = Runner::Operator(OperatorRunner::new(
    //         record,
    //         self.ctx.hlc.clone(),
    //         operator,
    //         None,
    //     ));
    //     self.operators_runners
    //         .insert(id, (runner, DataFlowNodeKind::Operator));
    //     Ok(())
    // }

    // pub fn add_static_source(
    //     &mut self,
    //     id: NodeId,
    //     output: PortDescriptor,
    //     source: Arc<dyn Source>,
    //     configuration: Option<HashMap<String, String>>,
    // ) -> ZFResult<()> {
    //     let record = SourceRecord {
    //         id: id.clone(),
    //         output,
    //         period: None,
    //         uri: None,
    //         configuration,
    //         runtime: self.ctx.runtime_name.clone(),
    //     };
    //     let dfn = DataFlowNode::Source(record.clone());
    //     self.operators.insert(dfn.get_id(), dfn);
    //     let non_periodic_hlc = PeriodicHLC::new(self.ctx.hlc.clone(), None);
    //     let runner = Runner::Source(SourceRunner::new(record, non_periodic_hlc, source, None));
    //     self.operators_runners
    //         .insert(id, (runner, DataFlowNodeKind::Source));
    //     Ok(())
    // }

    // pub fn add_static_sink(
    //     &mut self,
    //     id: NodeId,
    //     input: PortDescriptor,
    //     sink: Arc<dyn Sink>,
    //     configuration: Option<HashMap<String, String>>,
    // ) -> ZFResult<()> {
    //     let record = SinkRecord {
    //         id: id.clone(),
    //         input,
    //         uri: None,
    //         configuration,
    //         runtime: self.ctx.runtime_name.clone(),
    //     };
    //     let dfn = DataFlowNode::Sink(record.clone());
    //     self.operators.insert(dfn.get_id(), dfn);
    //     let runner = Runner::Sink(SinkRunner::new(record, sink, None));
    //     self.operators_runners
    //         .insert(id, (runner, DataFlowNodeKind::Sink));
    //     Ok(())
    // }

    // pub fn add_link(
    //     &mut self,
    //     from: LinkFromDescriptor,
    //     to: LinkToDescriptor,
    //     size: Option<usize>,
    //     queueing_policy: Option<String>,
    //     priority: Option<usize>,
    // ) -> ZFResult<()> {
    //     let connection = LinkDescriptor {
    //         from,
    //         to,
    //         size,
    //         queueing_policy,
    //         priority,
    //     };

    //     let from_type = match self.operators.get(&connection.from.node) {
    //         Some(op) => match op.has_output(connection.from.output.clone()) {
    //             true => op.get_output_type(connection.from.output.clone())?,
    //             false => {
    //                 return Err(ZFError::PortNotFound((
    //                     connection.from.node.clone(),
    //                     connection.from.output.clone(),
    //                 )))
    //             }
    //         },
    //         None => return Err(ZFError::OperatorNotFound(connection.from.node.clone())),
    //     };

    //     let to_type = match self.operators.get(&connection.to.node) {
    //         Some(op) => match op.has_input(connection.to.input.clone()) {
    //             true => op.get_input_type(connection.to.input.clone())?,
    //             false => {
    //                 return Err(ZFError::PortNotFound((
    //                     connection.to.node.clone(),
    //                     connection.to.input.clone(),
    //                 )))
    //             }
    //         },
    //         None => return Err(ZFError::OperatorNotFound(connection.to.node.clone())),
    //     };

    //     if from_type == to_type {
    //         self.links.push(connection);
    //         return Ok(());
    //     }

    //     Err(ZFError::PortTypeNotMatching((
    //         String::from(from_type),
    //         String::from(to_type),
    //     )))
    // }

    pub fn get_runner(&self, operator_id: &NodeId) -> Option<&Runner> {
        self.runners.get(operator_id)
    }

    pub fn get_runners(&self) -> Vec<&Runner> {
        self.runners.values().collect()
    }

    pub fn get_sources(&self) -> Vec<&Runner> {
        self.runners
            .values()
            .filter(|runner| matches!(runner, Runner::Source(_)))
            .collect()
    }

    pub fn get_sinks(&self) -> Vec<&Runner> {
        self.runners
            .values()
            .filter(|runner| matches!(runner, Runner::Sink(_)))
            .collect()
    }

    pub fn get_operators(&self) -> Vec<&Runner> {
        self.runners
            .values()
            .filter(|runner| matches!(runner, Runner::Operator(_)))
            .collect()
    }

    pub fn get_connectors(&self) -> Vec<&Runner> {
        self.runners
            .values()
            .filter(|runner| matches!(runner, Runner::Receiver(_) | Runner::Sender(_)))
            .collect()
    }
}

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

pub mod instance;
pub mod loader;
pub mod node;

use async_std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::convert::TryFrom;

use self::node::{OperatorLoaded, SinkLoaded, SourceLoaded};
use crate::model::connector::ZFConnectorRecord;
use crate::model::link::{LinkFromDescriptor, LinkToDescriptor, PortDescriptor};
use crate::model::period::PeriodDescriptor;
use crate::{
    model::dataflow::DataFlowRecord, model::link::LinkDescriptor, runtime::RuntimeContext,
    types::ZFResult,
};
use crate::{NodeId, Operator, PortId, Sink, Source, State, ZFError};

pub struct Dataflow {
    pub(crate) flow: Arc<str>,
    pub(crate) context: RuntimeContext,
    pub(crate) sources: HashMap<NodeId, SourceLoaded>,
    pub(crate) operators: HashMap<NodeId, OperatorLoaded>,
    pub(crate) sinks: HashMap<NodeId, SinkLoaded>,
    pub(crate) connectors: HashMap<NodeId, ZFConnectorRecord>,
    pub(crate) links: Vec<LinkDescriptor>,
}

impl Dataflow {
    /// Creates a new (empty) Dataflow.
    ///
    /// This function should be called when creating a *static* Dataflow. If you intend on
    /// dynamically loading your nodes, use `try_new` instead.
    ///
    /// After adding the nodes (through `add_static_source`, `add_static_sink` and
    /// `add_static_operator`) you can instantiate your Dataflow by creating a `DataflowInstance`.
    pub fn new(context: RuntimeContext, flow: &str) -> Self {
        Self {
            flow: flow.into(),
            context,
            sources: HashMap::default(),
            operators: HashMap::default(),
            sinks: HashMap::default(),
            connectors: HashMap::default(),
            links: Vec::default(),
        }
    }

    pub fn try_new(context: RuntimeContext, record: DataFlowRecord) -> ZFResult<Self> {
        let res_sources: ZFResult<Vec<SourceLoaded>> = record
            .sources
            .into_iter()
            .filter(|source| source.runtime == context.runtime_name)
            .map(SourceLoaded::try_from)
            .collect();
        let sources: HashMap<_, _> = res_sources?
            .into_iter()
            .map(|source| (source.id.clone(), source))
            .collect();

        let res_operators: ZFResult<Vec<OperatorLoaded>> = record
            .operators
            .into_iter()
            .filter(|operator| operator.runtime == context.runtime_name)
            .map(OperatorLoaded::try_from)
            .collect();
        let operators: HashMap<_, _> = res_operators?
            .into_iter()
            .map(|operator| (operator.id.clone(), operator))
            .collect();

        let res_sinks: ZFResult<Vec<SinkLoaded>> = record
            .sinks
            .into_iter()
            .filter(|sink| sink.runtime == context.runtime_name)
            .map(SinkLoaded::try_from)
            .collect();
        let sinks: HashMap<_, _> = res_sinks?
            .into_iter()
            .map(|sink| (sink.id.clone(), sink))
            .collect();

        let connectors: HashMap<_, _> = record
            .connectors
            .into_iter()
            .filter(|connector| connector.runtime == context.runtime_name)
            .map(|connector| (connector.id.clone(), connector))
            .collect();

        Ok(Self {
            flow: record.flow.into(),
            context,
            sources,
            operators,
            sinks,
            connectors,
            links: record.links,
        })
    }

    pub fn add_static_source(
        &mut self,
        id: NodeId,
        period: Option<PeriodDescriptor>,
        output: PortDescriptor,
        state: State,
        source: Arc<dyn Source>,
    ) {
        self.sources.insert(
            id.clone(),
            SourceLoaded {
                id,
                output,
                period,
                state: Arc::new(RwLock::new(state)),
                source,
                library: None,
            },
        );
    }

    pub fn add_static_operator(
        &mut self,
        id: NodeId,
        inputs: Vec<PortDescriptor>,
        outputs: Vec<PortDescriptor>,
        state: State,
        operator: Arc<dyn Operator>,
    ) {
        let inputs: HashMap<PortId, String> = inputs
            .into_iter()
            .map(|desc| (desc.port_id.into(), desc.port_type))
            .collect();
        let outputs: HashMap<_, _> = outputs
            .into_iter()
            .map(|desc| (desc.port_id.into(), desc.port_type))
            .collect();

        self.operators.insert(
            id.clone(),
            OperatorLoaded {
                id,
                inputs,
                outputs,
                state: Arc::new(RwLock::new(state)),
                operator,
                library: None,
            },
        );
    }

    pub fn add_static_sink(
        &mut self,
        id: NodeId,
        input: PortDescriptor,
        state: State,
        sink: Arc<dyn Sink>,
    ) {
        self.sinks.insert(
            id.clone(),
            SinkLoaded {
                id,
                input,
                state: Arc::new(RwLock::new(state)),
                sink,
                library: None,
            },
        );
    }

    /// Add a link, connecting two nodes.
    ///
    /// ## Error
    ///
    /// This function will return error if the nodes that are to be linked where not previously
    /// added to the Dataflow **or** if the types of the ports (declared in the nodes) are not
    /// identical.
    pub fn add_link(
        &mut self,
        from: LinkFromDescriptor,
        to: LinkToDescriptor,
        size: Option<usize>,
        queueing_policy: Option<String>,
        priority: Option<usize>,
    ) -> ZFResult<()> {
        let from_type = self.get_node_port_type(&from.node, &from.output)?;
        let to_type = self.get_node_port_type(&to.node, &to.input)?;

        if from_type == to_type {
            self.links.push(LinkDescriptor {
                from,
                to,
                size,
                queueing_policy,
                priority,
            });
            return Ok(());
        }

        Err(ZFError::PortTypeNotMatching((from_type, to_type)))
    }

    fn get_node_port_type(&self, node_id: &NodeId, port_id: &str) -> ZFResult<String> {
        if let Some(operator) = self.operators.get(node_id) {
            if let Some(port_type) = operator.inputs.get(port_id) {
                return Ok(port_type.clone());
            } else if let Some(port_type) = operator.outputs.get(port_id) {
                return Ok(port_type.clone());
            } else {
                return Err(ZFError::PortNotFound((
                    node_id.clone(),
                    port_id.to_string(),
                )));
            }
        }

        if let Some(source) = self.sources.get(node_id) {
            if source.output.port_id == port_id {
                return Ok(source.output.port_type.clone());
            }

            return Err(ZFError::PortNotFound((
                node_id.clone(),
                port_id.to_string(),
            )));
        }

        if let Some(sink) = self.sinks.get(node_id) {
            if sink.input.port_id == port_id {
                return Ok(sink.input.port_type.clone());
            }

            return Err(ZFError::PortNotFound((
                node_id.clone(),
                port_id.to_string(),
            )));
        }

        Err(ZFError::OperatorNotFound(node_id.clone()))
    }
}

// pub struct DataFlowGraph {
//     pub uuid: Uuid,
//     flow: Arc<str>,
//     pub context: RuntimeContext,
//     runners: HashMap<NodeId, Runner>,
// }

// impl DataFlowGraph {
//     pub fn try_new(context: RuntimeContext, dataflow_record: DataFlowRecord) -> ZFResult<Self> {
//         // Filter all the nodes to only keep the ones that are assigned to the current runtime.
//         let sources: Vec<SourceRecord> = dataflow_record
//             .sources
//             .into_iter()
//             .filter(|source| source.runtime == context.runtime_name)
//             .collect();

//         let operators: Vec<OperatorRecord> = dataflow_record
//             .operators
//             .into_iter()
//             .filter(|operator| operator.runtime == context.runtime_name)
//             .collect();

//         let sinks: Vec<SinkRecord> = dataflow_record
//             .sinks
//             .into_iter()
//             .filter(|sink| sink.runtime == context.runtime_name)
//             .collect();

//         let connectors: Vec<ZFConnectorRecord> = dataflow_record
//             .connectors
//             .into_iter()
//             .filter(|connector| connector.runtime == context.runtime_name)
//             .collect();

//         // Gather all the ids of the nodes assigned to the current runtime to only create the links
//         // needed on that runtime.
//         let mut nodes: Vec<NodeId> =
//             Vec::with_capacity(sources.len() + operators.len() + sinks.len() + connectors.len());
//         nodes.extend(
//             sources
//                 .iter()
//                 .map(|source| source.id.clone())
//                 .collect::<Vec<_>>(),
//         );
//         nodes.extend(
//             operators
//                 .iter()
//                 .map(|operator| operator.id.clone())
//                 .collect::<Vec<_>>(),
//         );
//         nodes.extend(sinks.iter().map(|sink| sink.id.clone()).collect::<Vec<_>>());
//         nodes.extend(
//             connectors
//                 .iter()
//                 .map(|connector| connector.id.clone())
//                 .collect::<Vec<_>>(),
//         );

//         // Create all the necessary links.
//         let mut links = create_links(&nodes, &dataflow_record.links)?;

//         // Create all the runners.
//         let mut runners = HashMap::with_capacity(nodes.len());

//         for source in sources.into_iter() {
//             let io = links.remove(&source.id);
//             runners.insert(
//                 source.id.clone(),
//                 Runner::Source(SourceRunner::try_new(&context, source, io)?),
//             );
//         }

//         for operator in operators.into_iter() {
//             let io = links.remove(&operator.id);
//             runners.insert(
//                 operator.id.clone(),
//                 Runner::Operator(OperatorRunner::try_new(&context, operator, io)?),
//             );
//         }

//         for sink in sinks.into_iter() {
//             let io = links.remove(&sink.id);
//             runners.insert(
//                 sink.id.clone(),
//                 Runner::Sink(SinkRunner::try_new(&context, sink, io)?),
//             );
//         }

//         for connector in connectors.into_iter() {
//             let io = links.remove(&connector.id);
//             match connector.kind {
//                 ZFConnectorKind::Sender => {
//                     runners.insert(
//                         connector.id.clone(),
//                         Runner::Sender(ZenohSender::try_new(&context, connector, io)?),
//                     );
//                 }
//                 ZFConnectorKind::Receiver => {
//                     runners.insert(
//                         connector.id.clone(),
//                         Runner::Receiver(ZenohReceiver::try_new(&context, connector, io)?),
//                     );
//                 }
//             }
//         }

//         Ok(Self {
//             uuid: Uuid::new_v4(),
//             flow: dataflow_record.flow.into(),
//             context,
//             runners,
//         })
//     }

//     pub fn set_name(&mut self, name: String) {
//         self.flow = name.into();
//     }

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

// pub fn get_runner(&self, operator_id: &NodeId) -> Option<&Runner> {
//     self.runners.get(operator_id)
// }

// pub fn get_runners(&self) -> Vec<&Runner> {
//     self.runners.values().collect()
// }

// pub fn get_sources(&self) -> Vec<&Runner> {
//     self.runners
//         .values()
//         .filter(|runner| matches!(runner, Runner::Source(_)))
//         .collect()
// }

// pub fn get_sinks(&self) -> Vec<&Runner> {
//     self.runners
//         .values()
//         .filter(|runner| matches!(runner, Runner::Sink(_)))
//         .collect()
// }

// pub fn get_operators(&self) -> Vec<&Runner> {
//     self.runners
//         .values()
//         .filter(|runner| matches!(runner, Runner::Operator(_)))
//         .collect()
// }

// pub fn get_connectors(&self) -> Vec<&Runner> {
//     self.runners
//         .values()
//         .filter(|runner| matches!(runner, Runner::Receiver(_) | Runner::Sender(_)))
//         .collect()
// }
// }

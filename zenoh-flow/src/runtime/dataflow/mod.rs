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
use std::time::Duration;
use uuid::Uuid;

use crate::model::connector::ZFConnectorRecord;
use crate::model::dataflow::record::DataFlowRecord;
use crate::model::link::{LinkDescriptor, LinkFromDescriptor, LinkToDescriptor, PortDescriptor};
use crate::model::period::PeriodDescriptor;
use crate::runtime::dataflow::node::{OperatorLoaded, SinkLoaded, SourceLoaded};
use crate::runtime::RuntimeContext;
use crate::{FlowId, NodeId, Operator, PortId, PortType, Sink, Source, State, ZFError, ZFResult};

pub struct Dataflow {
    pub(crate) uuid: Uuid,
    pub(crate) flow_id: FlowId,
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
    pub fn new(context: RuntimeContext, id: FlowId, uuid: Option<Uuid>) -> Self {
        let uuid = match uuid {
            Some(uuid) => uuid,
            None => Uuid::new_v4(),
        };

        Self {
            uuid,
            flow_id: id,
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
            .map(|r| context.loader.load_source(r))
            .collect();
        let sources: HashMap<_, _> = res_sources?
            .into_iter()
            .map(|source| (source.id.clone(), source))
            .collect();

        let res_operators: ZFResult<Vec<OperatorLoaded>> = record
            .operators
            .into_iter()
            .filter(|operator| operator.runtime == context.runtime_name)
            .map(|r| context.loader.load_operator(r))
            .collect();
        let operators: HashMap<_, _> = res_operators?
            .into_iter()
            .map(|operator| (operator.id.clone(), operator))
            .collect();

        let res_sinks: ZFResult<Vec<SinkLoaded>> = record
            .sinks
            .into_iter()
            .filter(|sink| sink.runtime == context.runtime_name)
            .map(|r| context.loader.load_sink(r))
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
            uuid: record.uuid,
            flow_id: record.flow.into(),
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
        deadline: Option<Duration>,
        state: State,
        operator: Arc<dyn Operator>,
    ) {
        let inputs: HashMap<PortId, PortType> = inputs
            .into_iter()
            .map(|desc| (desc.port_id, desc.port_type))
            .collect();
        let outputs: HashMap<PortId, PortType> = outputs
            .into_iter()
            .map(|desc| (desc.port_id, desc.port_type))
            .collect();

        self.operators.insert(
            id.clone(),
            OperatorLoaded {
                id,
                inputs,
                outputs,
                deadline,
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

    fn get_node_port_type(&self, node_id: &NodeId, port_id: &PortId) -> ZFResult<PortType> {
        if let Some(operator) = self.operators.get(node_id) {
            if let Some(port_type) = operator.inputs.get(port_id) {
                return Ok(port_type.clone());
            } else if let Some(port_type) = operator.outputs.get(port_id) {
                return Ok(port_type.clone());
            } else {
                return Err(ZFError::PortNotFound((node_id.clone(), port_id.clone())));
            }
        }

        if let Some(source) = self.sources.get(node_id) {
            if source.output.port_id == *port_id {
                return Ok(source.output.port_type.clone());
            }

            return Err(ZFError::PortNotFound((node_id.clone(), port_id.clone())));
        }

        if let Some(sink) = self.sinks.get(node_id) {
            if sink.input.port_id == *port_id {
                return Ok(sink.input.port_type.clone());
            }

            return Err(ZFError::PortNotFound((node_id.clone(), port_id.clone())));
        }

        Err(ZFError::NodeNotFound(node_id.clone()))
    }
}

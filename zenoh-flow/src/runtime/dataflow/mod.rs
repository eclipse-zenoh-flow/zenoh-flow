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

use async_std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

use crate::model::connector::ZFConnectorRecord;
use crate::model::dataflow::record::DataFlowRecord;
use crate::model::dataflow::validator::DataflowValidator;
use crate::model::deadline::E2EDeadlineRecord;
use crate::model::link::{LinkDescriptor, PortDescriptor};
use crate::model::{InputDescriptor, OutputDescriptor};
use crate::runtime::dataflow::node::{OperatorLoaded, SinkLoaded, SourceLoaded};
use crate::runtime::RuntimeContext;
use crate::{
    DurationDescriptor, FlowId, NodeId, Operator, PortId, PortType, Sink, Source, State, ZFResult,
};

pub struct Dataflow {
    pub(crate) uuid: Uuid,
    pub(crate) flow_id: FlowId,
    pub(crate) context: RuntimeContext,
    pub(crate) sources: HashMap<NodeId, SourceLoaded>,
    pub(crate) operators: HashMap<NodeId, OperatorLoaded>,
    pub(crate) sinks: HashMap<NodeId, SinkLoaded>,
    pub(crate) connectors: HashMap<NodeId, ZFConnectorRecord>,
    pub(crate) links: Vec<LinkDescriptor>,
    validator: DataflowValidator,
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
            validator: DataflowValidator::new(),
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

        let mut dataflow = Self {
            uuid: record.uuid,
            flow_id: record.flow.into(),
            context,
            sources,
            operators,
            sinks,
            connectors,
            links: record.links,
            validator: DataflowValidator::new(),
        };

        if let Some(e2e_deadlines) = record.end_to_end_deadlines {
            e2e_deadlines
                .into_iter()
                .for_each(|deadline| dataflow.add_end_to_end_deadline(deadline))
        }

        Ok(dataflow)
    }

    pub fn try_add_static_source(
        &mut self,
        id: NodeId,
        period: Option<DurationDescriptor>,
        output: PortDescriptor,
        state: State,
        source: Arc<dyn Source>,
    ) -> ZFResult<()> {
        self.validator.try_add_source(id.clone(), output.clone())?;

        self.sources.insert(
            id.clone(),
            SourceLoaded {
                id,
                output,
                state: Arc::new(Mutex::new(state)),
                period: period.map(|dur_desc| dur_desc.to_duration()),
                source,
                library: None,
                end_to_end_deadlines: vec![],
            },
        );

        Ok(())
    }

    pub fn try_add_static_operator(
        &mut self,
        id: NodeId,
        inputs: Vec<PortDescriptor>,
        outputs: Vec<PortDescriptor>,
        local_deadline: Option<Duration>,
        state: State,
        operator: Arc<dyn Operator>,
    ) -> ZFResult<()> {
        self.validator
            .try_add_operator(id.clone(), &inputs, &outputs)?;

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
                local_deadline,
                state: Arc::new(Mutex::new(state)),
                operator,
                library: None,
                end_to_end_deadlines: vec![],
            },
        );

        Ok(())
    }

    pub fn try_add_static_sink(
        &mut self,
        id: NodeId,
        input: PortDescriptor,
        state: State,
        sink: Arc<dyn Sink>,
    ) -> ZFResult<()> {
        self.validator.try_add_sink(id.clone(), input.clone())?;

        self.sinks.insert(
            id.clone(),
            SinkLoaded {
                id,
                input,
                state: Arc::new(Mutex::new(state)),
                sink,
                library: None,
                end_to_end_deadlines: vec![],
            },
        );

        Ok(())
    }

    /// Add a link, connecting two nodes.
    ///
    /// ## Error
    ///
    /// This function will return error if the nodes that are to be linked where not previously
    /// added to the Dataflow **or** if the types of the ports (declared in the nodes) are not
    /// identical.
    pub fn try_add_link(
        &mut self,
        from: OutputDescriptor,
        to: InputDescriptor,
        size: Option<usize>,
        queueing_policy: Option<String>,
        priority: Option<usize>,
    ) -> ZFResult<()> {
        self.validator.try_add_link(&from, &to)?;

        self.links.push(LinkDescriptor {
            from,
            to,
            size,
            queueing_policy,
            priority,
        });

        Ok(())
    }

    pub fn try_add_deadline(
        &mut self,
        from: OutputDescriptor,
        to: InputDescriptor,
        duration: Duration,
    ) -> ZFResult<()> {
        self.validator.validate_deadline(&from, &to)?;
        let deadline = E2EDeadlineRecord { from, to, duration };
        self.add_end_to_end_deadline(deadline);

        Ok(())
    }

    fn add_end_to_end_deadline(&mut self, deadline: E2EDeadlineRecord) {
        // Look for the "from" node in either Sources and Operators.
        if let Some(source) = self.sources.get_mut(&deadline.from.node) {
            source.end_to_end_deadlines.push(deadline.clone());
        }

        if let Some(operator) = self.operators.get_mut(&deadline.from.node) {
            operator.end_to_end_deadlines.push(deadline.clone());
        }

        // Look for the "to" node in either Operators and Sinks.
        if let Some(operator) = self.operators.get_mut(&deadline.to.node) {
            operator.end_to_end_deadlines.push(deadline.clone());
        }

        if let Some(sink) = self.sinks.get_mut(&deadline.to.node) {
            sink.end_to_end_deadlines.push(deadline);
        }
    }
}

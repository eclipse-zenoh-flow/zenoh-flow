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

use crate::{Operator, Sink, Source};
use async_std::sync::Arc;
use node::DataFlowNode;
use std::collections::HashMap;
use uhlc::HLC;

use crate::runtime::loader::{load_operator, load_sink, load_source};
use crate::runtime::message::Message;
use crate::runtime::runners::connector::{ZenohReceiver, ZenohSender};
use crate::runtime::runners::{
    operator::OperatorRunner, sink::SinkRunner, source::SourceRunner, Runner,
};
use crate::{
    model::connector::ZFConnectorKind,
    model::dataflow::DataFlowRecord,
    model::link::{LinkDescriptor, LinkFromDescriptor, LinkToDescriptor, PortDescriptor},
    model::node::{OperatorRecord, SinkRecord, SourceRecord},
    runtime::graph::link::link,
    runtime::graph::node::DataFlowNodeKind,
    runtime::RuntimeContext,
    types::{OperatorId, ZFError, ZFResult},
    utils::hlc::PeriodicHLC,
};
use uuid::Uuid;

pub struct DataFlowGraph {
    pub uuid: Uuid,
    pub flow: String,
    pub operators: HashMap<OperatorId, DataFlowNode>,
    pub links: Vec<LinkDescriptor>,
    pub operators_runners: HashMap<OperatorId, (Runner, DataFlowNodeKind)>,
    pub ctx: RuntimeContext,
}

impl DataFlowGraph {
    pub fn new(ctx: RuntimeContext) -> Self {
        Self {
            uuid: Uuid::nil(),
            flow: "".to_string(),
            operators: HashMap::new(),
            links: Vec::new(),
            operators_runners: HashMap::new(),
            ctx,
        }
    }

    pub fn from_record(dr: DataFlowRecord, ctx: RuntimeContext) -> ZFResult<Self> {
        let mut operators = HashMap::new();
        let mut links: Vec<LinkDescriptor> = Vec::new();
        for o in dr.operators {
            let dfn = DataFlowNode::Operator(o);
            operators.insert(dfn.get_id(), dfn);
        }

        for s in dr.sources {
            let dfn = DataFlowNode::Source(s);
            operators.insert(dfn.get_id(), dfn);
        }

        for s in dr.sinks {
            let dfn = DataFlowNode::Sink(s);
            operators.insert(dfn.get_id(), dfn);
        }

        for c in dr.connectors {
            let dfn = DataFlowNode::Connector(c);
            operators.insert(dfn.get_id(), dfn);
        }

        for l in dr.links {
            let (from_runtime, from_type) = match operators.get(&l.from.node) {
                Some(op) => match op.has_output(l.from.output.clone()) {
                    true => (op.get_runtime(), op.get_output_type(l.from.output.clone())?),
                    false => {
                        return Err(ZFError::PortNotFound((l.from.node, l.from.output.clone())))
                    }
                },
                None => return Err(ZFError::OperatorNotFound(l.from.node)),
            };

            let (to_runtime, to_type) = match operators.get(&l.to.node) {
                Some(op) => match op.has_input(l.to.input.clone()) {
                    true => (op.get_runtime(), op.get_input_type(l.to.input.clone())?),
                    false => return Err(ZFError::PortNotFound((l.to.node, l.to.input.clone()))),
                },
                None => return Err(ZFError::OperatorNotFound(l.to.node)),
            };

            if to_type != from_type {
                return Err(ZFError::PortTypeNotMatching((
                    to_type.to_string(),
                    from_type.to_string(),
                )));
            }

            if from_runtime == to_runtime {
                log::debug!("[Graph instantiation] [same runtime] Pushing link: {:?}", l);
                links.push(l.clone());
            } else {
                log::warn!(
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
            operators_runners: HashMap::new(),
            ctx,
        })
    }

    pub fn set_name(&mut self, name: String) {
        self.flow = name;
    }

    pub fn add_static_operator(
        &mut self,
        hlc: Arc<HLC>,
        id: OperatorId,
        inputs: Vec<PortDescriptor>,
        outputs: Vec<PortDescriptor>,
        operator: Arc<dyn Operator>,
        configuration: Option<HashMap<String, String>>,
    ) -> ZFResult<()> {
        let record = OperatorRecord {
            id: id.clone(),
            inputs,
            outputs,
            uri: None,
            configuration,
            runtime: self.ctx.runtime_name.clone(),
        };
        let dfn = DataFlowNode::Operator(record.clone());
        self.operators.insert(dfn.get_id(), dfn);
        let runner = Runner::Operator(OperatorRunner::new(record, hlc, operator, None));
        self.operators_runners
            .insert(id, (runner, DataFlowNodeKind::Operator));
        Ok(())
    }

    pub fn add_static_source(
        &mut self,
        hlc: Arc<HLC>,
        id: OperatorId,
        output: PortDescriptor,
        source: Arc<dyn Source>,
        configuration: Option<HashMap<String, String>>,
    ) -> ZFResult<()> {
        let record = SourceRecord {
            id: id.clone(),
            output,
            period: None,
            uri: None,
            configuration,
            runtime: self.ctx.runtime_name.clone(),
        };
        let dfn = DataFlowNode::Source(record.clone());
        self.operators.insert(dfn.get_id(), dfn);
        let non_periodic_hlc = PeriodicHLC::new(hlc, None);
        let runner = Runner::Source(SourceRunner::new(record, non_periodic_hlc, source, None));
        self.operators_runners
            .insert(id, (runner, DataFlowNodeKind::Source));
        Ok(())
    }

    pub fn add_static_sink(
        &mut self,
        id: OperatorId,
        input: PortDescriptor,
        sink: Arc<dyn Sink>,
        configuration: Option<HashMap<String, String>>,
    ) -> ZFResult<()> {
        let record = SinkRecord {
            id: id.clone(),
            input,
            uri: None,
            configuration,
            runtime: self.ctx.runtime_name.clone(),
        };
        let dfn = DataFlowNode::Sink(record.clone());
        self.operators.insert(dfn.get_id(), dfn);
        let runner = Runner::Sink(SinkRunner::new(record, sink, None));
        self.operators_runners
            .insert(id, (runner, DataFlowNodeKind::Sink));
        Ok(())
    }

    pub fn add_link(
        &mut self,
        from: LinkFromDescriptor,
        to: LinkToDescriptor,
        size: Option<usize>,
        queueing_policy: Option<String>,
        priority: Option<usize>,
    ) -> ZFResult<()> {
        let connection = LinkDescriptor {
            from,
            to,
            size,
            queueing_policy,
            priority,
        };

        let from_type = match self.operators.get(&connection.from.node) {
            Some(op) => match op.has_output(connection.from.output.clone()) {
                true => op.get_output_type(connection.from.output.clone())?,
                false => {
                    return Err(ZFError::PortNotFound((
                        connection.from.node.clone(),
                        connection.from.output.clone(),
                    )))
                }
            },
            None => return Err(ZFError::OperatorNotFound(connection.from.node.clone())),
        };

        let to_type = match self.operators.get(&connection.to.node) {
            Some(op) => match op.has_input(connection.to.input.clone()) {
                true => op.get_input_type(connection.to.input.clone())?,
                false => {
                    return Err(ZFError::PortNotFound((
                        connection.to.node.clone(),
                        connection.to.input.clone(),
                    )))
                }
            },
            None => return Err(ZFError::OperatorNotFound(connection.to.node.clone())),
        };

        if from_type == to_type {
            self.links.push(connection);
            return Ok(());
        }

        Err(ZFError::PortTypeNotMatching((
            String::from(from_type),
            String::from(to_type),
        )))
    }

    pub fn load(&mut self) -> ZFResult<()> {
        for op in self.operators.values() {
            if op.get_runtime() != self.ctx.runtime_name {
                continue;
            }

            match op {
                DataFlowNode::Operator(inner) => {
                    match &inner.uri {
                        Some(uri) => {
                            let runner =
                                load_operator(inner.clone(), self.ctx.hlc.clone(), uri.clone())?;
                            let runner = Runner::Operator(runner);
                            self.operators_runners
                                .insert(inner.id.clone(), (runner, DataFlowNodeKind::Operator));
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
                                inner.clone(),
                                PeriodicHLC::new(self.ctx.hlc.clone(), inner.period.clone()),
                                uri.clone(),
                            )?;
                            let runner = Runner::Source(runner);
                            self.operators_runners
                                .insert(inner.id.clone(), (runner, DataFlowNodeKind::Source));
                        }
                        None => {
                            // static source
                        }
                    }
                }
                DataFlowNode::Sink(inner) => {
                    match &inner.uri {
                        Some(uri) => {
                            let runner = load_sink(inner.clone(), uri.clone())?;
                            let runner = Runner::Sink(runner);
                            self.operators_runners
                                .insert(inner.id.clone(), (runner, DataFlowNodeKind::Sink));
                        }
                        None => {
                            //static sink
                        }
                    }
                }
                DataFlowNode::Connector(zc) => match zc.kind {
                    ZFConnectorKind::Sender => {
                        let runner =
                            ZenohSender::new(self.ctx.session.clone(), zc.resource.clone(), None);
                        let runner = Runner::Sender(runner);
                        self.operators_runners
                            .insert(zc.id.clone(), (runner, DataFlowNodeKind::Connector));
                    }

                    ZFConnectorKind::Receiver => {
                        let runner =
                            ZenohReceiver::new(self.ctx.session.clone(), zc.resource.clone(), None);
                        let runner = Runner::Receiver(runner);
                        self.operators_runners
                            .insert(zc.id.clone(), (runner, DataFlowNodeKind::Connector));
                    }
                },
            }
        }
        Ok(())
    }

    pub async fn make_connections(&self) -> ZFResult<()> {
        // Connects the operators via our FIFOs

        for l in &self.links {
            log::debug!("Creating link: {}", l);

            let up_node = self
                .operators
                .get(&l.from.node)
                .ok_or_else(|| ZFError::OperatorNotFound(l.from.node.clone()))?;
            let down_node = self
                .operators
                .get(&l.to.node)
                .ok_or_else(|| ZFError::OperatorNotFound(l.from.node.clone()))?;

            if up_node.get_runtime() != self.ctx.runtime_name
                || down_node.get_runtime() != self.ctx.runtime_name
            {
                continue;
            }

            let (up_runner, _) = self
                .operators_runners
                .get(&l.from.node)
                .ok_or_else(|| ZFError::OperatorNotFound(l.from.node.clone()))?;
            let (down_runner, _) = self
                .operators_runners
                .get(&l.to.node)
                .ok_or_else(|| ZFError::OperatorNotFound(l.to.node.clone()))?;
            let (tx, rx) = link::<Message>(None, l.from.output.clone(), l.to.input.clone());

            up_runner.add_output(tx).await?;
            down_runner.add_input(rx).await?;
        }

        Ok(())
    }

    pub fn get_runner(&self, operator_id: &OperatorId) -> Option<&Runner> {
        self.operators_runners.get(operator_id).map(|(r, _)| r)
    }

    pub fn get_runners(&self) -> Vec<&Runner> {
        let mut runners = vec![];

        for (runner, _) in self.operators_runners.values() {
            runners.push(runner);
        }
        runners
    }

    pub fn get_sources(&self) -> Vec<&Runner> {
        let mut runners = vec![];

        for (runner, kind) in self.operators_runners.values() {
            if let DataFlowNodeKind::Source = kind {
                runners.push(runner);
            }
        }
        runners
    }

    pub fn get_sinks(&self) -> Vec<&Runner> {
        let mut runners = vec![];

        for (runner, kind) in self.operators_runners.values() {
            if let DataFlowNodeKind::Sink = kind {
                runners.push(runner);
            }
        }
        runners
    }

    pub fn get_operators(&self) -> Vec<&Runner> {
        let mut runners = vec![];

        for (runner, kind) in self.operators_runners.values() {
            if let DataFlowNodeKind::Operator = kind {
                runners.push(runner);
            }
        }
        runners
    }

    pub fn get_connectors(&self) -> Vec<&Runner> {
        let mut runners = vec![];

        for (runner, kind) in self.operators_runners.values() {
            if let DataFlowNodeKind::Connector = kind {
                runners.push(runner);
            }
        }
        runners
    }
}

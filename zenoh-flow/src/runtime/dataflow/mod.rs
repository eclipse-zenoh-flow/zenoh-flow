//
// Copyright (c) 2022 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

pub mod instance;
pub mod loader;
pub mod node;

use std::collections::HashMap;
use std::sync::Arc;
use uhlc::HLC;
use uuid::Uuid;

use crate::model::record::{
    DataFlowRecord, LinkRecord, OperatorRecord, SinkRecord, SourceRecord, ZFConnectorKind,
    ZFConnectorRecord,
};
// use crate::model::dataflow::validator::DataflowValidator;
use self::instance::runners::connector::{ZenohReceiver, ZenohSender};
use self::instance::runners::Runner;
use self::instance::{create_links, DataFlowInstance};
use self::node::{OperatorFactory, SinkFactory, SourceFactory};
use crate::model::descriptor::{InputDescriptor, OutputDescriptor};
use crate::prelude::{Context, ErrorKind};
use crate::runtime::RuntimeContext;
use crate::traits::{self, Node};
use crate::types::NodeId;
use crate::{zferror, Result as ZFResult};

use super::InstanceContext;

/// TODO(J-Loudet) Documentation: a DataFlow is the structure that lets us generate, through the
/// different factories, a DataFlowInstance. However, a DataFlow is not a "DataFlowFactory": it can
/// only produce a single a DataFlowInstance. The node factories allow us to "stop" the nodes and
/// restart them anew.
///
/// Later on, we envision using these factories to start replicas / do load-balancing.
///
/// In short:
/// - Factory  = node generation, i.e. a node that can be `run`
/// - DataFlow = a set of node factories
/// - DataFlowInstance = a set of running nodes
pub struct DataFlow {
    pub(crate) uuid: Uuid,
    pub(crate) flow: Arc<str>, // TODO(J-Loudet) Agree on naming, flow vs name
    pub(crate) context: RuntimeContext,
    pub(crate) source_factories: HashMap<NodeId, SourceFactory>,
    pub(crate) operator_factories: HashMap<NodeId, OperatorFactory>,
    pub(crate) sink_factories: HashMap<NodeId, SinkFactory>,
    pub(crate) connectors: HashMap<NodeId, ZFConnectorRecord>,
    pub(crate) links: Vec<LinkRecord>,
    pub(crate) counter: u32,
}

impl DataFlow {
    /// TODO(J-Loudet) Improve documentation.
    ///
    /// We use it internally to test the performance of Zenoh-Flow. Not a good idea to try to use it
    /// directly.
    pub fn new(name: impl AsRef<str>, context: RuntimeContext) -> Self {
        Self {
            uuid: Uuid::new_v4(),
            flow: name.as_ref().into(),
            context,
            source_factories: HashMap::new(),
            operator_factories: HashMap::new(),
            sink_factories: HashMap::new(),
            connectors: HashMap::new(),
            links: Vec::new(),
            counter: 0,
        }
    }

    /// TODO(J-Loudet) Improve documentation.
    ///
    /// We use it internally to test the performance of Zenoh-Flow. Not a good idea to try to use it
    /// directly.
    pub fn add_source_factory(
        &mut self,
        node_id: impl AsRef<str>,
        record: SourceRecord,
        factory: Arc<dyn traits::SourceFactoryTrait>,
    ) {
        self.source_factories.insert(
            node_id.as_ref().into(),
            SourceFactory::new_static(record, factory),
        );
    }

    /// TODO(J-Loudet) Improve documentation.
    ///
    /// We use it internally to test the performance of Zenoh-Flow. Not a good idea to try to use it
    /// directly.
    pub fn add_operator_factory(
        &mut self,
        node_id: impl AsRef<str>,
        record: OperatorRecord,
        factory: Arc<dyn traits::OperatorFactoryTrait>,
    ) {
        self.operator_factories.insert(
            node_id.as_ref().into(),
            OperatorFactory::new_static(record, factory),
        );
    }

    /// TODO(J-Loudet) Improve documentation.
    ///
    /// We use it internally to test the performance of Zenoh-Flow. Not a good idea to try to use it
    /// directly.
    pub fn add_sink_factory(
        &mut self,
        node_id: impl AsRef<str>,
        record: SinkRecord,
        factory: Arc<dyn traits::SinkFactoryTrait>,
    ) {
        self.sink_factories.insert(
            node_id.as_ref().into(),
            SinkFactory::new_static(record, factory),
        );
    }

    /// TODO(J-Loudet) Improve documentation.
    ///
    /// We use it internally to test the performance of Zenoh-Flow. Not a good idea to try to use it
    /// directly.
    pub fn add_link(&mut self, from: OutputDescriptor, to: InputDescriptor) {
        self.links.push(LinkRecord {
            uid: self.counter,
            from,
            to,
            size: None,
            queueing_policy: None,
            priority: None,
        });
        self.counter += 1;
    }

    /// TODO(J-Loudet) Improve documentation.
    ///
    /// Try to create a DataFlow based on a DataFlowRecord (TODO Add link).
    ///
    /// # Error
    ///
    /// Failures can happen when trying to load node factories. (TODO Add links to the different
    /// `load_*_factory`)
    pub fn try_new(record: DataFlowRecord, context: RuntimeContext) -> ZFResult<Self> {
        let DataFlowRecord {
            uuid,
            flow,
            operators,
            sinks,
            sources,
            connectors,
            links,
            counter: _,
        } = record;

        let source_factories = sources
            .into_iter()
            .map(|(source_id, source_record)| {
                context
                    .loader
                    .load_source_factory(source_record)
                    // `map` leaves the Error untouched and allows us to transform the Ok into a
                    // tuple so we can finally build an HashMap
                    .map(|source_factory| (source_id, source_factory))
            })
            .collect::<ZFResult<HashMap<NodeId, SourceFactory>>>()?;

        let operator_factories = operators
            .into_iter()
            .map(|(operator_id, operator_record)| {
                context
                    .loader
                    .load_operator_factory(operator_record)
                    // `map` leaves the Error untouched and allows us to transform the Ok into a
                    // tuple so we can finally build an HashMap
                    .map(|operator_factory| (operator_id, operator_factory))
            })
            .collect::<ZFResult<HashMap<NodeId, OperatorFactory>>>()?;

        let sink_factories = sinks
            .into_iter()
            .map(|(sink_id, sink_record)| {
                context
                    .loader
                    .load_sink_factory(sink_record)
                    // `map` leaves the Error untouched and allows us to transform the Ok into a
                    // tuple so we can finally build an HashMap
                    .map(|sink_factory| (sink_id, sink_factory))
            })
            .collect::<ZFResult<HashMap<NodeId, SinkFactory>>>()?;

        Ok(Self {
            uuid,
            flow: flow.into(),
            context,
            source_factories,
            operator_factories,
            sink_factories,
            connectors,
            links,
            counter: 0,
        })
    }

    /// TODO(J-Loudet) Improve documentation.
    ///
    /// Try instantiating the DataFlow2, hence generating a DataFlowInstance2.
    pub async fn try_instantiate(self, hlc: Arc<HLC>) -> ZFResult<DataFlowInstance> {
        let instance_context = Arc::new(InstanceContext {
            flow_id: self.flow.clone(),
            instance_id: self.uuid,
            runtime: self.context.clone(),
        });

        let mut node_ids: Vec<NodeId> = Vec::with_capacity(
            self.source_factories.len()
                + self.operator_factories.len()
                + self.sink_factories.len()
                + self.connectors.len(),
        );

        node_ids.append(&mut self.source_factories.keys().cloned().collect::<Vec<_>>());
        node_ids.append(&mut self.operator_factories.keys().cloned().collect::<Vec<_>>());
        node_ids.append(&mut self.sink_factories.keys().cloned().collect::<Vec<_>>());
        node_ids.append(&mut self.connectors.keys().cloned().collect::<Vec<_>>());

        let mut links = create_links(&node_ids, &self.links, hlc.clone())?;

        let ctx = Context::new(&instance_context);

        let mut runners = HashMap::with_capacity(self.source_factories.len());
        for (source_id, source_factory) in &self.source_factories {
            let mut context = ctx.clone();
            let (_, outputs) = links.remove(source_id).ok_or_else(|| {
                zferror!(
                    ErrorKind::IOError,
                    "Links for Source < {} > were not created.",
                    &source_id
                )
            })?;

            let source = source_factory
                .factory
                .new_source(&mut context, &source_factory.configuration, outputs)
                .await?;
            let runner = Runner::new(source, context.inputs_callbacks, context.outputs_callbacks);
            runners.insert(source_id.clone(), runner);
        }

        for (operator_id, operator_factory) in &self.operator_factories {
            let mut context = ctx.clone();
            let (inputs, outputs) = links.remove(operator_id).ok_or_else(|| {
                zferror!(
                    ErrorKind::IOError,
                    "Links for Operator < {} > were not created.",
                    &operator_id
                )
            })?;

            let operator = operator_factory
                .factory
                .new_operator(
                    &mut context,
                    &operator_factory.configuration,
                    inputs,
                    outputs,
                )
                .await?;
            let runner = Runner::new(
                operator,
                context.inputs_callbacks,
                context.outputs_callbacks,
            );
            runners.insert(operator_id.clone(), runner);
        }

        for (sink_id, sink_factory) in &self.sink_factories {
            let mut context = ctx.clone();
            let (inputs, _) = links.remove(sink_id).ok_or_else(|| {
                zferror!(
                    ErrorKind::IOError,
                    "Links for Sink < {} > were not created.",
                    &sink_id
                )
            })?;

            let sink = sink_factory
                .factory
                .new_sink(&mut context, &sink_factory.configuration, inputs)
                .await?;
            let runner = Runner::new(sink, context.inputs_callbacks, context.outputs_callbacks);
            runners.insert(sink_id.clone(), runner);
        }

        for (connector_id, connector_record) in &self.connectors {
            let session = instance_context.runtime.session.clone();
            let node = match &connector_record.kind {
                ZFConnectorKind::Sender => {
                    let (inputs, _) = links.remove(connector_id).ok_or_else(|| {
                        zferror!(
                            ErrorKind::IOError,
                            "Links for Sink < {} > were not created.",
                            connector_id
                        )
                    })?;
                    Some(
                        Arc::new(ZenohSender::new(connector_record, session, inputs).await?)
                            as Arc<dyn Node>,
                    )
                }
                ZFConnectorKind::Receiver => {
                    let (_, outputs) = links.remove(connector_id).ok_or_else(|| {
                        zferror!(
                            ErrorKind::IOError,
                            "Links for Source < {} > were not created.",
                            &connector_id
                        )
                    })?;
                    Some(
                        Arc::new(ZenohReceiver::new(connector_record, session, outputs).await?)
                            as Arc<dyn Node>,
                    )
                }
            };

            let runner = Runner::new(node, vec![], vec![]);
            runners.insert(connector_id.clone(), runner);
        }

        Ok(DataFlowInstance {
            _instance_context: instance_context,
            data_flow: self,
            runners,
        })
    }
}

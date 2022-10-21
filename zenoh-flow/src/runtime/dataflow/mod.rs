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
use uuid::Uuid;

use crate::model::record::{
    DataFlowRecord, LinkRecord, OperatorRecord, SinkRecord, SourceRecord, ZFConnectorRecord,
};
// use crate::model::dataflow::validator::DataflowValidator;
use self::node::{OperatorFactory, SinkFactory, SourceFactory};
use crate::model::descriptor::{InputDescriptor, OutputDescriptor};
use crate::runtime::RuntimeContext;
use crate::traits;
use crate::types::NodeId;
use crate::Result as ZFResult;

/// `DataFlow` is an intermediate structure which primary purpose is to store the loaded libraries.
///
/// This intermediate structure is needed to create data flows programmatically. It is mostly used
/// **internally** to assess the performance of Zenoh-Flow.
///
/// **End users should not use it directly**: _no verifications to ensure the validity of the data flow
/// are performed at this step or in the next ones._
pub struct DataFlow {
    pub(crate) uuid: Uuid,
    pub(crate) flow: Arc<str>,
    pub(crate) context: RuntimeContext,
    pub(crate) source_factories: HashMap<NodeId, SourceFactory>,
    pub(crate) operator_factories: HashMap<NodeId, OperatorFactory>,
    pub(crate) sink_factories: HashMap<NodeId, SinkFactory>,
    pub(crate) connectors: HashMap<NodeId, ZFConnectorRecord>,
    pub(crate) links: Vec<LinkRecord>,
    pub(crate) counter: u32,
}

impl DataFlow {
    /// Create a new empty `DataFlow` named `name` with the provided `RuntimeContext`.
    ///
    /// **Unless you know very well what you are doing, you should not use this function**.
    ///
    /// This function is the entry point to create a data flow programmatically. **It should not be
    /// called by end users**.
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

    /// Add a `SourceFactory` to the `DataFlow`.
    ///
    /// **Unless you know very well what you are doing, you should not use this method**.
    ///
    /// If the Source is not correctly connected to downstream nodes, its data will never be
    /// received.
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

    /// Add an `OperatorFactory` to the `DataFlow`.
    ///
    /// **Unless you know very well what you are doing, you should not use this method**.
    ///
    /// If the Operator is not correctly connected to upstream and downstream nodes, it will never
    /// receive, process and emit data.
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

    /// Add a `SinkFactory` to the `DataFlow`.
    ///
    /// **Unless you know very well what you are doing, you should not use this method**.
    ///
    /// If the Sink is not correctly connected to upstream nodes, it will never receive data.
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

    /// Add a `Link` to the `DataFlow`.
    ///
    /// **Unless you know very well what you are doing, you should not use this method**.
    ///
    /// If the `OutputDescriptor` and `InputDescriptor` are incorrect, Zenoh-Flow will error out at
    /// _runtime_.
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

    /// Given a `DataFlowRecord`, create the corresponding `DataFlow` by dynamically loading the
    /// shared libraries.
    ///
    /// # Error
    ///
    /// Failures can happen when trying to load node factories.
    pub fn try_new(record: DataFlowRecord, context: RuntimeContext) -> ZFResult<Self> {
        let DataFlowRecord {
            uuid,
            flow,
            operators,
            sinks,
            sources,
            connectors,
            links,
            counter,
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
                    .map(|operator_factory| (operator_id, operator_factory))
            })
            .collect::<ZFResult<HashMap<NodeId, OperatorFactory>>>()?;

        let sink_factories = sinks
            .into_iter()
            .map(|(sink_id, sink_record)| {
                context
                    .loader
                    .load_sink_factory(sink_record)
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
            counter,
        })
    }
}

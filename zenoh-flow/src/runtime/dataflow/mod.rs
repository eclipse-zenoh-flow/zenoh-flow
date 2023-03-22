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

use self::node::{OperatorConstructor, SinkConstructor, SourceConstructor};
use self::node::{OperatorFn, SinkFn, SourceFn};
use crate::model::descriptor::{InputDescriptor, OutputDescriptor};
use crate::model::record::{
    DataFlowRecord, LinkRecord, OperatorRecord, SinkRecord, SourceRecord, ZFConnectorRecord,
};
use crate::runtime::RuntimeContext;
use crate::types::NodeId;
use crate::Result as ZFResult;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

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
    pub(crate) source_constructors: HashMap<NodeId, SourceConstructor>,
    pub(crate) operator_constructors: HashMap<NodeId, OperatorConstructor>,
    pub(crate) sink_constructors: HashMap<NodeId, SinkConstructor>,
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
            source_constructors: HashMap::new(),
            operator_constructors: HashMap::new(),
            sink_constructors: HashMap::new(),
            connectors: HashMap::new(),
            links: Vec::new(),
            counter: 0,
        }
    }

    /// Add a `Source` to the `DataFlow`.
    ///
    /// **Unless you know very well what you are doing, you should not use this method**.
    ///
    /// If the Source is not correctly connected to downstream nodes, its data will never be
    /// received.
    pub fn add_source(&mut self, record: SourceRecord, constructor: SourceFn) {
        self.source_constructors.insert(
            record.id.clone(),
            SourceConstructor::new_static(record, constructor),
        );
    }

    /// Add an `Operator` to the `DataFlow`.
    ///
    /// **Unless you know very well what you are doing, you should not use this method**.
    ///
    /// If the Operator is not correctly connected to upstream and downstream nodes, it will never
    /// receive, process and emit data.
    pub fn add_operator(&mut self, record: OperatorRecord, constructor: OperatorFn) {
        self.operator_constructors.insert(
            record.id.clone(),
            OperatorConstructor::new_static(record, constructor),
        );
    }

    /// Add a `Sink` to the `DataFlow`.
    ///
    /// **Unless you know very well what you are doing, you should not use this method**.
    ///
    /// If the Sink is not correctly connected to upstream nodes, it will never receive data.
    pub fn add_sink(&mut self, record: SinkRecord, constructor: SinkFn) {
        self.sink_constructors.insert(
            record.id.clone(),
            SinkConstructor::new_static(record, constructor),
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
            shared_memory_element_size: None,
            shared_memory_elements: None,
            shared_memory_backoff: None,
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

        let source_constructors = sources
            .into_iter()
            .filter(|(_, record)| record.runtime == context.runtime_name)
            .map(|(source_id, source_record)| {
                context
                    .loader
                    .load_source_constructor(source_record)
                    // `map` leaves the Error untouched and allows us to transform the Ok into a
                    // tuple so we can finally build an HashMap
                    .map(|source_constructor| (source_id, source_constructor))
            })
            .collect::<ZFResult<HashMap<NodeId, SourceConstructor>>>()?;

        let operator_constructors = operators
            .into_iter()
            .filter(|(_, record)| record.runtime == context.runtime_name)
            .map(|(operator_id, operator_record)| {
                context
                    .loader
                    .load_operator_constructor(operator_record)
                    .map(|operator_constructor| (operator_id, operator_constructor))
            })
            .collect::<ZFResult<HashMap<NodeId, OperatorConstructor>>>()?;

        let sink_constructors = sinks
            .into_iter()
            .filter(|(_, record)| record.runtime == context.runtime_name)
            .map(|(sink_id, sink_record)| {
                context
                    .loader
                    .load_sink_constructor(sink_record)
                    .map(|sink_constructor| (sink_id, sink_constructor))
            })
            .collect::<ZFResult<HashMap<NodeId, SinkConstructor>>>()?;

        let connectors = connectors
            .into_iter()
            .filter(|(_, record)| record.runtime == context.runtime_name)
            .collect::<HashMap<NodeId, ZFConnectorRecord>>();

        Ok(Self {
            uuid,
            flow: flow.into(),
            context,
            source_constructors,
            operator_constructors,
            sink_constructors,
            connectors,
            links,
            counter,
        })
    }
}

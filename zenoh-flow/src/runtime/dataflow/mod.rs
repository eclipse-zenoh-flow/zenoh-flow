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

use crate::model::connector::ZFConnectorRecord;
use crate::model::dataflow::record::DataFlowRecord;
use crate::model::dataflow::validator::DataflowValidator;
use crate::model::link::{LinkRecord, PortDescriptor};
use crate::model::{InputDescriptor, OutputDescriptor};
use crate::runtime::dataflow::node::{OperatorLoaded, SinkLoaded, SourceLoaded};
use crate::runtime::RuntimeContext;
use crate::traits::{Operator, Sink, Source};
use crate::types::{Configuration, FlowId, NodeId};
use crate::Result as ZFResult;

/// The data flow struct.
/// This struct contains all the information needed to instantiate a data flow.
/// running within a Zenoh Flow runtime.
/// It stores in a `RuntimeContext` all the loaded nodes and the connectors.
/// It also contains a `DataflowValidator` that is used to verify that the
/// data flow can be instantiated.
pub struct Dataflow {
    pub(crate) uuid: Uuid,
    pub(crate) flow_id: FlowId,
    pub(crate) context: RuntimeContext,
    pub(crate) sources: HashMap<NodeId, SourceLoaded>,
    pub(crate) operators: HashMap<NodeId, OperatorLoaded>,
    pub(crate) sinks: HashMap<NodeId, SinkLoaded>,
    pub(crate) connectors: HashMap<NodeId, ZFConnectorRecord>,
    pub(crate) links: Vec<LinkRecord>,
    validator: DataflowValidator,
    counter: u32,
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
            counter: 0u32,
        }
    }

    /// Creates a new `Dataflow` departing from
    /// an existing [`DataFlowRecord`](`DataFlowRecord`)
    ///
    /// This function is called by the runtime when instantiating
    /// a dynamically loaded graph.
    /// It loads the nodes using the loader stored in
    /// the [`RuntimeContext`](`RuntimeContext`).
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - fails to load nodes
    pub fn try_new(context: RuntimeContext, record: DataFlowRecord) -> ZFResult<Self> {
        let loaded_sources = record
            .sources
            .into_values()
            .filter(|source| source.runtime == context.runtime_name)
            .map(|source| context.loader.load_source(source))
            .collect::<Result<Vec<_>, _>>()?;
        let sources: HashMap<_, _> = loaded_sources
            .into_iter()
            .map(|source| (source.id.clone(), source))
            .collect();

        let loaded_operators = record
            .operators
            .into_values()
            .filter(|operator| operator.runtime == context.runtime_name)
            .map(|r| context.loader.load_operator(r))
            .collect::<Result<Vec<_>, _>>()?;
        let operators: HashMap<_, _> = loaded_operators
            .into_iter()
            .map(|operator| (operator.id.clone(), operator))
            .collect();

        let loaded_sinks = record
            .sinks
            .into_values()
            .filter(|sink| sink.runtime == context.runtime_name)
            .map(|r| context.loader.load_sink(r))
            .collect::<Result<Vec<_>, _>>()?;
        let sinks: HashMap<_, _> = loaded_sinks
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
            validator: DataflowValidator::new(),
            counter: record.counter,
        })
    }

    /// Tries to add a static source to the data flow.
    ///
    /// If the validation fails the source cannot be added.
    /// This is meant to be used when creating an empty data flow using
    /// `Dataflow::new(..)` function.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - validation fails
    pub fn try_add_static_source(
        &mut self,
        id: NodeId,
        configuration: Option<Configuration>,
        outputs: Vec<PortDescriptor>,
        source: Arc<dyn Source>,
    ) -> ZFResult<()> {
        // self.validator.try_add_source(id.clone(), output.clone())?;
        self.validator.try_add_source(id.clone(), &outputs)?;

        self.sources.insert(
            id.clone(),
            SourceLoaded {
                id,
                configuration,
                // output: (output, self.counter).into(),
                source,
                library: None,
            },
        );
        self.counter += 1;

        Ok(())
    }

    /// Tries to add a static operator to the data flow.
    ///
    /// If the validation fails the operator cannot be added.
    /// This is meant to be used when creating an empty data flow using
    /// `Dataflow::new(..)` function.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - validation fails
    pub fn try_add_static_operator(
        &mut self,
        id: NodeId,
        configuration: Option<Configuration>,
        inputs: Vec<PortDescriptor>,
        outputs: Vec<PortDescriptor>,
        operator: Arc<dyn Operator>,
    ) -> ZFResult<()> {
        self.validator
            .try_add_operator(id.clone(), &inputs, &outputs)?;

        // let inputs: HashMap<PortId, PortType> = inputs
        //     .into_iter()
        //     .map(|desc| (desc.port_id, desc.port_type))
        //     .collect();
        // let outputs: HashMap<PortId, PortType> = outputs
        //     .into_iter()
        //     .map(|desc| (desc.port_id, desc.port_type))
        //     .collect();

        self.operators.insert(
            id.clone(),
            OperatorLoaded {
                id,
                configuration,
                // inputs,
                // outputs,
                operator,
                library: None,
            },
        );

        Ok(())
    }

    /// Tries to add a static sink to the data flow.
    ///
    /// If the validation fails the sink cannot be added.
    /// This is meant to be used when creating an empty data flow using
    /// `Dataflow::new(..)` function.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - validation fails
    pub fn try_add_static_sink(
        &mut self,
        id: NodeId,
        configuration: Option<Configuration>,
        inputs: Vec<PortDescriptor>,
        sink: Arc<dyn Sink>,
    ) -> ZFResult<()> {
        // self.validator.try_add_sink(id.clone(), input.clone())?;
        self.validator.try_add_sink(id.clone(), &inputs)?;

        self.sinks.insert(
            id.clone(),
            SinkLoaded {
                id,
                configuration,
                // input: (input, self.counter).into(),
                sink,
                library: None,
            },
        );
        self.counter += 1;
        Ok(())
    }

    /// Add a link, connecting two nodes.
    ///
    /// ## Error
    ///
    /// This function will return error if the nodes that are to be linked where not previously
    /// added to the Dataflow **or** if the types of the ports (declared in the nodes) are not
    /// identical.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - validation fails
    pub fn try_add_link(
        &mut self,
        from: OutputDescriptor,
        to: InputDescriptor,
        size: Option<usize>,
        queueing_policy: Option<String>,
        priority: Option<usize>,
    ) -> ZFResult<()> {
        self.validator.try_add_link(&from, &to)?;

        self.links.push(LinkRecord {
            uid: self.counter,
            from,
            to,
            size,
            queueing_policy,
            priority,
        });
        self.counter += 1;

        Ok(())
    }
}

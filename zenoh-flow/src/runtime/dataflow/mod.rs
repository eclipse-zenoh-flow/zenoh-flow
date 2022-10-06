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
    DataFlowRecord, LinkRecord, OperatorRecord, SinkRecord, SourceRecord, ZFConnectorRecord,
};
// use crate::model::dataflow::validator::DataflowValidator;
use crate::model::descriptor::{InputDescriptor, OutputDescriptor, PortDescriptor};
use crate::prelude::{Context, ErrorKind};
use crate::runtime::dataflow::node::{OperatorLoaded, SinkLoaded, SourceLoaded};
use crate::runtime::RuntimeContext;
use crate::traits::{self, Operator, Sink, Source};
use crate::types::{Configuration, FlowId, NodeId};
use crate::{zferror, Result as ZFResult};

use self::instance::runners::Runner2;
use self::instance::{create_links, DataFlowInstance2};
use self::node::{OperatorFactory, SinkFactory, SourceFactory};

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
pub struct DataFlow2 {
    pub(crate) uuid: Uuid,
    pub(crate) flow: Arc<str>, // TODO(J-Loudet) Agree on naming, flow vs name
    pub(crate) context: RuntimeContext,
    pub(crate) source_factories: HashMap<NodeId, SourceFactory>,
    pub(crate) operator_factories: HashMap<NodeId, OperatorFactory>,
    pub(crate) sink_factories: HashMap<NodeId, SinkFactory>,
    pub(crate) connectors: Vec<ZFConnectorRecord>,
    pub(crate) links: Vec<LinkRecord>,
    pub(crate) counter: usize,
}

impl DataFlow2 {
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
            connectors: Vec::new(),
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
        factory: Arc<dyn traits::SourceFactory>,
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
        factory: Arc<dyn traits::OperatorFactory>,
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
        factory: Arc<dyn traits::SinkFactory>,
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
    pub async fn try_instantiate(self, hlc: Arc<HLC>) -> ZFResult<DataFlowInstance2> {
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

        let mut links = create_links(&node_ids, &self.links, Arc::clone(&hlc))?;

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
            let runner = Runner2::new(source, context.inputs_callbacks, context.outputs_callbacks);
            runners.insert(Arc::clone(source_id), runner);
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
            let runner = Runner2::new(
                operator,
                context.inputs_callbacks,
                context.outputs_callbacks,
            );
            runners.insert(Arc::clone(operator_id), runner);
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
            let runner = Runner2::new(sink, context.inputs_callbacks, context.outputs_callbacks);
            runners.insert(Arc::clone(sink_id), runner);
        }

        Ok(DataFlowInstance2 {
            _instance_context: instance_context,
            data_flow: self,
            runners,
        })
    }
}

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
    // validator: DataflowValidator,
    counter: usize,
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
            // validator: DataflowValidator::new(),
            counter: 0,
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
            // validator: DataflowValidator::new(),
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
        _outputs: Vec<PortDescriptor>,
        source: Arc<dyn Source>,
    ) -> ZFResult<()> {
        // self.validator.try_add_source(id.clone(), output.clone())?;
        // self.validator.try_add_source(id.clone(), &outputs)?;

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
        _inputs: Vec<PortDescriptor>,
        _outputs: Vec<PortDescriptor>,
        operator: Arc<dyn Operator>,
    ) -> ZFResult<()> {
        // TODO Add validator again.
        // self.validator
        //     .try_add_operator(id.clone(), &inputs, &outputs)?;

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
        _inputs: Vec<PortDescriptor>,
        sink: Arc<dyn Sink>,
    ) -> ZFResult<()> {
        // TODO Add validator again.
        // self.validator.try_add_sink(id.clone(), &inputs)?;

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
        // TODO Add validator again.
        // self.validator.try_add_link(&from, &to)?;

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

//
// Copyright (c) 2021 - 2023 ZettaScale Technology
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

use std::{collections::HashMap, sync::Arc};

#[cfg(feature = "zenoh")]
use crate::runners::builtin::zenoh::{sink::ZenohSink, source::ZenohSource};
#[cfg(feature = "zenoh")]
use crate::runners::connectors::{ZenohConnectorReceiver, ZenohConnectorSender};
use crate::{
    instance::DataFlowInstance,
    loader::{Loader, NodeSymbol},
    runners::Runner,
};
use anyhow::{bail, Context as errContext};
use uhlc::HLC;
#[cfg(feature = "zenoh")]
use zenoh::Session;
#[cfg(feature = "shared-memory")]
use zenoh_flow_commons::SharedMemoryConfiguration;
use zenoh_flow_commons::{NodeId, RecordId, Result, RuntimeId};
use zenoh_flow_descriptors::{SinkVariant, SourceVariant};
use zenoh_flow_nodes::{
    prelude::{Inputs, Outputs},
    Context, OperatorFn, SinkFn, SourceFn,
};
use zenoh_flow_records::DataFlowRecord;

pub(crate) type Channels = HashMap<NodeId, (Inputs, Outputs)>;

pub struct Runtime {
    pub(crate) hlc: Arc<HLC>,
    #[cfg(feature = "zenoh")]
    pub(crate) session: Arc<Session>,
    pub(crate) runtime_id: RuntimeId,
    #[cfg(feature = "shared-memory")]
    pub(crate) shared_memory: SharedMemoryConfiguration,
    pub(crate) loader: Loader,
    pub(crate) flows: HashMap<RecordId, DataFlowInstance>,
}

impl Runtime {
    pub fn id(&self) -> &RuntimeId {
        &self.runtime_id
    }

    /// TODO@J-Loudet
    pub fn new(
        id: RuntimeId,
        loader: Loader,
        hlc: Arc<HLC>,
        #[cfg(feature = "zenoh")] session: Arc<Session>,
        #[cfg(feature = "shared-memory")] shared_memory: SharedMemoryConfiguration,
    ) -> Self {
        Self {
            runtime_id: id,
            #[cfg(feature = "zenoh")]
            session,
            hlc,
            loader,
            #[cfg(feature = "shared-memory")]
            shared_memory,
            flows: HashMap::default(),
        }
    }

    /// TODO@J-Loudet
    pub async fn try_instantiate_data_flow(&mut self, data_flow: DataFlowRecord) -> Result<()> {
        let mut channels = self.create_channels(&data_flow)?;
        let mut runners = HashMap::<NodeId, Runner>::default();

        let context = Context::new(
            data_flow.name().clone(),
            data_flow.id().clone(),
            self.runtime_id.clone(),
        );

        runners.extend(
            self.try_load_operators(&data_flow, &mut channels, context.clone())
                .await?,
        );
        runners.extend(
            self.try_load_sources(&data_flow, &mut channels, context.clone())
                .await?,
        );
        runners.extend(
            self.try_load_sinks(&data_flow, &mut channels, context.clone())
                .await?,
        );

        #[cfg(feature = "zenoh")]
        {
            runners.extend(self.try_load_receivers(&data_flow, &mut channels).await?);
            runners.extend(self.try_load_senders(&data_flow, &mut channels)?);
        }

        self.flows.insert(
            data_flow.id().clone(),
            DataFlowInstance {
                record: data_flow,
                runners,
            },
        );

        Ok(())
    }

    /// TODO@J-Loudet
    pub fn get_instance_mut(&mut self, record_id: &RecordId) -> Option<&mut DataFlowInstance> {
        self.flows.get_mut(record_id)
    }

    /// Create all the channels for the provided `DataFlowRecord`.
    ///
    /// # Errors
    ///
    /// The only scenario in which this method fails is if we did not correctly processed the data flow descriptor and
    /// ended up having a link with nodes on two different runtime.
    fn create_channels(&self, record: &DataFlowRecord) -> Result<Channels> {
        let nodes_runtime = match record.mapping.get(&self.runtime_id) {
            Some(nodes) => nodes,
            // NOTE: There is a possibility that the runtime that is orchestrating the deployment of the data flow will
            // not have to run any node. In which case, `record.mapping.get` will return nothing.
            None => return Ok(HashMap::default()),
        };

        let mut channels = HashMap::default();
        for link in &record.links {
            if !nodes_runtime.contains(&link.from.node) || !nodes_runtime.contains(&link.to.node) {
                #[cfg(feature = "zenoh")]
                {
                    // NOTE: If any of the two nodes run on this runtime then we have an issue, we did not process
                    // correctly the data flow and forgot to add a connector.
                    if nodes_runtime.contains(&link.from.node)
                        || nodes_runtime.contains(&link.to.node)
                    {
                        bail!(
                            r#"
Zenoh-Flow encountered a fatal internal error: a link is connecting two nodes that are on *different* runtime.

The problematic link is:
{}
"#,
                            link
                        );
                    }

                    continue;
                }

                #[cfg(not(feature = "zenoh"))]
                {
                    bail!(
                        r#"
The Zenoh-Flow runtime was not compiled with the feature "zenoh" enabled, it is thus impossible to have nodes running on
different runtime. Maybe enable the "zenoh" feature?

The problematic link is:
{}
"#,
                        link
                    )
                }
            }

            let (tx, rx) = flume::unbounded();
            let (_, outputs) = channels
                .entry(link.from.node.clone())
                .or_insert_with(|| (Inputs::default(), Outputs::new(self.hlc.clone())));
            outputs.insert(link.from.output.clone(), tx);

            let (inputs, _) = channels
                .entry(link.to.node.clone())
                .or_insert_with(|| (Inputs::default(), Outputs::new(self.hlc.clone())));
            inputs.insert(link.to.input.clone(), rx);
        }

        Ok(channels)
    }

    /// TODO@J-Loudet
    async fn try_load_operators(
        &mut self,
        record: &DataFlowRecord,
        channels: &mut Channels,
        context: Context,
    ) -> Result<HashMap<NodeId, Runner>> {
        let mut runners = HashMap::default();
        let assigned_nodes = match record.mapping.get(&self.runtime_id) {
            Some(nodes) => nodes,
            None => return Ok(HashMap::default()),
        };

        for (operator_id, operator) in record
            .operators
            .iter()
            .filter(|(_, operator)| assigned_nodes.contains(&operator.id))
        {
            let (inputs, outputs) = channels.remove(operator_id).context(format!(
                r#"
Zenoh-Flow encountered a fatal internal error.
The channels for the Inputs and Outputs of Operator < {} > were not created.
        "#,
                &operator_id
            ))?;

            let constructor = self
                .loader
                .try_load_constructor::<OperatorFn>(&operator.library, &NodeSymbol::Operator)?;
            let operator_node = (constructor)(
                context.clone(),
                operator.configuration.clone(),
                inputs,
                outputs,
            )
            .await?;
            runners.insert(
                operator_id.clone(),
                Runner::new(operator_id.clone(), operator_node),
            );
        }

        Ok(runners)
    }

    /// TODO@J-Loudet
    async fn try_load_sources(
        &mut self,
        record: &DataFlowRecord,
        channels: &mut Channels,
        context: Context,
    ) -> Result<HashMap<NodeId, Runner>> {
        let mut runners = HashMap::default();
        let assigned_nodes = match record.mapping.get(&self.runtime_id) {
            Some(nodes) => nodes,
            None => return Ok(HashMap::default()),
        };

        for (source_id, source) in record
            .sources
            .iter()
            .filter(|(_, source)| assigned_nodes.contains(&source.id))
        {
            let (_, outputs) = channels.remove(source_id).context(format!(
                r#"
Zenoh-Flow encountered a fatal internal error.
The channels for the Outputs of Source < {} > were not created.
        "#,
                &source_id
            ))?;

            let runner = match &source.source {
                SourceVariant::Library(uri) => {
                    let constructor = self
                        .loader
                        .try_load_constructor::<SourceFn>(uri, &NodeSymbol::Source)?;
                    let source_node =
                        (constructor)(context.clone(), source.configuration.clone(), outputs)
                            .await?;

                    Runner::new(source.id.clone(), source_node)
                }
                #[cfg(not(feature = "zenoh"))]
                SourceVariant::Zenoh(_) => {
                    bail!(
                        r#"
The Zenoh-Flow runtime was compiled without the feature "zenoh" but includes a built-in Zenoh Source.
Maybe change the features in the Cargo.toml?
"#
                    )
                }
                #[cfg(feature = "zenoh")]
                SourceVariant::Zenoh(key_exprs) => {
                    let dyn_source =
                        ZenohSource::try_new(&source.id, self.session.clone(), key_exprs, outputs)
                            .await?;
                    Runner::new(source.id.clone(), Arc::new(dyn_source))
                }
            };

            runners.insert(source_id.clone(), runner);
        }

        Ok(runners)
    }

    /// TODO@J-Loudet
    async fn try_load_sinks(
        &mut self,
        record: &DataFlowRecord,
        channels: &mut Channels,
        context: Context,
    ) -> Result<HashMap<NodeId, Runner>> {
        let mut runners = HashMap::default();
        let assigned_nodes = match record.mapping.get(&self.runtime_id) {
            Some(nodes) => nodes,
            None => return Ok(HashMap::default()),
        };

        for (sink_id, sink) in record
            .sinks
            .iter()
            .filter(|(_, sink)| assigned_nodes.contains(&sink.id))
        {
            let (inputs, _) = channels.remove(sink_id).context(format!(
                r#"
Zenoh-Flow encountered a fatal internal error.
The channels for the Inputs of Sink < {} > were not created.
        "#,
                &sink_id
            ))?;

            let runner = match &sink.sink {
                SinkVariant::Library(uri) => {
                    let constructor = self
                        .loader
                        .try_load_constructor::<SinkFn>(uri, &NodeSymbol::Sink)?;
                    let sink_node =
                        (constructor)(context.clone(), sink.configuration.clone(), inputs).await?;

                    Runner::new(sink.id.clone(), sink_node)
                }
                #[cfg(not(feature = "zenoh"))]
                SinkVariant::Zenoh(_) => {
                    bail!(
                        r#"
The Zenoh-Flow runtime was compiled without the feature "zenoh" but includes a built-in Zenoh Sink.
Maybe change the features in the Cargo.toml?
"#
                    )
                }
                #[cfg(feature = "zenoh")]
                SinkVariant::Zenoh(key_exprs) => {
                    let zenoh_sink = ZenohSink::try_new(
                        sink_id.clone(),
                        self.session.clone(),
                        key_exprs,
                        #[cfg(feature = "shared-memory")]
                        &self.shared_memory,
                        inputs,
                    )
                    .await?;

                    Runner::new(sink_id.clone(), Arc::new(zenoh_sink))
                }
            };

            runners.insert(sink_id.clone(), runner);
        }

        Ok(runners)
    }

    /// TODO@J-Loudet
    #[cfg(feature = "zenoh")]
    async fn try_load_receivers(
        &mut self,
        record: &DataFlowRecord,
        channels: &mut Channels,
    ) -> Result<HashMap<NodeId, Runner>> {
        let mut runners = HashMap::new();

        for (receiver_id, receiver) in record
            .receivers
            .iter()
            .filter(|(_, receiver)| receiver.runtime() == &self.runtime_id)
        {
            let (_, outputs) = channels.remove(receiver_id).context(format!(
                r#"
Zenoh-Flow encountered a fatal internal error.
The channels for the Outputs of Connector Receiver < {} > were not created.
        "#,
                receiver_id
            ))?;

            let runner =
                ZenohConnectorReceiver::try_new(self.session.clone(), receiver.clone(), outputs)
                    .await?;

            runners.insert(
                receiver_id.clone(),
                Runner::new(receiver_id.clone(), Arc::new(runner)),
            );
        }

        Ok(runners)
    }

    /// TODO@J-Loudet
    #[cfg(feature = "zenoh")]
    fn try_load_senders(
        &mut self,
        record: &DataFlowRecord,
        channels: &mut Channels,
    ) -> Result<HashMap<NodeId, Runner>> {
        let mut runners = HashMap::new();

        for (sender_id, sender) in record
            .senders
            .iter()
            .filter(|(_, sender)| sender.runtime() == &self.runtime_id)
        {
            let (inputs, _) = channels.remove(sender_id).context(format!(
                r#"
Zenoh-Flow encountered a fatal internal error.
The channels for the Inputs of Connector Sender < {} > were not created.
        "#,
                sender_id
            ))?;

            let runner = ZenohConnectorSender::try_new(
                self.session.clone(),
                #[cfg(feature = "shared-memory")]
                &self.shared_memory,
                sender.clone(),
                inputs,
            )?;

            runners.insert(
                sender_id.clone(),
                Runner::new(sender_id.clone(), Arc::new(runner)),
            );
        }

        Ok(runners)
    }
}

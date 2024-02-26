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

// This file centralizes all the logic regarding the LOADING of a data flow on a Runtime.
//
// The entry point, from an external point of view (think API consumer), is the method: `try_load_data_flow`.
//
// This method requires several steps:
// - Creating all the channels connecting all the nodes -> the channels are then passed down the constructor of all
//   the nodes.
// - For each type of node and each node:
//   - load its library,
//   - call its constructor with the correct parameters (i.e. only Inputs for a Sink, only Outputs for a Source).

use super::Runtime;
use crate::loader::NodeSymbol;
use crate::runners::builtin::zenoh::sink::ZenohSink;
use crate::runners::builtin::zenoh::source::ZenohSource;
use crate::{instance::DataFlowInstance, runners::Runner};

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Context as _};
use async_std::sync::RwLock;
use libloading::Library;
use url::Url;
use zenoh_flow_commons::{NodeId, Result};
use zenoh_flow_descriptors::{SinkVariant, SourceVariant};
use zenoh_flow_nodes::prelude::{Inputs, Outputs};
use zenoh_flow_nodes::{Context, OperatorFn, SinkFn, SourceFn};
use zenoh_flow_records::DataFlowRecord;

pub(crate) type Channels = HashMap<NodeId, (Inputs, Outputs)>;

impl Runtime {
    /// TODO@J-Loudet
    pub async fn try_load_data_flow(&self, data_flow: DataFlowRecord) -> Result<()> {
        // -----------------------------------
        // The following code tries to do two things:
        // 1. minimizing the amount of time `self.flows` is locked,
        // 2. ensuring that which ever thread accesses a data flow from `self.flows` will access it **after** it was
        //    fully loaded.
        //
        // To achieve 1. we put all `DataFlowInstance` into `Arc` which lets us drop the lock on `self.flows` while
        // retaining on reference on the `DataFlowInstance` we are interested in.
        //
        // To achieve 2. when we want to load an instance we insert in `self.flows` a **locked** lock of the instance
        // we are trying to create.
        let instance_id = data_flow.instance_id().clone();
        let instance = Arc::new(RwLock::new(DataFlowInstance::new(data_flow)));
        let mut instance_guard = instance.write().await;

        let mut flows_guard = self.flows.write().await;
        let instance_from_flows = flows_guard
            .entry(instance_id)
            .or_insert_with(|| instance.clone())
            .clone();

        drop(flows_guard);

        // NOTE: We have to compare the `Arc` we obtain from `self.flows` in case another thread was tasked with
        // creating the same instance and arrived there first.
        //
        // If the two pointers are equals then we have to continue, if they are different it means another thread was on
        // it before.
        if !Arc::ptr_eq(&instance, &instance_from_flows) {
            tracing::warn!(
                "Data Flow < {} > ({}) is either being instantiated or already is, exiting",
                instance_guard.instance_id(),
                instance_guard.name()
            );
            return Ok(());
        }
        // -----------------------------------

        let mut runners = HashMap::<NodeId, Runner>::default();
        let data_flow = &instance_guard.record;

        let mut channels = self.create_channels(data_flow)?;

        let context = Context::new(
            data_flow.name().clone(),
            data_flow.instance_id().clone(),
            self.runtime_id.clone(),
        );

        runners.extend(
            self.try_load_operators(data_flow, &mut channels, context.clone())
                .await?,
        );
        runners.extend(
            self.try_load_sources(data_flow, &mut channels, context.clone())
                .await?,
        );
        runners.extend(
            self.try_load_sinks(data_flow, &mut channels, context.clone())
                .await?,
        );

        #[cfg(feature = "zenoh")]
        {
            runners.extend(self.try_load_receivers(data_flow, &mut channels).await?);
            runners.extend(self.try_load_senders(data_flow, &mut channels)?);
        }

        instance_guard.runners = runners;
        Ok(())
    }

    /// Create all the channels for the provided `DataFlowRecord`.
    ///
    /// # Errors
    ///
    /// The only scenario in which this method fails is if we did not correctly processed the data flow descriptor and
    /// ended up having a link with nodes on two different runtime.
    fn create_channels(&self, record: &DataFlowRecord) -> Result<Channels> {
        let nodes_runtime = match record.mapping().get(&self.runtime_id) {
            Some(nodes) => nodes,
            // NOTE: There is a possibility that the runtime that is orchestrating the deployment of the data flow will
            // not have to run any node. In which case, `record.mapping.get` will return nothing.
            None => return Ok(HashMap::default()),
        };

        let mut channels = HashMap::default();
        for link in record.links() {
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
        &self,
        record: &DataFlowRecord,
        channels: &mut Channels,
        context: Context,
    ) -> Result<HashMap<NodeId, Runner>> {
        let mut runners = HashMap::default();
        let assigned_nodes = match record.mapping().get(&self.runtime_id) {
            Some(nodes) => nodes,
            None => return Ok(HashMap::default()),
        };

        for (operator_id, operator) in record
            .operators()
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

            let (constructor, library) = self
                .try_load_constructor::<OperatorFn>(&operator.library, &NodeSymbol::Operator)
                .await?;
            let operator_node = (constructor)(
                context.clone(),
                operator.configuration.clone(),
                inputs,
                outputs,
            )
            .await?;
            runners.insert(
                operator_id.clone(),
                Runner::new(operator_id.clone(), operator_node, Some(library)),
            );
        }

        Ok(runners)
    }

    /// TODO@J-Loudet
    async fn try_load_sources(
        &self,
        record: &DataFlowRecord,
        channels: &mut Channels,
        context: Context,
    ) -> Result<HashMap<NodeId, Runner>> {
        let mut runners = HashMap::default();
        let assigned_nodes = match record.mapping().get(&self.runtime_id) {
            Some(nodes) => nodes,
            None => return Ok(HashMap::default()),
        };

        for (source_id, source) in record
            .sources()
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
                    let (constructor, library) = self
                        .try_load_constructor::<SourceFn>(uri, &NodeSymbol::Source)
                        .await?;
                    let source_node =
                        (constructor)(context.clone(), source.configuration.clone(), outputs)
                            .await?;

                    Runner::new(source.id.clone(), source_node, Some(library))
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
                    Runner::new(source.id.clone(), Arc::new(dyn_source), None)
                }
            };

            runners.insert(source_id.clone(), runner);
        }

        Ok(runners)
    }

    /// TODO@J-Loudet
    async fn try_load_sinks(
        &self,
        record: &DataFlowRecord,
        channels: &mut Channels,
        context: Context,
    ) -> Result<HashMap<NodeId, Runner>> {
        let mut runners = HashMap::default();
        let assigned_nodes = match record.mapping().get(&self.runtime_id) {
            Some(nodes) => nodes,
            None => return Ok(HashMap::default()),
        };

        for (sink_id, sink) in record
            .sinks()
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
                    let (constructor, library) = self
                        .try_load_constructor::<SinkFn>(uri, &NodeSymbol::Sink)
                        .await?;
                    let sink_node =
                        (constructor)(context.clone(), sink.configuration.clone(), inputs).await?;

                    Runner::new(sink.id.clone(), sink_node, Some(library))
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

                    Runner::new(sink_id.clone(), Arc::new(zenoh_sink), None)
                }
            };

            runners.insert(sink_id.clone(), runner);
        }

        Ok(runners)
    }

    /// TODO@J-Loudet
    #[cfg(feature = "zenoh")]
    async fn try_load_receivers(
        &self,
        record: &DataFlowRecord,
        channels: &mut Channels,
    ) -> Result<HashMap<NodeId, Runner>> {
        use crate::runners::connectors::ZenohConnectorReceiver;

        let mut runners = HashMap::new();
        let assigned_nodes = match record.mapping().get(&self.runtime_id) {
            Some(nodes) => nodes,
            None => return Ok(HashMap::default()),
        };

        for (receiver_id, receiver) in record
            .receivers()
            .iter()
            .filter(|(_, receiver)| assigned_nodes.contains(&receiver.id()))
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
                Runner::new(receiver_id.clone(), Arc::new(runner), None),
            );
        }

        Ok(runners)
    }

    /// TODO@J-Loudet
    #[cfg(feature = "zenoh")]
    fn try_load_senders(
        &self,
        record: &DataFlowRecord,
        channels: &mut Channels,
    ) -> Result<HashMap<NodeId, Runner>> {
        use crate::runners::connectors::ZenohConnectorSender;

        let mut runners = HashMap::new();
        let assigned_nodes = match record.mapping().get(&self.runtime_id) {
            Some(nodes) => nodes,
            None => return Ok(HashMap::default()),
        };

        for (sender_id, sender) in record
            .senders()
            .iter()
            .filter(|(_, sender)| assigned_nodes.contains(&sender.id()))
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
                Runner::new(sender_id.clone(), Arc::new(runner), None),
            );
        }

        Ok(runners)
    }

    async fn try_load_constructor<C>(
        &self,
        url: &Url,
        node_symbol: &NodeSymbol,
    ) -> Result<(C, Arc<Library>)> {
        let mut loader_write_guard = self.loader.lock().await;
        loader_write_guard.try_load_constructor::<C>(url, node_symbol)
    }
}

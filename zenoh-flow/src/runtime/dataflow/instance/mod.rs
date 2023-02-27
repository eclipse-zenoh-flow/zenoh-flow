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

pub mod builtin;
pub mod runners;

use self::runners::connector::{ZenohReceiver, ZenohSender};
use self::runners::Runner;
use super::DataFlow;
use crate::io::{Inputs, Outputs};
use crate::model::record::{LinkRecord, ZFConnectorKind};
use crate::prelude::{Context, Node};
use crate::runtime::InstanceContext;
use crate::types::NodeId;
use crate::zfresult::ErrorKind;
use crate::Result;
use crate::{bail, zferror};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use uhlc::HLC;

/// A `DataFlowInstance` is an instance of a data flow that is ready to be run.
///
/// All Zenoh-Flow daemons involved in the deployment of an instance of a data flow will create this
/// structure to manage the nodes they are responsible for. Each daemon will keep in that structure
/// only their view of the instance.
pub struct DataFlowInstance {
    pub(crate) _instance_context: Arc<InstanceContext>,
    pub(crate) data_flow: DataFlow,
    pub(crate) runners: HashMap<NodeId, Runner>,
}

impl Deref for DataFlowInstance {
    type Target = DataFlow;

    fn deref(&self) -> &Self::Target {
        &self.data_flow
    }
}

impl DataFlowInstance {
    /// Retrieve the `NodeId` of the `Sink`s of this data flow instance running on the current
    /// daemon.
    ///
    /// CAVEAT: It is possible (and likely) that not all `Sink`s run on a single daemon. Hence, this
    /// list will be a subset of the list of all `Sink`s of this data flow.
    pub fn get_sinks(&self) -> Vec<NodeId> {
        self.sink_constructors.keys().cloned().collect()
    }

    /// Retrieve the `NodeId` of the `Source`s of this data flow instance running on the current
    /// daemon.
    ///
    /// CAVEAT: It is possible (and likely) that not all `Source`s run on a single daemon. Hence,
    /// this list will be a subset of the list of all `Source`s of this data flow.
    pub fn get_sources(&self) -> Vec<NodeId> {
        self.source_constructors.keys().cloned().collect()
    }

    /// Retrieve the `NodeId` of the `Operator`s of this data flow instance running on the current
    /// daemon.
    ///
    /// CAVEAT: It is possible (and likely) that not all `Operator`s run on a single daemon. Hence,
    /// this list will be a subset of the list of all `Operator`s of this data flow.
    pub fn get_operators(&self) -> Vec<NodeId> {
        self.operator_constructors.keys().cloned().collect()
    }

    /// Retrieve the `NodeId` of the `ZFConnector`s of this data flow instance running on the
    /// current daemon.
    ///
    /// CAVEAT: It is possible (and likely) that not all `ZFConnector`s run on a single daemon.
    /// Hence, this list will be a subset of the list of all `ZFConnector`s of this data flow.
    pub fn get_connectors(&self) -> Vec<NodeId> {
        self.connectors.keys().cloned().collect()
    }

    /// Start the node whose id matches the one provided.
    ///
    /// Start means launching as many tasks as necessary to run continuously the `Node`, input
    /// and/or output callbacks.
    ///
    /// Start is idempotent, if the node is already running, nothing will happen.
    ///
    /// # Error
    ///
    /// This method can return an error if the provided `node_id` is not found.
    pub fn start_node(&mut self, node_id: &NodeId) -> Result<()> {
        if let Some(runner) = self.runners.get_mut(node_id) {
            runner.start();
            return Ok(());
        }

        bail!(
            ErrorKind::NodeNotFound(node_id.clone()),
            "Node < {} > not found",
            node_id
        )
    }

    /// Stop the node whose id matches the one provided.
    ///
    /// Stop means canceling all the tasks that were launched. Note that `stop` does not interrupt a
    /// currently running task. The task will effectively be stopped the next time it encounters an
    /// `await`.
    ///
    /// Stop is idempotent, if the node is not running, nothing will happen.
    ///
    /// # Error
    ///
    /// This method can return an error if the provided `node_id` is not found.
    pub async fn stop_node(&mut self, node_id: &NodeId) -> Result<()> {
        if let Some(runner) = self.runners.get_mut(node_id) {
            return runner.stop().await;
        }

        bail!(
            ErrorKind::NodeNotFound(node_id.clone()),
            "Node < {} > not found",
            node_id
        )
    }

    /// Given a `DataFlow` and an `HLC`, try to instantiate the data flow by generating all the
    /// nodes (via their factories) and all the connections --- _running on the daemon_.
    ///
    /// # Error
    ///
    /// This function can return an error if:
    /// - some links are missing which resulted in some missing connections,
    /// - a factory failed to generate a node.
    pub async fn try_instantiate(data_flow: DataFlow, hlc: Arc<HLC>) -> Result<Self> {
        let instance_context = Arc::new(InstanceContext {
            flow_id: data_flow.flow.clone(),
            instance_id: data_flow.uuid,
            runtime: data_flow.context.clone(),
        });

        let mut node_ids: Vec<NodeId> = Vec::with_capacity(
            data_flow.source_constructors.len()
                + data_flow.operator_constructors.len()
                + data_flow.sink_constructors.len()
                + data_flow.connectors.len(),
        );

        node_ids.append(
            &mut data_flow
                .source_constructors
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
        );
        node_ids.append(
            &mut data_flow
                .operator_constructors
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
        );
        node_ids.append(
            &mut data_flow
                .sink_constructors
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
        );
        node_ids.append(&mut data_flow.connectors.keys().cloned().collect::<Vec<_>>());

        let mut links = create_links(&node_ids, &data_flow.links, hlc.clone())?;

        let context = Context::new(&instance_context);

        let mut runners = HashMap::with_capacity(data_flow.source_constructors.len());
        for (source_id, source_constructor) in &data_flow.source_constructors {
            let (_, outputs) = links.remove(source_id).ok_or_else(|| {
                zferror!(
                    ErrorKind::IOError,
                    "Links for Source < {} > were not created.",
                    &source_id
                )
            })?;

            let source = (source_constructor.constructor)(
                context.clone(),
                source_constructor.configuration.clone(),
                outputs,
            )
            .await?;

            let runner = Runner::new(source);
            runners.insert(source_id.clone(), runner);
        }

        for (operator_id, operator_constructor) in &data_flow.operator_constructors {
            let (inputs, outputs) = links.remove(operator_id).ok_or_else(|| {
                zferror!(
                    ErrorKind::IOError,
                    "Links for Operator < {} > were not created.",
                    &operator_id
                )
            })?;

            let operator = (operator_constructor.constructor)(
                context.clone(),
                operator_constructor.configuration.clone(),
                inputs,
                outputs,
            )
            .await?;

            let runner = Runner::new(operator);
            runners.insert(operator_id.clone(), runner);
        }

        for (sink_id, sink_constructor) in &data_flow.sink_constructors {
            let (inputs, _) = links.remove(sink_id).ok_or_else(|| {
                zferror!(
                    ErrorKind::IOError,
                    "Links for Sink < {} > were not created.",
                    &sink_id
                )
            })?;

            let sink = (sink_constructor.constructor)(
                context.clone(),
                sink_constructor.configuration.clone(),
                inputs,
            )
            .await?;

            let runner = Runner::new(sink);
            runners.insert(sink_id.clone(), runner);
        }

        for (connector_id, connector_record) in &data_flow.connectors {
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
                    Arc::new(ZenohSender::new(connector_record, session, inputs).await?)
                        as Arc<dyn Node>
                }
                ZFConnectorKind::Receiver => {
                    let (_, outputs) = links.remove(connector_id).ok_or_else(|| {
                        zferror!(
                            ErrorKind::IOError,
                            "Links for Source < {} > were not created.",
                            &connector_id
                        )
                    })?;
                    Arc::new(
                        ZenohReceiver::new(connector_record, session, hlc.clone(), outputs).await?,
                    ) as Arc<dyn Node>
                }
            };

            let runner = Runner::new(node);
            runners.insert(connector_id.clone(), runner);
        }

        Ok(DataFlowInstance {
            _instance_context: instance_context,
            data_flow,
            runners,
        })
    }
}

/// Creates the [`Link`](`Link`) between the `nodes` using `links`.
///
/// # Errors
/// An error variant is returned in case of:
/// -  port id is duplicated.
pub(crate) fn create_links(
    nodes: &[NodeId],
    links: &[LinkRecord],
    hlc: Arc<HLC>,
) -> Result<HashMap<NodeId, (Inputs, Outputs)>> {
    let mut io: HashMap<NodeId, (Inputs, Outputs)> = HashMap::with_capacity(nodes.len());

    for link_desc in links {
        let upstream_node = link_desc.from.node.clone();
        let downstream_node = link_desc.to.node.clone();

        // Nodes have been filtered based on their runtime. If the runtime of either one of the node
        // is not equal to that of the current runtime, the channels should not be created.
        if !nodes.contains(&upstream_node) || !nodes.contains(&downstream_node) {
            continue;
        }

        // FIXME Introduce a user-configurable maximum capacity on the links. This also requires
        // implementing a dropping policy.
        let (tx, rx) = flume::unbounded();
        let from = link_desc.from.output.clone();
        let to = link_desc.to.input.clone();

        match io.get_mut(&upstream_node) {
            Some((_, outputs)) => outputs.insert(from.clone(), tx),
            None => {
                let inputs = Inputs::new();
                let mut outputs = Outputs::new(hlc.clone());
                outputs.insert(from.clone(), tx);

                io.insert(upstream_node, (inputs, outputs));
            }
        }

        match io.get_mut(&downstream_node) {
            Some((inputs, _)) => inputs.insert(to.clone(), rx),
            None => {
                let outputs = Outputs::new(hlc.clone());

                let mut inputs = Inputs::new();
                inputs.insert(to.clone(), rx);

                io.insert(downstream_node, (inputs, outputs));
            }
        }
    }

    Ok(io)
}

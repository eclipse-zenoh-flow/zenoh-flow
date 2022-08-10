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

#![allow(clippy::manual_async_fn)]
use std::collections::HashMap;
use std::convert::TryFrom;

use crate::model::dataflow::descriptor::FlattenDataFlowDescriptor;
use crate::{
    model::{
        dataflow::record::DataFlowRecord,
        node::{SimpleOperatorDescriptor, SinkDescriptor, SourceDescriptor},
    },
    serde::{Deserialize, Serialize},
};
use async_std::sync::Arc;
use uuid::Uuid;

use self::dataflow::loader::LoaderConfig;
use crate::error::ZFError;
use crate::runtime::dataflow::loader::Loader;
use crate::runtime::message::ControlMessage;
use crate::types::{FlowId, RuntimeId, ZFResult};
use uhlc::HLC;
use zenoh::config::Config as ZenohConfig;
use zenoh::Session;
use zrpc::zrpcresult::{ZRPCError, ZRPCResult};
use zrpc_macros::znservice;

pub mod dataflow;
pub mod message;
pub mod resources;

/// The context of a Zenoh Flow runtime.
/// This is shared across all the instances in a runtime.
/// It allows sharing the `zenoh::Session`, the `Loader`,
/// the `HLC` and other relevant singletons.
#[derive(Clone)]
pub struct RuntimeContext {
    pub session: Arc<Session>,
    pub loader: Arc<Loader>,
    pub hlc: Arc<HLC>,
    pub runtime_name: RuntimeId,
    pub runtime_uuid: Uuid,
}

/// The context of a Zenoh Flow graph instance.
#[derive(Clone)]
pub struct InstanceContext {
    pub flow_id: FlowId,
    pub instance_id: Uuid,
    pub runtime: RuntimeContext,
}

/// This function maps a [`FlattenDataFlowDescriptor`](`FlattenDataFlowDescriptor`) into
/// the infrastructure.
/// The initial implementation simply maps all missing mapping
/// to the provided runtime.
///
/// # Errors
/// An error variant is returned in case of:
/// - unable to map node to infrastructure
pub async fn map_to_infrastructure(
    mut descriptor: FlattenDataFlowDescriptor,
    runtime: &str,
) -> ZFResult<FlattenDataFlowDescriptor> {
    log::debug!("[Dataflow mapping] Begin mapping for: {}", descriptor.flow);

    let runtime_id: Arc<str> = runtime.into();

    // Initial "stupid" mapping, if an operator is not mapped, we map to the local runtime.
    // function is async because it could involve other nodes.
    let mut mapping = descriptor.mapping.clone().map_or(HashMap::new(), |m| m);

    for o in &descriptor.operators {
        mapping
            .entry(o.id.clone())
            .or_insert_with(|| runtime_id.clone());
    }

    for o in &descriptor.sources {
        mapping
            .entry(o.id.clone())
            .or_insert_with(|| runtime_id.clone());
    }

    for o in &descriptor.sinks {
        mapping
            .entry(o.id.clone())
            .or_insert_with(|| runtime_id.clone());
    }

    descriptor.mapping = Some(mapping);
    Ok(descriptor)
}

// Runtime related types, maybe can be moved.

/// Runtime Status, either Ready or Not Ready.
/// When Ready it is able to accept commands and instantiate graphs.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeStatusKind {
    Ready,
    NotReady,
}

/// The Runtime information.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RuntimeInfo {
    pub id: Uuid,
    pub name: Arc<str>,
    pub tags: Vec<String>,
    pub status: RuntimeStatusKind,
    // Do we need/want also RAM usage?
}

/// The detailed runtime status.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RuntimeStatus {
    pub id: Uuid,
    pub status: RuntimeStatusKind,
    pub running_flows: usize,
    pub running_operators: usize,
    pub running_sources: usize,
    pub running_sinks: usize,
    pub running_connectors: usize,
}

/// Wrapper for Zenoh kind.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ZenohConfigKind {
    Peer,
    Client,
}

impl std::fmt::Display for ZenohConfigKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ZenohConfigKind::Peer => write!(f, "peer"),
            ZenohConfigKind::Client => write!(f, "client"),
        }
    }
}

impl TryFrom<zenoh::config::whatami::WhatAmI> for ZenohConfigKind {
    type Error = ZFError;
    fn try_from(value: zenoh::config::whatami::WhatAmI) -> Result<Self, Self::Error> {
        match value {
            zenoh::config::whatami::WhatAmI::Client => Ok(Self::Client),
            zenoh::config::whatami::WhatAmI::Peer => Ok(Self::Peer),
            _ => Err(ZFError::MissingConfiguration),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<zenoh::config::whatami::WhatAmI> for ZenohConfigKind {
    fn into(self) -> zenoh::config::whatami::WhatAmI {
        match self {
            Self::Peer => zenoh::config::whatami::WhatAmI::Peer,
            Self::Client => zenoh::config::whatami::WhatAmI::Client,
        }
    }
}

/// The runtime configuration.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RuntimeConfig {
    pub pid_file: String, //Where the PID file resides
    pub path: String,     //Where the libraries are downloaded/located
    pub name: String,
    pub uuid: Uuid,
    pub zenoh: ZenohConfig,
    pub loader: LoaderConfig,
}

/// The interface the Runtime expose to a client
/// (eg. another runtime, the cli, the mgmt API)[^note]
/// The service is exposed using zenoh-rpc, the server and client
/// are generated automatically.
///
/// [^note]: We may split this interface in the future.
#[znservice(
    timeout_s = 60,
    prefix = "/zf/runtime",
    service_uuid = "00000000-0000-0000-0000-000000000001"
)]
pub trait Runtime {
    /// Creates the instance (`DataFlowRecord`) from the
    /// given [`FlattenDataFlowDescriptor`][^note].
    ///
    /// This function:
    /// 1) Generates the instance `Uuid`
    /// 2) Flattens the descriptor
    /// 3) Maps the flow into the infrastructure
    /// 4) Creates the associated record
    /// 5) Stores the record in Zenoh
    /// 6) Prepares all the involved runtimes to host the data flow instance
    ///
    /// Returns the `DataFlowRecord` associted with the instance
    ///
    /// [^note]: When the registry will be in place it will take
    /// the Flow identifier as parameter
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - error on zenoh-rpc
    /// - unable to map
    /// - unable to prepare nodes
    async fn create_instance(&self, flow: FlattenDataFlowDescriptor) -> ZFResult<DataFlowRecord>;
    //TODO: workaround - it should just take the ID of the flow (when
    // the registry will be in place)

    /// Deletes the given instance].
    /// This function:
    /// 1) Cleans the instance nodes from all the involved runtimes.
    /// 2) Deletes the record from zenoh
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - error on zenoh-rpc
    /// - instance not stopped
    /// - unable to clean
    /// - zenoh error
    async fn delete_instance(&self, record_id: Uuid) -> ZFResult<DataFlowRecord>;

    /// Instantiates the given [`FlattenDataFlowDescriptor`][^note].
    /// and, creates the associated [`DataFlowRecord`].
    /// The record contains an [`Uuid`] that identifies the record.
    /// The actual instantiation process runs asynchronously in the runtime.
    ///
    /// It is equivalent to calling `create_instance` and
    /// then `start_instance`.
    ///
    /// [^note]: When the registry will be in place it will take
    /// the Flow identifier as parameter
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - error on zenoh-rpc
    /// - unable to instantiate
    async fn instantiate(&self, flow: FlattenDataFlowDescriptor) -> ZFResult<DataFlowRecord>;
    //TODO: workaround - it should just take the ID of the flow (when
    // the registry will be in place)

    /// Sends a teardown request for the given record identified by the [`Uuid`]
    /// Note the request is asynchronous, the runtime that receives the request will
    /// return immediately, but the teardown process will run asynchronously in the runtime.
    ///
    /// It is equivalent to calling `stop_instance` and then `delete_delete`.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - error on zenoh-rpc
    /// - unable to teardown
    /// - instance not found
    async fn teardown(&self, record_id: Uuid) -> ZFResult<DataFlowRecord>;

    /// Prepares the runtime host the instance identified by the [`Uuid`].
    /// Preparing a runtime means, fetch the operators/source/sinks libraries,
    /// create the needed structures in memory, the links.
    /// Once everything is prepared the runtime should return the [`DataFlowRecord`]
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - error on zenoh-rpc
    /// - unable to prepare
    async fn prepare(&self, record_id: Uuid) -> ZFResult<DataFlowRecord>;

    /// Cleans the runtime from the remains of the given record.
    /// Cleans means unload the libraries, drop data structures and destroy links.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - error on zenoh-rpc
    /// - unable to clean
    async fn clean(&self, record_id: Uuid) -> ZFResult<DataFlowRecord>;

    /// Starts the instance on all involved nodes.
    ///
    /// It first starts all the nodes and then the sources.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - error on zenoh-rpc
    /// - record not found
    /// - record already started
    async fn start_instance(&self, record_id: Uuid) -> ZFResult<()>;

    /// Stops the instance on all involved nodes.
    ///
    /// It first stops the sources then the other nodes.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - error on zenoh-rpc
    /// - unable to clean
    async fn stop_instance(&self, record_id: Uuid) -> ZFResult<DataFlowRecord>;

    /// Starts the sinks, connectors, and operators for the given record.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - error on zenoh-rpc
    /// - record not found
    /// - record already started
    async fn start(&self, record_id: Uuid) -> ZFResult<()>;

    /// Starts the sources for the given record.
    /// Note that this should be called only after the `start(record)` has returned
    /// successfully otherwise data may be lost.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - error on zenoh-rpc
    /// - record not found
    /// - sources already started
    async fn start_sources(&self, record_id: Uuid) -> ZFResult<()>;

    /// Stops the sinks, connectors, and operators for the given record.
    /// Note that this should be called after the `stop_sources(record)` has returned
    /// successfully otherwise data may be lost.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - error on zenoh-rpc
    /// - record not found
    /// - record already stopped
    async fn stop(&self, record_id: Uuid) -> ZFResult<()>;

    /// Stops the sources for the given record.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - error on zenoh-rpc
    /// - record not found
    /// - sources already stopped
    async fn stop_sources(&self, record_id: Uuid) -> ZFResult<()>;

    /// Starts the given graph node for the given instance.
    /// A graph node can be a source, a sink, a connector, or an operator.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - error on zenoh-rpc
    /// - record not found
    /// - node already started
    /// - node not found
    async fn start_node(&self, record_id: Uuid, node: String) -> ZFResult<()>;

    /// Stops the given graph node from the given instance.
    /// A graph node can be a source, a sink, a connector, or an operator.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - error on zenoh-rpc
    /// - record not found
    /// - node already started
    /// - node not found
    async fn stop_node(&self, record_id: Uuid, node: String) -> ZFResult<()>;

    // FIXME A source now has several outputs.

    // /// Start a recording for the given source.
    // ///
    // /// # Errors
    // /// An error variant is returned in case of:
    // /// - error on zenoh-rpc
    // /// - record not found
    // /// - record already started
    // /// - source not found
    // /// - node is not a source
    // async fn start_record(&self, instance_id: Uuid, source_id: NodeId) -> ZFResult<String>;

    // /// Stops the recording for the given source.
    // ///
    // /// # Errors
    // /// An error variant is returned in case of:
    // /// - error on zenoh-rpc
    // /// - record not found
    // /// - record already stopped
    // /// - source not found
    // /// - node is not a source
    // async fn stop_record(&self, instance_id: Uuid, source_id: NodeId) -> ZFResult<String>;

    // /// Starts the replay for the given source.
    // /// The replay creates a new node that has the same port and links as the
    // /// source is replaying.
    // ///
    // /// # Errors
    // /// An error variant is returned in case of:
    // /// - error on zenoh-rpc
    // /// - record not found
    // /// - replay already started
    // /// - source not found
    // /// - node is not a source
    // async fn start_replay(
    //     &self,
    //     instance_id: Uuid,
    //     source_id: NodeId,
    //     key_expr: String,
    // ) -> ZFResult<NodeId>;

    // /// Stops the replay for the given source.
    // /// This stops and removes the replay node from the graph.
    // ///
    // /// # Errors
    // /// An error variant is returned in case of:
    // /// - error on zenoh-rpc
    // /// - record not found
    // /// - replay already stopped
    // /// - source not found
    // /// - node is not a source
    // async fn stop_replay(
    //     &self,
    //     instance_id: Uuid,
    //     source_id: NodeId,
    //     replay_id: NodeId,
    // ) -> ZFResult<NodeId>;

    /// Gets the state of the given graph node for the given instance.
    /// A graph node can be a source, a sink, a connector, or an operator.
    /// The node state represents the current state of the node:
    /// `enum NodeState { Running, Stopped, Error(err) }`
    // async fn get_node_state(&self, record_id: Uuid, node: String) -> ZFResult<NodeState>;

    /// Sends the `message` to `node` for the given record.
    /// This is useful for sending out-of-band notification to a node.
    /// eg. in the case of deadline miss notification.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - error on zenoh-rpc
    /// - record not found
    async fn notify_runtime(
        &self,
        record_id: Uuid,
        runtime: String,
        message: ControlMessage,
    ) -> ZFResult<()>;

    /// Checks the compatibility for the given `operator`
    /// Compatibility is based on tags and some machine characteristics (eg. CPU architecture, OS)
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - error on zenoh-rpc
    async fn check_operator_compatibility(
        &self,
        operator: SimpleOperatorDescriptor,
    ) -> ZFResult<bool>;

    /// Checks the compatibility for the given `source`
    /// Compatibility is based on tags and some machine characteristics (eg. CPU architecture, OS)
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - error on zenoh-rpc
    async fn check_source_compatibility(&self, source: SourceDescriptor) -> ZFResult<bool>;

    /// Checks the compatibility for the given `sink`
    /// Compatibility is based on tags and some machine characteristics (eg. CPU architecture, OS)
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - error on zenoh-rpc
    async fn check_sink_compatibility(&self, sink: SinkDescriptor) -> ZFResult<bool>;
}

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

#![allow(clippy::manual_async_fn)]

use std::sync::Arc;

use crate::{
    model::{
        component::{OperatorDescriptor, SinkDescriptor, SourceDescriptor},
        dataflow::DataFlowRecord,
    },
    serde::{Deserialize, Serialize},
};
use uuid::Uuid;

use crate::{
    model::dataflow::{DataFlowDescriptor, Mapping},
    ZFResult,
};

use crate::runtime::message::ControlMessage;

use znrpc_macros::znservice;
use zrpc::zrpcresult::{ZRPCError, ZRPCResult};

// zrpc required modules
// use std::convert::TryFrom;
// use futures::prelude::*;
// use async_std::prelude::FutureExt;
// use zenoh::*;
//

pub mod graph;
pub mod loader;
pub mod message;
pub mod resources;
pub mod runners;

pub async fn map_to_infrastructure(
    mut descriptor: DataFlowDescriptor,
    runtime: &str,
) -> ZFResult<DataFlowDescriptor> {
    log::debug!("[Dataflow mapping] Begin mapping for: {}", descriptor.flow);

    let runtime_id: Arc<str> = runtime.into();

    // Initial "stupid" mapping, if an operator is not mapped, we map to the local runtime.
    // function is async because it could involve other nodes.

    let mut mappings = Vec::new();

    for o in &descriptor.operators {
        match descriptor.get_mapping(&o.id) {
            Some(_) => (),
            None => {
                let mapping = Mapping {
                    id: o.id.clone(),
                    runtime: runtime_id.clone(),
                };
                mappings.push(mapping);
            }
        }
    }

    for o in &descriptor.sources {
        match descriptor.get_mapping(&o.id) {
            Some(_) => (),
            None => {
                let mapping = Mapping {
                    id: o.id.clone(),
                    runtime: runtime_id.clone(),
                };
                mappings.push(mapping);
            }
        }
    }

    for o in &descriptor.sinks {
        match descriptor.get_mapping(&o.id) {
            Some(_) => (),
            None => {
                let mapping = Mapping {
                    id: o.id.clone(),
                    runtime: runtime_id.clone(),
                };
                mappings.push(mapping);
            }
        }
    }

    for m in mappings {
        descriptor.add_mapping(m)
    }

    Ok(descriptor)
}

// Runtime related types, maybe can be moved.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeStatusKind {
    Ready,
    NotReady,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RuntimeInfo {
    pub id: Uuid,
    pub name: Arc<str>,
    pub tags: Vec<String>,
    pub status: RuntimeStatusKind,
    // Do we need/want also RAM usage?
}

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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZenohConfig {
    pub kind: ZenohConfigKind, // whether the runtime is a peer or a client
    pub listen: Vec<String>,   // if the runtime is a peer, where it listens
    pub locators: Vec<String>, // where to connect (eg. a router if the runtime is a client, or other peers/routers if the runtime is a peer)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFRuntimeConfig {
    pub pid_file: String, //Where the PID file resides
    pub path: String,     //Where the libraries are downloaded/located
    pub name: Option<String>,
    pub uuid: Option<Uuid>,
    pub zenoh: ZenohConfig,
}

/// The interface the Runtime expose to a client
/// (eg. another runtime, the cli, the mgmt API)
#[znservice(
    timeout_s = 60,
    prefix = "/zf/runtime",
    service_uuid = "00000000-0000-0000-0000-000000000001"
)]
pub trait ZFRuntime {
    /// Sends an initiation request for the given [`FlowId`]
    /// Note the request is asynchronous, the runtime that receives the request
    /// flattens the descriptor, maps it to the infrastructure,
    /// and, creates the associated [`DataFlowRecord`].
    /// The record contains an [`Uuid`] that identifies the record.
    /// The actual instantiation process runs asynchronously in the runtime.
    async fn instantiate(&self, flow: DataFlowDescriptor) -> ZFResult<DataFlowRecord>; //TODO: workaround - it should just take the ID of the flow...

    /// Sends a teardown request for the given record identified by the [`Uuid`]
    /// Note the request is asynchronous, the runtime that receives the request will
    /// return immediately, but the teardown process will run asynchronously in the runtime.
    async fn teardown(&self, record_id: Uuid) -> ZFResult<DataFlowRecord>;

    /// Prepares the runtime host the instance identified [`Uuid`] for the Flow identified by [`FlowId`].
    /// Preparing a runtime means, fetch the operators/source/sinks libraries,
    /// create the needed structures in memory, the links.
    /// Once everything is prepared the runtime should return the [`DataFlowRecord`]
    async fn prepare(&self, flow: DataFlowDescriptor, record_id: Uuid) -> ZFResult<DataFlowRecord>; //TODO: workaround - it should just take the ID of the flow...

    /// Cleans the runtime from the remains of the given record.
    /// Cleans means unload the libraries, drop data structures and destroy links.
    async fn clean(&self, record_id: Uuid) -> ZFResult<DataFlowRecord>;

    /// Starts the sinks, connectors, and operators for the given record.
    async fn start(&self, record_id: Uuid) -> ZFResult<()>;

    /// Starts the sources for the given record.
    /// Note that this should be called only after the `start(record)` has returned
    /// successfully otherwise data may be lost.
    async fn start_sources(&self, record_id: Uuid) -> ZFResult<()>;

    /// Stops the sinks, connectors, and operators for the given record.
    /// Note that this should be called after the `stop_sources(record)` has returned
    /// successfully otherwise data may be lost.
    async fn stop(&self, record_id: Uuid) -> ZFResult<()>;

    /// Stops the sources for the given record.
    async fn stop_sources(&self, record_id: Uuid) -> ZFResult<()>;

    /// Starts the given graph node for the given instance.
    /// A graph node can be a source, a sink, a connector, or an operator.
    async fn start_node(&self, record_id: Uuid, node: String) -> ZFResult<()>;

    /// Stops the given graph node from the given instance.
    /// A graph node can be a source, a sink, a connector, or an operator.
    async fn stop_node(&self, record_id: Uuid, node: String) -> ZFResult<()>;

    /// Gets the state of the given graph node for the given instance.
    /// A graph node can be a source, a sink, a connector, or an operator.
    /// The node state represents the current state of the node:
    /// `enum ComponentState { Running, Stopped, Error(err) }`
    // async fn get_node_state(&self, record_id: Uuid, node: String) -> ZFResult<ComponentState>;

    /// Sends the `message` to `node` for the given record.
    /// This is useful for sending out-of-band notification to a node.
    /// eg. in the case of deadline miss notification.
    async fn notify_runtime(
        &self,
        record_id: Uuid,
        runtime: String,
        message: ControlMessage,
    ) -> ZFResult<()>;

    /// Checks the compatibility for the given `operator`
    /// Compatibility is based on tags and some machine characteristics (eg. CPU architecture, OS)
    async fn check_operator_compatibility(&self, operator: OperatorDescriptor) -> ZFResult<bool>;

    /// Checks the compatibility for the given `source`
    /// Compatibility is based on tags and some machine characteristics (eg. CPU architecture, OS)
    async fn check_source_compatibility(&self, source: SourceDescriptor) -> ZFResult<bool>;

    /// Checks the compatibility for the given `sink`
    /// Compatibility is based on tags and some machine characteristics (eg. CPU architecture, OS)
    async fn check_sink_compatibility(&self, sink: SinkDescriptor) -> ZFResult<bool>;
}

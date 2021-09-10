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

use uuid::Uuid;
use zenoh_flow::async_std::sync::Arc;
use zenoh_flow::{
    model::{
        dataflow::DataFlowDescriptor,
        operator::{ZFOperatorDescriptor, ZFSinkDescriptor, ZFSourceDescriptor},
        ZFRegistryGraph,
    },
    ZFResult,
};
use znrpc_macros::znservice;
use zrpc::zrpcresult::{ZRPCError, ZRPCResult};

use zenoh_fragmentation_e2e::{PUTApiArgs, ZenohCdn};

pub mod config;
pub mod registry;

#[derive(Debug)]
pub enum CZFError {
    PackageNotFoundInWorkspace(String, String),
    NoRootFoundInWorkspace(String),
    CrateTypeNotCompatible(String),
    CommandFailed(std::io::Error, &'static str),
    CommandError(&'static str, String, Vec<u8>),
    ParseTOML(toml::de::Error),
    ParseJSON(serde_json::Error),
    ParseYAML(serde_yaml::Error),
    MissingField(String, &'static str),
    IoFile(&'static str, std::io::Error, std::path::PathBuf),
    ParsingError(&'static str),
    BuildFailed,
    ZenohError(zenoh::ZError),
}

impl From<toml::de::Error> for CZFError {
    fn from(err: toml::de::Error) -> Self {
        Self::ParseTOML(err)
    }
}

impl From<serde_json::Error> for CZFError {
    fn from(err: serde_json::Error) -> Self {
        Self::ParseJSON(err)
    }
}

impl From<serde_yaml::Error> for CZFError {
    fn from(err: serde_yaml::Error) -> Self {
        Self::ParseYAML(err)
    }
}

impl From<zenoh::ZError> for CZFError {
    fn from(err: zenoh::ZError) -> Self {
        Self::ZenohError(err)
    }
}

impl std::fmt::Display for CZFError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub type CZFResult<T> = Result<T, CZFError>;

#[znservice(
    timeout_s = 60,
    prefix = "/zf/registry",
    service_uuid = "00000000-0000-0000-0000-000000000002"
)]
pub trait ZFRegistry {
    async fn get_flow(&self, flow_id: String) -> ZFResult<DataFlowDescriptor>;

    //async fn get_graph(&self, graph_id: String) -> ZFResult<GraphDescriptor>;

    async fn get_all_graphs(&self) -> ZFResult<Vec<ZFRegistryGraph>>;

    async fn get_operator(
        &self,
        operator_id: String,
        tag: Option<String>,
        os: String,
        arch: String,
    ) -> ZFResult<ZFOperatorDescriptor>;

    async fn get_sink(&self, sink_id: String, tag: Option<String>) -> ZFResult<ZFSinkDescriptor>;

    async fn get_source(
        &self,
        source_id: String,
        tag: Option<String>,
    ) -> ZFResult<ZFSourceDescriptor>;

    async fn remove_flow(&self, flow_id: String) -> ZFResult<DataFlowDescriptor>;

    // async fn remove_graph(&self, graph_id: String) -> ZFResult<GraphDescriptor>;

    async fn remove_operator(
        &self,
        operator_id: String,
        tag: Option<String>,
    ) -> ZFResult<ZFOperatorDescriptor>;

    async fn remove_sink(&self, sink_id: String, tag: Option<String>)
        -> ZFResult<ZFSinkDescriptor>;

    async fn remove_source(
        &self,
        source_id: String,
        tag: Option<String>,
    ) -> ZFResult<ZFSourceDescriptor>;

    async fn add_flow(&self, flow: DataFlowDescriptor) -> ZFResult<String>;

    async fn add_graph(&self, graph: ZFRegistryGraph) -> ZFResult<String>;

    async fn add_operator(
        &self,
        operator: ZFOperatorDescriptor,
        tag: Option<String>,
    ) -> ZFResult<String>;

    async fn add_sink(&self, sink: ZFSinkDescriptor, tag: Option<String>) -> ZFResult<String>;

    async fn add_source(&self, source: ZFSourceDescriptor, tag: Option<String>)
        -> ZFResult<String>;
}

#[derive(Clone)]
pub struct ZFRegistryFileClient {
    pub zcdn: ZenohCdn,
}

impl ZFRegistryFileClient {
    pub async fn send_component(_path: &Path, _metadata: &ZFRegistryGraph) -> CZFResult<()> {
        Ok(())
    }

    pub async fn get_component(_component_id: String, _path: &Path) -> CZFResult<()> {
        Ok(())
    }
}

impl From<Arc<zenoh::Zenoh>> for ZFRegistryFileClient {
    fn from(zenoh: Arc<zenoh::Zenoh>) -> Self {
        let mut zcdn = ZenohCdn::new(zenoh);
        zcdn.set_upload_args(PUTApiArgs {
            chunk_size: 65000usize,
        }); //TODO should not be hardcoded

        Self { zcdn }
    }
}

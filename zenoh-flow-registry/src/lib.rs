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

use std::path::Path;
use uuid::Uuid;
use zenoh::Path as ZPath;
use zenoh_cdn::client::Client;
use zenoh_flow::async_std::sync::Arc;
use zenoh_flow::model::RegistryComponentArchitecture;
use zenoh_flow::OperatorId;
use zenoh_flow::{
    model::{
        component::{OperatorDescriptor, SinkDescriptor, SourceDescriptor},
        dataflow::DataFlowDescriptor,
        RegistryGraph,
    },
    ZFResult,
};
use znrpc_macros::znservice;
use zrpc::zrpcresult::{ZRPCError, ZRPCResult};

pub mod config;
pub mod registry;
pub mod templates;

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
    GenericError(String),
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
pub trait Registry {
    async fn get_flow(&self, flow_id: OperatorId) -> ZFResult<DataFlowDescriptor>;

    //async fn get_graph(&self, graph_id: String) -> ZFResult<GraphDescriptor>;

    async fn get_all_graphs(&self) -> ZFResult<Vec<RegistryGraph>>;

    async fn get_operator(
        &self,
        operator_id: OperatorId,
        tag: Option<String>,
        os: String,
        arch: String,
    ) -> ZFResult<OperatorDescriptor>;

    async fn get_sink(&self, sink_id: OperatorId, tag: Option<String>) -> ZFResult<SinkDescriptor>;

    async fn get_source(
        &self,
        source_id: OperatorId,
        tag: Option<String>,
    ) -> ZFResult<SourceDescriptor>;

    async fn remove_flow(&self, flow_id: OperatorId) -> ZFResult<DataFlowDescriptor>;

    // async fn remove_graph(&self, graph_id: String) -> ZFResult<GraphDescriptor>;

    async fn remove_operator(
        &self,
        operator_id: OperatorId,
        tag: Option<String>,
    ) -> ZFResult<OperatorDescriptor>;

    async fn remove_sink(
        &self,
        sink_id: OperatorId,
        tag: Option<String>,
    ) -> ZFResult<SinkDescriptor>;

    async fn remove_source(
        &self,
        source_id: OperatorId,
        tag: Option<String>,
    ) -> ZFResult<SourceDescriptor>;

    async fn add_flow(&self, flow: DataFlowDescriptor) -> ZFResult<OperatorId>;

    async fn add_graph(&self, graph: RegistryGraph) -> ZFResult<OperatorId>;

    async fn add_operator(
        &self,
        operator: OperatorDescriptor,
        tag: Option<String>,
    ) -> ZFResult<OperatorId>;

    async fn add_sink(&self, sink: SinkDescriptor, tag: Option<String>) -> ZFResult<OperatorId>;

    async fn add_source(
        &self,
        source: SourceDescriptor,
        tag: Option<String>,
    ) -> ZFResult<OperatorId>;
}

#[derive(Clone)]
pub struct RegistryFileClient {
    pub zcdn: Client,
}

impl RegistryFileClient {
    pub async fn send_component(
        &self,
        path: &Path,
        id: &str,
        arch: &RegistryComponentArchitecture,
        tag: &str,
    ) -> CZFResult<()> {
        let resource_name =
            ZPath::try_from(format!("/{}/{}/{}/{}/library", id, tag, arch.os, arch.arch))?;
        Ok(self.zcdn.upload(path, &resource_name).await?)
    }

    pub async fn get_component(_component_id: String, _path: &Path) -> CZFResult<()> {
        Ok(())
    }
}

impl From<Arc<zenoh::Zenoh>> for RegistryFileClient {
    fn from(zenoh: Arc<zenoh::Zenoh>) -> Self {
        let zcdn = Client::new(zenoh);
        Self { zcdn }
    }
}

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

use uuid::Uuid;
use zenoh_flow::{
    model::{
        dataflow::DataFlowDescriptor,
        link::ZFPortDescriptor,
        operator::{ZFOperatorDescriptor, ZFSinkDescriptor, ZFSourceDescriptor},
        period::ZFPeriodDescriptor,
    },
    serde::{Deserialize, Serialize},
    ZFResult,
};
use znrpc_macros::znservice;
use zrpc::zrpcresult::{ZRPCError, ZRPCResult};

pub mod config;

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

impl std::fmt::Display for CZFError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub type CZFResult<T> = Result<T, CZFError>;

#[derive(Debug, Serialize, Deserialize)]
pub struct ZFRegistryGraph {
    pub id: String,
    pub classes: Vec<String>,
    pub tags: Vec<ZFRegistryComponentTag>,
    pub inputs: Vec<ZFPortDescriptor>,
    pub outputs: Vec<ZFPortDescriptor>,
    pub period: Option<ZFPeriodDescriptor>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ZFRegistryComponentTag {
    pub name: String,
    pub requirement_labels: Vec<String>,
    pub architectures: Vec<ZFRegistryComponentArchitecture>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ZFRegistryComponentArchitecture {
    pub arch: String,
    pub os: String,
    pub uri: String,
    pub checksum: String,
    pub signature: String,
}

#[znservice(
    timeout_s = 60,
    prefix = "/zf/registry",
    service_uuid = "00000000-0000-0000-0000-000000000002"
)]
pub trait ZFRegistry {
    async fn get_flow(&self, flow_id: String) -> ZFResult<DataFlowDescriptor>;

    //async fn get_graph(&self, graph_id: String) -> ZFResult<GraphDescriptor>;

    async fn get_operator(
        &self,
        operator_id: String,
        tag: Option<String>,
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

    // async fn add_graph(&self, graph: GraphDescriptor) -> ZFResult<String>;

    async fn add_operator(
        &self,
        operator: ZFOperatorDescriptor,
        tag: Option<String>,
    ) -> ZFResult<String>;

    async fn add_sink(&self, sink: ZFSinkDescriptor, tag: Option<String>) -> ZFResult<String>;

    async fn add_source(&self, source: ZFSourceDescriptor, tag: Option<String>)
        -> ZFResult<String>;
}

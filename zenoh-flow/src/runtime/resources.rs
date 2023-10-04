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

#![allow(unused)]

#[cfg(feature = "data_bincode")]
extern crate bincode;

#[cfg(feature = "data_cbor")]
extern crate serde_cbor;

#[cfg(feature = "data_json")]
extern crate serde_json;

use crate::model::record::DataFlowRecord;
use crate::model::registry::RegistryNode;
use crate::runtime::{RuntimeConfig, RuntimeInfo, RuntimeStatus};
use crate::zfresult::ErrorKind;
use crate::Result;
use crate::{bail, zferror};
use async_std::pin::Pin;
use async_std::stream::Stream;
use async_std::task::{Context, Poll};
use futures::StreamExt;
use futures_lite::FutureExt;
use pin_project_lite::pin_project;
use serde::{de::DeserializeOwned, Serialize};
use std::convert::TryFrom;
use std::sync::Arc;
use uhlc::HLC;
use uuid::Uuid;
use zenoh::prelude::r#async::*;
use zenoh::query::Reply;

use super::Job;

//NOTE: this should be pub(crate)

/// Root prefix for key expressions when running as a router plugin.
pub static ROOT_PLUGIN_RUNTIME_PREFIX: &str = "@/router/";
/// Root suffix for key expression when running as router plugin.
pub static ROOT_PLUGIN_RUNTIME_SUFFIX: &str = "plugin/zenoh-flow";
/// Root for key expression when running as standalone.
pub static ROOT_STANDALONE: &str = "zenoh-flow";

/// Token for the runtime in the key expression.
pub static KEY_RUNTIMES: &str = "runtimes";
/// Token for the registry in the key expression.
pub static KEY_REGISTRY: &str = "registry";

/// TOken for the flow in the key expression.
pub static KEY_FLOWS: &str = "flows";
/// Token for the graphs in the key expression.
pub static KEY_GRAPHS: &str = "graphs";

/// Token for the leaf with information in the key expression.
pub static KEY_INFO: &str = "info";
/// Token for the leaf with status information in the key expression/
pub static KEY_STATUS: &str = "status";
/// Token for the leaf with configuration in the key expression.
pub static KEY_CONFIGURATION: &str = "configuration";

/// Token for job queue in the key expression.
pub static KEY_JOB_QUEUE: &str = "job-queue";

/// Token for the submitted jobs job queue in the key expression.
pub static KEY_JOB_SUBMITTED: &str = "sumbitted";

/// Token for the started jobs job queue in the key expression.
pub static KEY_JOB_STARTED: &str = "started";

/// Token for the done jobs job queue in the key expression.
pub static KEY_JOB_DONE: &str = "done";

/// Token for the failed jobs job queue in the key expression.
pub static KEY_JOB_FAILED: &str = "failed";

/// Generates the runtime info key expression.
#[macro_export]
macro_rules! RT_INFO_PATH {
    ($prefix:expr, $rtid:expr) => {
        format!(
            "{}/{}/{}/{}",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $rtid,
            $crate::runtime::resources::KEY_INFO
        )
    };
}

/// Generates the runtime status key expression.
#[macro_export]
macro_rules! RT_STATUS_PATH {
    ($prefix:expr, $rtid:expr) => {
        format!(
            "{}/{}/{}/{}",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $rtid,
            $crate::runtime::resources::KEY_STATUS
        )
    };
}
/// Generates the runtime configuration key expression.
#[macro_export]
macro_rules! RT_CONFIGURATION_PATH {
    ($prefix:expr, $rtid:expr) => {
        format!(
            "{}/{}/{}/{}",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $rtid,
            $crate::runtime::resources::KEY_CONFIGURATION
        )
    };
}

/// Generates the flow instance key expression.
#[macro_export]
macro_rules! RT_FLOW_PATH {
    ($prefix:expr, $rtid:expr, $fid:expr, $iid:expr) => {
        format!(
            "{}/{}/{}/{}/{}/{}",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $rtid,
            $crate::runtime::resources::KEY_FLOWS,
            $fid,
            $iid
        )
    };
}

/// Generates the flow selector by instance id.
#[macro_export]
macro_rules! RT_FLOW_SELECTOR_BY_INSTANCE {
    ($prefix:expr, $rtid:expr, $iid:expr) => {
        format!(
            "{}/{}/{}/{}/*/{}",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $rtid,
            $crate::runtime::resources::KEY_FLOWS,
            $iid
        )
    };
}

/// Generates the flow selector by flow id.
#[macro_export]
macro_rules! RT_FLOW_SELECTOR_BY_FLOW {
    ($prefix:expr, $rtid:expr, $fid:expr) => {
        format!(
            "{}/{}/{}/{}/{}/*",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $rtid,
            $crate::runtime::resources::KEY_FLOWS,
            $fid
        )
    };
}

/// Generate the selector for all flows.
#[macro_export]
macro_rules! RT_FLOW_SELECTOR_ALL {
    ($prefix:expr, $rtid:expr) => {
        format!(
            "{}/{}/{}/{}/*/*",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $rtid,
            $crate::runtime::resources::KEY_FLOWS
        )
    };
}

/// Generates the flow selector by instance, for all runtimes.
#[macro_export]
macro_rules! FLOW_SELECTOR_BY_INSTANCE {
    ($prefix:expr, $iid:expr) => {
        format!(
            "{}/{}/*/{}/*/{}",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $crate::runtime::resources::KEY_FLOWS,
            $iid
        )
    };
}
/// Generates the flow selector by flow, for all runtimes.
#[macro_export]
macro_rules! FLOW_SELECTOR_BY_FLOW {
    ($prefix:expr, $fid:expr) => {
        format!(
            "{}/{}/*/{}/{}/*",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $crate::runtime::resources::KEY_FLOWS,
            $fid
        )
    };
}

/// Generates the graph key expression.
#[macro_export]
macro_rules! REG_GRAPH_SELECTOR {
    ($prefix:expr, $fid:expr) => {
        format!(
            "{}/{}/{}/{}",
            $prefix,
            $crate::runtime::resources::KEY_REGISTRY,
            $crate::runtime::resources::KEY_GRAPHS,
            $fid
        )
    };
}

/// Generates the sumbitted jobs key expression (selector)
#[macro_export]
macro_rules! JQ_SUMBITTED_SEL {
    ($prefix:expr, $rid:expr) => {
        format!(
            "{}/{}/{}/{}/{}/*",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $rid,
            $crate::runtime::resources::KEY_JOB_QUEUE,
            $crate::runtime::resources::KEY_JOB_SUBMITTED
        )
    };
}

/// Generates the sumbitted job key expression
#[macro_export]
macro_rules! JQ_SUMBITTED_JOB {
    ($prefix:expr, $rid:expr, $jid: expr) => {
        format!(
            "{}/{}/{}/{}/{}/{}",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $rid,
            $crate::runtime::resources::KEY_JOB_QUEUE,
            $crate::runtime::resources::KEY_JOB_SUBMITTED,
            $jid
        )
    };
}

/// Generates the started job key expression
#[macro_export]
macro_rules! JQ_STARTED_JOB {
    ($prefix:expr, $rid:expr, $jid: expr) => {
        format!(
            "{}/{}/{}/{}/{}/{}",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $rid,
            $crate::runtime::resources::KEY_JOB_QUEUE,
            $crate::runtime::resources::KEY_JOB_STARTED,
            $jid
        )
    };
}

/// Generates the done job key expression
#[macro_export]
macro_rules! JQ_DONE_JOB {
    ($prefix:expr, $rid:expr, $jid: expr) => {
        format!(
            "{}/{}/{}/{}/{}/{}",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $rid,
            $crate::runtime::resources::KEY_JOB_QUEUE,
            $crate::runtime::resources::KEY_JOB_DONE,
            $jid
        )
    };
}

/// Generates the done job key expression
#[macro_export]
macro_rules! JQ_FAILED_JOB {
    ($prefix:expr, $rid:expr, $jid: expr) => {
        format!(
            "{}/{}/{}/{}/{}/{}",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $rid,
            $crate::runtime::resources::KEY_JOB_QUEUE,
            $crate::runtime::resources::KEY_JOB_FAILED,
            $jid
        )
    };
}

/// Deserialize data from Zenoh storage.
/// The format used depends on the features.
/// It can be JSON (default), bincode or CBOR.
///
/// # Errors
/// If it fails to deserialize an error
/// variant will be returned.
pub fn deserialize_data<T>(raw_data: &[u8]) -> Result<T>
where
    T: DeserializeOwned,
{
    #[cfg(feature = "data_bincode")]
    return Ok(bincode::deserialize::<T>(&raw_data)?);

    #[cfg(feature = "data_cbor")]
    return Ok(serde_cbor::from_slice::<T>(&raw_data)?);

    #[cfg(feature = "data_json")]
    return Ok(serde_json::from_str::<T>(std::str::from_utf8(raw_data)?)?);
}

/// Serializes data for zenoh
///
/// # Errors
/// If it fails to serialize an error
/// variant will be returned.
#[cfg(feature = "data_bincode")]

pub fn serialize_data<T: ?Sized>(data: &T) -> FResult<Vec<u8>>
where
    T: Serialize,
{
    Ok(bincode::serialize(data)?)
}

/// Serializes data for zenoh
///
/// # Errors
/// If it fails to serialize an error
/// variant will be returned.
#[cfg(feature = "data_json")]
pub fn serialize_data<T: ?Sized>(data: &T) -> Result<Vec<u8>>
where
    T: Serialize,
{
    Ok(serde_json::to_string(data)?.into_bytes())
}

/// Serializes data for zenoh
///
/// # Errors
/// If it fails to serialize an error
/// variant will be returned.
#[cfg(feature = "data_cbor")]
pub fn serialize_data<T>(data: &T) -> FResult<Vec<u8>>
where
    T: Serialize,
{
    Ok(serde_cbor::to_vec(data)?)
}
//

/// Converts data from Zenoh samples,
/// useful when converting data from a subscriber
///
/// # Errors
/// It can return an error in the following cases
/// - fails to deserialize
/// - the sample is not an APP_OCTET_STREAM
/// - the sample is a Delete
pub fn convert<T>(sample: Sample) -> Result<T>
where
    T: DeserializeOwned,
{
    match sample.kind {
        SampleKind::Put => match sample.value.encoding {
            Encoding::APP_OCTET_STREAM => {
                match deserialize_data::<T>(&sample.value.payload.contiguous()) {
                    Ok(data) => Ok(data),
                    Err(e) => Err(e),
                }
            }
            _ => {
                log::warn!(
                    "Received sample with wrong encoding {:?}, dropping",
                    sample.value.encoding
                );
                Err(zferror!(
                    ErrorKind::DeserializationError,
                    "Received sample with wrong encoding {:?}, dropping",
                    sample.value.encoding
                )
                .into())
            }
        },
        SampleKind::Delete => {
            log::warn!("Received delete sample drop it");
            Err(zferror!(
                ErrorKind::DeserializationError,
                "Received delete sample dropping it"
            )
            .into())
        }
    }
}

/// The `DataStore` provides all the methods to access/store/listen and update
/// all the information stored in zenoh storages.
#[derive(Clone)]
pub struct DataStore {
    //Name TBD
    z: Arc<zenoh::Session>,
}

impl DataStore {
    /// Creates a new `DataStore` from an `Arc<zenoh::Session>`
    pub fn new(z: Arc<zenoh::Session>) -> Self {
        Self { z }
    }

    /// Gets the [`RuntimeInfo`](`RuntimeInfo`) for the given `rtid`.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - no data present in zenoh
    /// - fails to deserialize
    pub async fn get_runtime_info(&self, rtid: &ZenohId) -> Result<RuntimeInfo> {
        let selector = RT_INFO_PATH!(ROOT_STANDALONE, rtid);

        self.get_from_zenoh::<RuntimeInfo>(&selector).await
    }

    /// Gets the  [`RuntimeInfo`](`RuntimeInfo`) for all the runtimes in the
    /// infrastructure
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - no data present in zenoh
    /// - fails to deserialize
    pub async fn get_all_runtime_info(&self) -> Result<Vec<RuntimeInfo>> {
        let selector = RT_INFO_PATH!(ROOT_STANDALONE, "*");

        self.get_vec_from_zenoh::<RuntimeInfo>(&selector).await
    }

    /// Gets the  [`RuntimeInfo`](`RuntimeInfo`) for the runtime with the
    /// given name `rtid`.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - no data present in zenoh
    /// - fails to deserialize
    pub async fn get_runtime_info_by_name(&self, rtid: &str) -> Result<RuntimeInfo> {
        let selector = RT_INFO_PATH!(ROOT_STANDALONE, "*");
        let rts = self.get_vec_from_zenoh::<RuntimeInfo>(&selector).await?;
        for rt in &rts {
            if *rt.name == *rtid {
                return Ok(rt.clone());
            }
        }
        bail!(ErrorKind::NotFound)
    }

    /// Removes the information for the given runtime `rtid`.
    ///
    /// # Errors
    /// If zenoh delete fails an error variant is returned.
    pub async fn remove_runtime_info(&self, rtid: &ZenohId) -> Result<()> {
        let path = RT_INFO_PATH!(ROOT_STANDALONE, rtid);

        self.z.delete(&path).res().await
    }

    /// Stores the given  [`RuntimeInfo`](`RuntimeInfo`) for the given `rtid`
    /// in Zenoh.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - fails to serialize
    /// - zenoh put fails
    pub async fn add_runtime_info(&self, rtid: &ZenohId, rt_info: &RuntimeInfo) -> Result<()> {
        let path = RT_INFO_PATH!(ROOT_STANDALONE, rtid);

        let encoded_info = serialize_data(rt_info)?;
        self.z.put(&path, encoded_info).res().await
    }

    /// Gets [`RuntimeConfig`](`RuntimeConfig`) for the given `rtid`
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - no data present in zenoh
    /// - fails to deserialize
    pub async fn get_runtime_config(&self, rtid: &ZenohId) -> Result<RuntimeConfig> {
        let selector = RT_CONFIGURATION_PATH!(ROOT_STANDALONE, rtid);
        self.get_from_zenoh::<RuntimeConfig>(&selector).await
    }

    /// Subscribes to configuration changes for the given `rtid`
    /// **NOTE:** not implemented.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - zenoh subscribe fails
    /// - fails to deserialize
    pub async fn subscribe_runtime_config(
        &self,
        rtid: &ZenohId,
    ) -> Result<zenoh::subscriber::Subscriber<'static, flume::Receiver<Sample>>> {
        // let selector = RT_CONFIGURATION_PATH!(ROOT_STANDALONE, rtid))?;
        //
        // Ok(self.z
        //     .subscribe(&selector)
        //     .await
        //     .map(|change_stream| ZFRuntimeConfigStream { change_stream })?)
        bail!(ErrorKind::Unimplemented)
    }

    /// Removes the configuration for the given `rtid`.
    ///
    /// # Errors
    /// If zenoh delete fails an error variant is returned.
    pub async fn remove_runtime_config(&self, rtid: &ZenohId) -> Result<()> {
        let path = RT_CONFIGURATION_PATH!(ROOT_STANDALONE, rtid);

        self.z.delete(&path).res().await
    }

    /// Stores the given [`RuntimeConfig`](`RuntimeConfig`) for the given
    /// `rtid` in Zenoh.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - fails to serialize
    /// - zenoh put fails
    pub async fn add_runtime_config(&self, rtid: &ZenohId, rt_info: &RuntimeConfig) -> Result<()> {
        let path = RT_CONFIGURATION_PATH!(ROOT_STANDALONE, rtid);

        let encoded_info = serialize_data(rt_info)?;
        self.z.put(&path, encoded_info).res().await
    }

    /// Gets the `RuntimeStatus` for the given runtime `rtid`.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - no data present in zenoh
    /// - fails to deserialize
    pub async fn get_runtime_status(&self, rtid: &ZenohId) -> Result<RuntimeStatus> {
        let selector = RT_STATUS_PATH!(ROOT_STANDALONE, rtid);
        self.get_from_zenoh::<RuntimeStatus>(&selector).await
    }

    /// Gets the [`RuntimeStatus`](`RuntimeStatus`) for the given `rtid`.
    ///
    /// # Errors
    /// If zenoh delete fails an error variant is returned.
    pub async fn remove_runtime_status(&self, rtid: &ZenohId) -> Result<()> {
        let path = RT_STATUS_PATH!(ROOT_STANDALONE, rtid);

        self.z.delete(&path).res().await
    }

    /// Stores the given [`RuntimeStatus`](`RuntimeStatus`) for the given `rtid`
    /// in Zenoh.
    ///
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - fails to serialize
    /// - zenoh put fails
    pub async fn add_runtime_status(&self, rtid: &ZenohId, rt_info: &RuntimeStatus) -> Result<()> {
        let path = RT_STATUS_PATH!(ROOT_STANDALONE, rtid);

        let encoded_info = serialize_data(rt_info)?;
        self.z.put(&path, encoded_info).res().await
    }

    /// Gets the [`DataFlowRecord`](`DataFlowRecord`) running on the given runtime `rtid` for the
    /// given instance `iid`.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - no data present in zenoh
    /// - fails to deserialize
    pub async fn get_runtime_flow_by_instance(
        &self,
        rtid: &ZenohId,
        iid: &Uuid,
    ) -> Result<DataFlowRecord> {
        let selector = RT_FLOW_SELECTOR_BY_INSTANCE!(ROOT_STANDALONE, rtid, iid);

        self.get_from_zenoh::<DataFlowRecord>(&selector).await
    }

    /// Gets the [`DataFlowRecord`](`DataFlowRecord`) running across the
    /// infrastructure for the instance `iid`.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - no data present in zenoh
    /// - fails to deserialize
    pub async fn get_flow_by_instance(&self, iid: &Uuid) -> Result<DataFlowRecord> {
        let selector = RT_FLOW_SELECTOR_BY_INSTANCE!(ROOT_STANDALONE, "*", iid);
        self.get_from_zenoh::<DataFlowRecord>(&selector).await
    }

    /// Gets all the [`DataFlowRecord`](`DataFlowRecord`) for the given
    /// instance `iid` running on the given runtime `rtid`.
    ///
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - no data present in zenoh
    /// - fails to deserialize
    pub async fn get_runtime_flow_instances(
        &self,
        rtid: &ZenohId,
        fid: &str,
    ) -> Result<Vec<DataFlowRecord>> {
        let selector = RT_FLOW_SELECTOR_BY_FLOW!(ROOT_STANDALONE, rtid, fid);

        self.get_vec_from_zenoh::<DataFlowRecord>(&selector).await
    }

    /// Gets all the [`DataFlowRecord`](`DataFlowRecord`) running across
    /// the infrastructure for the given flow `fid`.
    ///
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - no data present in zenoh
    /// - fails to deserialize
    pub async fn get_flow_instances(&self, fid: &str) -> Result<Vec<DataFlowRecord>> {
        let selector = FLOW_SELECTOR_BY_FLOW!(ROOT_STANDALONE, fid);
        self.get_vec_from_zenoh::<DataFlowRecord>(&selector).await
    }

    /// Gets all the [`DataFlowRecord`](`DataFlowRecord`) running across the
    /// infrastructure.
    pub async fn get_all_instances(&self) -> Result<Vec<DataFlowRecord>> {
        let selector = FLOW_SELECTOR_BY_FLOW!(ROOT_STANDALONE, "*");
        self.get_vec_from_zenoh::<DataFlowRecord>(&selector).await
    }

    /// Gets all the runtimes UUID where the given instance `iid` is running.
    pub async fn get_flow_instance_runtimes(&self, iid: &Uuid) -> Result<Vec<ZenohId>> {
        let selector = RT_FLOW_SELECTOR_BY_INSTANCE!(ROOT_STANDALONE, "*", iid);

        let mut ds = self.z.get(&selector).res().await?;

        let mut runtimes = Vec::new();

        for kv in ds.into_iter() {
            if let Ok(sample) = &kv.sample {
                let id = sample
                    .key_expr
                    .as_str()
                    .split('/')
                    .nth(2) // The way the key_expr are built, the 3rd "token" is the instance id
                    .ok_or_else(|| {
                        log::error!(
                            "Could not extract the instance id from key expression: {}",
                            sample.key_expr.as_str()
                        );
                        zferror!(ErrorKind::DeserializationError)
                    })?;
                runtimes.push(id.parse::<ZenohId>()?);
            }
        }

        Ok(runtimes)
    }

    /// Removes information on the given instance `iid` of the given flow `fid`
    /// running on the given runtime `rtid` from Zenoh.
    ///
    /// # Errors
    /// If zenoh delete fails an error variant is returned.
    pub async fn remove_runtime_flow_instance(
        &self,
        rtid: &ZenohId,
        fid: &str,
        iid: &Uuid,
    ) -> Result<()> {
        let path = RT_FLOW_PATH!(ROOT_STANDALONE, rtid, fid, iid);

        self.z.delete(&path).res().await
    }

    /// Stores the given [`DataFlowRecord`](`DataFlowRecord`) running on the
    /// given runtime `rtid` in Zenoh.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - fails to serialize
    /// - zenoh put fails
    pub async fn add_runtime_flow(
        &self,
        rtid: &ZenohId,
        flow_instance: &DataFlowRecord,
    ) -> Result<()> {
        let path = RT_FLOW_PATH!(
            ROOT_STANDALONE,
            rtid,
            flow_instance.flow,
            flow_instance.uuid
        );

        let encoded_info = serialize_data(flow_instance)?;
        self.z.put(&path, encoded_info).res().await
    }

    // Registry Related, registry is not yet in place.

    /// Stores the given [`RegistryNode`](`RegistryNode`) in the registry's
    /// Zenoh.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - fails to serialize
    /// - zenoh put fails
    pub async fn add_graph(&self, graph: &RegistryNode) -> Result<()> {
        let path = REG_GRAPH_SELECTOR!(ROOT_STANDALONE, &graph.id);

        let encoded_info = serialize_data(graph)?;
        self.z.put(&path, encoded_info).res().await
    }

    /// Gets the [`RegistryNode`](`RegistryNode`) associated with the given
    /// `graph_id` from registry's Zenoh.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - no data present in zenoh
    /// - fails to deserialize
    pub async fn get_graph(&self, graph_id: &str) -> Result<RegistryNode> {
        let selector = REG_GRAPH_SELECTOR!(ROOT_STANDALONE, graph_id);
        self.get_from_zenoh::<RegistryNode>(&selector).await
    }

    /// Gets all the nodes [`RegistryNode`](`RegistryNode`) within the
    /// registry's Zenoh.
    ///
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - no data present in zenoh
    /// - fails to deserialize
    pub async fn get_all_graphs(&self) -> Result<Vec<RegistryNode>> {
        let selector = REG_GRAPH_SELECTOR!(ROOT_STANDALONE, "*");
        self.get_vec_from_zenoh::<RegistryNode>(&selector).await
    }

    /// Removes the given node `graph_id` from registry's Zenoh.
    pub async fn delete_graph(&self, graph_id: &str) -> Result<()> {
        let path = REG_GRAPH_SELECTOR!(ROOT_STANDALONE, &graph_id);

        self.z.delete(&path).res().await
    }

    // Job Queue

    /// Subscribes to the job queue of the given `rtid`
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - zenoh subscribe fails
    /// - fails to deserialize
    pub async fn subscribe_sumbitted_jobs(
        &self,
        rtid: &ZenohId,
    ) -> Result<zenoh::subscriber::FlumeSubscriber<'static>> {
        let selector = JQ_SUMBITTED_SEL!(ROOT_STANDALONE, rtid);
        self.z.declare_subscriber(&selector).res().await
    }

    /// Submits the given [`Job`](`Job`) in the queue
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - fails to serialize
    /// - zenoh put fails
    pub async fn add_submitted_job(&self, rtid: &ZenohId, job: &Job) -> Result<()> {
        let path = JQ_SUMBITTED_JOB!(ROOT_STANDALONE, rtid, &job.id);
        let encoded_info = serialize_data(job)?;
        self.z.put(&path, encoded_info).res().await
    }

    pub async fn del_submitted_job(&self, rtid: &ZenohId, id: &Uuid) -> Result<()> {
        let path = JQ_SUMBITTED_JOB!(ROOT_STANDALONE, rtid, id);
        self.z.delete(&path).res().await
    }

    /// Sets as started the given [`Job`](`Job`) in the queue
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - fails to serialize
    /// - zenoh put fails
    pub async fn add_started_job(&self, rtid: &ZenohId, job: &Job) -> Result<()> {
        let path = JQ_STARTED_JOB!(ROOT_STANDALONE, rtid, &job.id);
        let encoded_info = serialize_data(job)?;
        self.z.put(&path, encoded_info).res().await
    }

    /// Sets as completed the given [`Job`](`Job`) in the queue
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - fails to serialize
    /// - zenoh put fails
    pub async fn add_done_job(&self, rtid: &ZenohId, job: &Job) -> Result<()> {
        let path = JQ_DONE_JOB!(ROOT_STANDALONE, rtid, &job.id);
        let encoded_info = serialize_data(job)?;
        self.z.put(&path, encoded_info).res().await
    }

    /// Sets as failed the given [`Job`](`Job`) in the queue
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - fails to serialize
    /// - zenoh put fails
    pub async fn add_failed_job(&self, rtid: &ZenohId, job: &Job) -> Result<()> {
        let path = JQ_FAILED_JOB!(ROOT_STANDALONE, rtid, &job.id);
        let encoded_info = serialize_data(job)?;
        self.z.put(&path, encoded_info).res().await
    }

    // Helpers

    /// Helper function to get a generic data `T` and deserializing it
    /// from Zenoh.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - no data present in zenoh
    /// - fails to deserialize
    /// - wrong zenoh encoding
    async fn get_from_zenoh<T>(&self, path: &str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let mut ds = self.z.get(path).res().await?;
        let data = ds.into_iter().collect::<Vec<Reply>>();
        match data.len() {
            0 => Err(zferror!(ErrorKind::Empty).into()),
            _ => {
                let kv = &data[0];
                match &kv.sample {
                    Ok(sample) => match &sample.value.encoding {
                        &Encoding::APP_OCTET_STREAM => {
                            let ni = deserialize_data::<T>(&sample.value.payload.contiguous())?;
                            Ok(ni)
                        }
                        _ => Err(zferror!(ErrorKind::DeserializationError).into()),
                    },
                    _ => Err(zferror!(ErrorKind::DeserializationError).into()),
                }
            }
        }
    }

    /// Helper function to get a vector of genetic `T` and deserializing
    /// it from Zenoh.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - wrong encoding
    /// - fails to deserialize
    async fn get_vec_from_zenoh<T>(&self, selector: &str) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        let mut ds = self.z.get(selector).res().await?;

        let mut zf_data: Vec<T> = Vec::new();

        for kv in ds.into_iter() {
            match &kv.sample {
                Ok(sample) => match &sample.value.encoding {
                    &Encoding::APP_OCTET_STREAM => {
                        let ni = deserialize_data::<T>(&sample.value.payload.contiguous())?;
                        zf_data.push(ni);
                    }
                    _ => return Err(zferror!(ErrorKind::DeserializationError).into()),
                },
                _ => return Err(zferror!(ErrorKind::DeserializationError).into()),
            }
        }
        Ok(zf_data)
    }
}

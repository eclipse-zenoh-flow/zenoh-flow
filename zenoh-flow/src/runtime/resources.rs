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

#![allow(unused)]

#[cfg(feature = "data_bincode")]
extern crate bincode;

#[cfg(feature = "data_cbor")]
extern crate serde_cbor;

#[cfg(feature = "data_json")]
extern crate serde_json;

use crate::model::dataflow::record::DataFlowRecord;
use crate::model::RegistryNode;
use crate::runtime::{RuntimeConfig, RuntimeInfo, RuntimeStatus};
use crate::zferror;
use crate::zfresult::ErrorKind;
use crate::Result;
use async_std::pin::Pin;
use async_std::stream::Stream;
use async_std::task::{Context, Poll};
use futures::StreamExt;
use pin_project_lite::pin_project;
use serde::{de::DeserializeOwned, Serialize};
use std::convert::TryFrom;
use std::sync::Arc;
use uuid::Uuid;
use zenoh::net::protocol::io::SplitBuffer;
use zenoh::prelude::*;
use zenoh::query::Reply;

//NOTE: this should be pub(crate)

/// Root prefix for key expressions when running as a router plugin.
pub static ROOT_PLUGIN_RUNTIME_PREFIX: &str = "/@/router/";
/// Root suffix for key expression when running as router plugin.
pub static ROOT_PLUGIN_RUNTIME_SUFFIX: &str = "/plugin/zenoh-flow";
/// Root for key expression when running as standalone.
pub static ROOT_STANDALONE: &str = "/zenoh-flow";

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

pin_project! {
    /// Custom stream to lister for Runtime Configuration changes.
    pub struct ZFRuntimeConfigStream {
        #[pin]
        sample_stream: zenoh::subscriber::SampleReceiver,
    }
}

impl ZFRuntimeConfigStream {
    pub async fn close(self) -> Result<()> {
        Ok(())
    }
}

impl Stream for ZFRuntimeConfigStream {
    type Item = crate::runtime::RuntimeConfig;

    #[inline(always)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match async_std::pin::Pin::new(self)
            .sample_stream
            .poll_next_unpin(cx)
        {
            Poll::Ready(Some(sample)) => match sample.kind {
                SampleKind::Put | SampleKind::Patch => match sample.value.encoding {
                    Encoding::APP_OCTET_STREAM => {
                        match deserialize_data::<crate::runtime::RuntimeConfig>(
                            &sample.value.payload.contiguous(),
                        ) {
                            Ok(info) => Poll::Ready(Some(info)),
                            Err(_) => Poll::Pending,
                        }
                    }
                    _ => {
                        log::warn!(
                            "Received sample with wrong encoding {:?}, dropping",
                            sample.value.encoding
                        );
                        Poll::Pending
                    }
                },
                SampleKind::Delete => {
                    log::warn!("Received delete sample drop it");
                    Poll::Pending
                }
            },
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
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
    pub async fn get_runtime_info(&self, rtid: &Uuid) -> Result<RuntimeInfo> {
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
        Err(zferror!(ErrorKind::NotFound).into())
    }

    /// Removes the information for the given runtime `rtid`.
    ///
    /// # Errors
    /// If zenoh delete fails an error variant is returned.
    pub async fn remove_runtime_info(&self, rtid: &Uuid) -> Result<()> {
        let path = RT_INFO_PATH!(ROOT_STANDALONE, rtid);

        Ok(self.z.delete(&path).await?)
    }

    /// Stores the given  [`RuntimeInfo`](`RuntimeInfo`) for the given `rtid`
    /// in Zenoh.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - fails to serialize
    /// - zenoh put fails
    pub async fn add_runtime_info(&self, rtid: &Uuid, rt_info: &RuntimeInfo) -> Result<()> {
        let path = RT_INFO_PATH!(ROOT_STANDALONE, rtid);

        let encoded_info = serialize_data(rt_info)?;
        Ok(self.z.put(&path, encoded_info).await?)
    }

    /// Gets [`RuntimeConfig`](`RuntimeConfig`) for the given `rtid`
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - no data present in zenoh
    /// - fails to deserialize
    pub async fn get_runtime_config(&self, rtid: &Uuid) -> Result<RuntimeConfig> {
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
    pub async fn subscribe_runtime_config(&self, rtid: &Uuid) -> Result<ZFRuntimeConfigStream> {
        // let selector = RT_CONFIGURATION_PATH!(ROOT_STANDALONE, rtid))?;
        //
        // Ok(self.z
        //     .subscribe(&selector)
        //     .await
        //     .map(|change_stream| ZFRuntimeConfigStream { change_stream })?)
        Err(zferror!(ErrorKind::Unimplemented).into())
    }

    /// Removes the configuration for the given `rtid`.
    ///
    /// # Errors
    /// If zenoh delete fails an error variant is returned.
    pub async fn remove_runtime_config(&self, rtid: &Uuid) -> Result<()> {
        let path = RT_CONFIGURATION_PATH!(ROOT_STANDALONE, rtid);

        Ok(self.z.delete(&path).await?)
    }

    /// Stores the given [`RuntimeConfig`](`RuntimeConfig`) for the given
    /// `rtid` in Zenoh.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - fails to serialize
    /// - zenoh put fails
    pub async fn add_runtime_config(&self, rtid: &Uuid, rt_info: &RuntimeConfig) -> Result<()> {
        let path = RT_CONFIGURATION_PATH!(ROOT_STANDALONE, rtid);

        let encoded_info = serialize_data(rt_info)?;
        Ok(self.z.put(&path, encoded_info).await?)
    }

    /// Gets the `RuntimeStatus` for the given runtime `rtid`.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - no data present in zenoh
    /// - fails to deserialize
    pub async fn get_runtime_status(&self, rtid: &Uuid) -> Result<RuntimeStatus> {
        let selector = RT_STATUS_PATH!(ROOT_STANDALONE, rtid);
        self.get_from_zenoh::<RuntimeStatus>(&selector).await
    }

    /// Gets the [`RuntimeStatus`](`RuntimeStatus`) for the given `rtid`.
    ///
    /// # Errors
    /// If zenoh delete fails an error variant is returned.
    pub async fn remove_runtime_status(&self, rtid: &Uuid) -> Result<()> {
        let path = RT_STATUS_PATH!(ROOT_STANDALONE, rtid);

        Ok(self.z.delete(&path).await?)
    }

    /// Stores the given [`RuntimeStatus`](`RuntimeStatus`) for the given `rtid`
    /// in Zenoh.
    ///
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - fails to serialize
    /// - zenoh put fails
    pub async fn add_runtime_status(&self, rtid: &Uuid, rt_info: &RuntimeStatus) -> Result<()> {
        let path = RT_STATUS_PATH!(ROOT_STANDALONE, rtid);

        let encoded_info = serialize_data(rt_info)?;
        Ok(self.z.put(&path, encoded_info).await?)
    }

    /// Gets the [`DataFlowRecord`](`DataFlowRecord`) running on the given
    /// runtime `rtid` for the given instance `iid`.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - no data present in zenoh
    /// - fails to deserialize
    pub async fn get_runtime_flow_by_instance(
        &self,
        rtid: &Uuid,
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
        rtid: &Uuid,
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
    pub async fn get_flow_instance_runtimes(&self, iid: &Uuid) -> Result<Vec<Uuid>> {
        let selector = RT_FLOW_SELECTOR_BY_INSTANCE!(ROOT_STANDALONE, "*", iid);

        let mut ds = self.z.get(&selector).await?;

        let data = ds.collect::<Vec<Reply>>().await;
        let mut runtimes = Vec::new();

        for kv in data.into_iter() {
            let id = kv
                .sample
                .key_expr
                .as_str()
                .split('/')
                .nth(3) // The way the key_expr are built, the 3rd "token" is the instance id
                .ok_or_else(|| {
                    log::error!(
                        "Could not extract the instance id from key expression: {}",
                        kv.sample.key_expr.as_str()
                    );
                    zferror!(ErrorKind::DeseralizationError)
                })?;
            runtimes.push(Uuid::parse_str(id).map_err(|e| zferror!(ErrorKind::DeseralizationError, e))?);
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
        rtid: &Uuid,
        fid: &str,
        iid: &Uuid,
    ) -> Result<()> {
        let path = RT_FLOW_PATH!(ROOT_STANDALONE, rtid, fid, iid);

        Ok(self.z.delete(&path).await?)
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
        rtid: &Uuid,
        flow_instance: &DataFlowRecord,
    ) -> Result<()> {
        let path = RT_FLOW_PATH!(
            ROOT_STANDALONE,
            rtid,
            flow_instance.flow,
            flow_instance.uuid
        );

        let encoded_info = serialize_data(flow_instance)?;
        Ok(self.z.put(&path, encoded_info).await?)
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
        Ok(self.z.put(&path, encoded_info).await?)
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

        Ok(self.z.delete(&path).await?)
    }

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
        let mut ds = self.z.get(path).await?;
        let data = ds.collect::<Vec<Reply>>().await;
        match data.len() {
            0 => Err(zferror!(ErrorKind::Empty).into()),
            _ => {
                let kv = &data[0];
                match &kv.sample.value.encoding {
                    &Encoding::APP_OCTET_STREAM => {
                        let ni = deserialize_data::<T>(&kv.sample.value.payload.contiguous())?;
                        Ok(ni)
                    }
                    _ => Err(zferror!(ErrorKind::DeseralizationError).into()),
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
        let mut ds = self.z.get(selector).await?;

        let data = ds.collect::<Vec<Reply>>().await;
        let mut zf_data: Vec<T> = Vec::new();

        for kv in data.into_iter() {
            match &kv.sample.value.encoding {
                &Encoding::APP_OCTET_STREAM => {
                    let ni = deserialize_data::<T>(&kv.sample.value.payload.contiguous())?;
                    zf_data.push(ni);
                }
                _ => return Err(zferror!(ErrorKind::DeseralizationError).into()),
            }
        }
        Ok(zf_data)
    }
}

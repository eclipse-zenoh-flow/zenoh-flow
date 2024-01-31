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

mod load;

use crate::{instance::DataFlowInstance, loader::Loader, InstanceStatus};

use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    sync::Arc,
};

use anyhow::{anyhow, bail};
use async_std::sync::{Mutex, RwLock};
use uhlc::HLC;
#[cfg(feature = "zenoh")]
use zenoh::Session;
#[cfg(feature = "shared-memory")]
use zenoh_flow_commons::SharedMemoryConfiguration;
use zenoh_flow_commons::{InstanceId, Result, RuntimeId};
use zenoh_flow_records::DataFlowRecord;

pub struct Runtime {
    pub(crate) name: Arc<str>,
    pub(crate) runtime_id: RuntimeId,
    pub(crate) hlc: Arc<HLC>,
    #[cfg(feature = "zenoh")]
    pub(crate) session: Arc<Session>,
    #[cfg(feature = "shared-memory")]
    pub(crate) shared_memory: SharedMemoryConfiguration,
    pub(crate) loader: Mutex<Loader>,
    flows: RwLock<HashMap<InstanceId, Arc<RwLock<DataFlowInstance>>>>,
}

impl Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "runtime: '{}' - {}", self.name, self.runtime_id)
    }
}

impl Display for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self)
    }
}

impl Runtime {
    /// Returns the unique identifier of this Zenoh-Flow runtime.
    pub fn id(&self) -> &RuntimeId {
        &self.runtime_id
    }

    pub fn hlc(&self) -> Arc<HLC> {
        self.hlc.clone()
    }

    /// Returns an atomically counted reference over the Zenoh session this Zenoh-Flow runtime leverages.
    #[cfg(feature = "zenoh")]
    pub fn session(&self) -> Arc<Session> {
        self.session.clone()
    }

    /// TODO@J-Loudet
    ///
    /// Creates a new Zenoh-Flow runtime.
    ///
    /// Runtime manages data flows:
    /// - node management,
    /// - library management
    ///
    /// # Features
    ///
    /// - "zenoh": requires a Zenoh session to be able to communicate with other Zenoh nodes
    /// - "shared-memory": enables the Zenoh shared-memory transport (experimental)
    pub fn new(
        id: RuntimeId,
        name: Arc<str>,
        loader: Loader,
        hlc: Arc<HLC>,
        #[cfg(feature = "zenoh")] session: Arc<Session>,
        #[cfg(feature = "shared-memory")] shared_memory: SharedMemoryConfiguration,
    ) -> Self {
        Self {
            name,
            runtime_id: id,
            #[cfg(feature = "zenoh")]
            session,
            hlc,
            loader: Mutex::new(loader),
            #[cfg(feature = "shared-memory")]
            shared_memory,
            flows: RwLock::new(HashMap::default()),
        }
    }

    /// Returns the [DataFlowRecord] associated with the provided instance.
    pub async fn get_record(&self, id: &InstanceId) -> Option<DataFlowRecord> {
        let flows = self.flows.read().await;
        if let Some(instance) = flows.get(id) {
            return Some(instance.read().await.record.clone());
        }

        None
    }

    /// Returns the [status](InstanceStatus) of the provided data flow instance or `None` if this runtime does not
    /// manage this instance.
    ///
    /// The possible values are:
    /// - [Loaded](InstanceStatus::Loaded) when all the nodes that this runtime manages have been successfully
    ///   loaded. In particular, this means that each node has successfully called its constructor.
    /// - [Running](InstanceStatus::Running) when the nodes that this runtime manages are running.
    /// - [Aborted](InstanceStatus::Aborted) when the nodes were previously running and their execution was aborted.
    pub async fn get_status(&self, id: &InstanceId) -> Option<InstanceStatus> {
        if let Some(instance) = self.flows.read().await.get(id) {
            return Some(*instance.read().await.status());
        }

        None
    }

    async fn try_get_instance(&self, id: &InstanceId) -> Result<Arc<RwLock<DataFlowInstance>>> {
        let flows_guard = self.flows.read().await;
        flows_guard
            .get(id)
            .cloned()
            .ok_or_else(|| anyhow!("Found no Data Flow with id < {} >", id))
    }

    pub async fn try_start_instance(&self, id: &InstanceId) -> Result<()> {
        let instance = self.try_get_instance(id).await?;
        let mut instance_guard = instance.write().await;

        instance_guard.start();

        tracing::trace!(
            "Started data flow ({}) instance < {} > ",
            instance_guard.name(),
            instance_guard.instance_id()
        );

        Ok(())
    }

    /// TODO@J-Loudet
    ///
    /// - abort all nodes
    pub async fn try_abort_instance(&self, id: &InstanceId) -> Result<()> {
        let instance = self.try_get_instance(id).await?;

        if !matches!(instance.read().await.status(), &InstanceStatus::Running) {
            return Ok(());
        }

        let mut instance_guard = instance.write().await;

        instance_guard.abort().await;

        tracing::trace!(
            "Aborted data flow ({}) instance < {} >",
            instance_guard.name(),
            instance_guard.instance_id()
        );

        Ok(())
    }

    pub async fn try_delete_instance(&self, id: &InstanceId) -> Result<()> {
        tracing::trace!("Attempting to delete instance < {} >", id);
        let instance = {
            let mut flows_guard = self.flows.write().await;
            flows_guard
                .remove(id)
                .ok_or_else(|| anyhow!("Found no Data Flow with id < {} >", id))?
        };

        let backup = Arc::downgrade(&instance);
        let mut instance = {
            if let Some(instance) = Arc::into_inner(instance) {
                instance.into_inner()
            } else {
                let backup = backup.upgrade().ok_or_else(|| {
                    anyhow!(
                        "Data Flow instance < {} > was, miraculously, deleted elsewhere",
                        id
                    )
                })?;

                {
                    let mut flows_guard = self.flows.write().await;
                    flows_guard.insert(id.clone(), backup);
                }

                bail!(
                    "Unable to delete Data Flow < {} >, another action is being performed on it",
                    id
                )
            }
        };

        tracing::trace!(
            "Aborting Data Flow ({}) < {} >",
            instance.name(),
            instance.instance_id()
        );
        instance.abort().await;

        drop(instance); // Forcefully drop the instance so we can check if we can free up some Libraries.
        self.loader.lock().await.remove_unused_libraries();

        Ok(())
    }
}

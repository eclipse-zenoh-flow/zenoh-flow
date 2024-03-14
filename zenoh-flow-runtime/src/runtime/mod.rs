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

mod builder;
pub use builder::RuntimeBuilder;

mod load;

use crate::{
    instance::{DataFlowInstance, InstanceStatus},
    loader::Loader,
    InstanceState,
};

use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    sync::Arc,
};

use anyhow::{anyhow, bail};
use async_std::sync::{Mutex, RwLock};
use thiserror::Error;
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

#[derive(Error, Debug)]
pub enum DataFlowErr {
    #[error("found no data flow with the provided id")]
    NotFound,
    #[error("the data flow is in a failed state, unable to process the request")]
    FailedState,
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
    pub fn builder(name: impl Into<String>) -> RuntimeBuilder {
        RuntimeBuilder::new(name)
    }

    /// Returns the unique identifier of this Zenoh-Flow runtime.
    pub fn id(&self) -> &RuntimeId {
        &self.runtime_id
    }

    /// Returns the name of this Zenoh-Flow runtime.
    ///
    /// The name of a Zenoh-Flow runtime does NOT have to be unique, it is only used as a human-readable description.
    pub fn name(&self) -> Arc<str> {
        self.name.clone()
    }

    pub fn hlc(&self) -> Arc<HLC> {
        self.hlc.clone()
    }

    /// Returns information regarding the data flows that are running on this Zenoh-Flow runtime.
    ///
    /// For each instance of a data flow, returns its unique identifier, its name and its state.
    pub async fn instances_state(&self) -> HashMap<InstanceId, (Arc<str>, InstanceState)> {
        let flows = self.flows.read().await;
        let mut states = HashMap::with_capacity(flows.len());
        for (instance_id, instance_lck) in flows.iter() {
            let instance = instance_lck.read().await;
            states.insert(
                instance_id.clone(),
                (instance.name().clone(), instance.state().clone()),
            );
        }

        states
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

    /// Returns the [DataFlowRecord] associated with the provided instance, *if that instance is not in a failed state*.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - this runtime does not manage a data flow with the provided id,
    /// - the data flow is in an failed state.
    pub async fn try_get_record(
        &self,
        id: &InstanceId,
    ) -> std::result::Result<DataFlowRecord, DataFlowErr> {
        let flows = self.flows.read().await;
        if let Some(instance) = flows.get(id) {
            let instance_guard = instance.read().await;
            if !matches!(instance_guard.state, InstanceState::Failed(_)) {
                return Ok(instance_guard.record.clone());
            }

            return Err(DataFlowErr::FailedState);
        }

        Err(DataFlowErr::NotFound)
    }

    /// Returns the [status](InstanceStatus) of the provided data flow instance or `None` if this runtime does not
    /// manage this instance.
    ///
    /// The possible values are:
    /// - [Loaded](InstanceStatus::Loaded) when all the nodes that this runtime manages have been successfully
    ///   loaded. In particular, this means that each node has successfully called its constructor.
    /// - [Running](InstanceStatus::Running) when the nodes that this runtime manages are running.
    /// - [Aborted](InstanceStatus::Aborted) when the nodes were previously running and their execution was aborted.
    pub async fn get_instance_status(&self, id: &InstanceId) -> Option<InstanceStatus> {
        if let Some(instance) = self.flows.read().await.get(id) {
            return Some(instance.read().await.status(&self.runtime_id));
        }

        None
    }

    /// Tries to retrieve the [DataFlowInstance] matching the provided [id](InstanceId) from the Zenoh-Flow runtime.
    ///
    /// # Errors
    ///
    /// This method will fail if:
    /// - this runtime manages no data flow with the provided instance id,
    /// - the data flow is in a failed state.
    ///
    /// Data Flow that are in a failed state cannot be started or aborted. They can provide their status or be deleted.
    async fn try_get_instance(
        &self,
        id: &InstanceId,
    ) -> std::result::Result<Arc<RwLock<DataFlowInstance>>, DataFlowErr> {
        let flows_guard = self.flows.read().await;

        let instance = flows_guard.get(id).cloned().ok_or(DataFlowErr::NotFound)?;

        if matches!(instance.read().await.state, InstanceState::Failed(_)) {
            return Err(DataFlowErr::FailedState);
        }

        Ok(instance)
    }

    #[tracing::instrument(name = "start", skip(self, id), fields(instance = %id))]
    pub async fn try_start_instance(&self, id: &InstanceId) -> Result<()> {
        let instance = self.try_get_instance(id).await?;
        let mut instance_guard = instance.write().await;

        instance_guard.start(&self.hlc).await?;

        tracing::info!("started");

        Ok(())
    }

    /// TODO@J-Loudet
    ///
    /// - abort all nodes
    #[tracing::instrument(name = "abort", skip(self, id), fields(instance = %id))]
    pub async fn try_abort_instance(&self, id: &InstanceId) -> Result<()> {
        let instance = self.try_get_instance(id).await?;

        if !matches!(instance.read().await.state(), &InstanceState::Running(_)) {
            return Ok(());
        }

        let mut instance_guard = instance.write().await;

        instance_guard.abort(&self.hlc).await;

        tracing::info!("aborted");

        Ok(())
    }

    #[tracing::instrument(name = "delete", skip(self, id), fields(instance = %id))]
    pub async fn try_delete_instance(&self, id: &InstanceId) -> Result<()> {
        let instance = {
            let mut flows_guard = self.flows.write().await;
            flows_guard
                .remove(id)
                .ok_or_else(|| anyhow!("found no instance with this id"))?
        };

        let backup = Arc::downgrade(&instance);
        let mut instance = {
            if let Some(instance) = Arc::into_inner(instance) {
                instance.into_inner()
            } else {
                let backup = backup
                    .upgrade()
                    .ok_or_else(|| anyhow!("instance was, miraculously, deleted elsewhere"))?;

                {
                    let mut flows_guard = self.flows.write().await;
                    flows_guard.insert(id.clone(), backup);
                }

                bail!("unable to delete instance, another action is being performed on it")
            }
        };

        instance.abort(&self.hlc).await;

        drop(instance); // Forcefully drop the instance so we can check if we can free up some Libraries.
        self.loader.lock().await.remove_unused_libraries();
        tracing::info!("deleted");

        Ok(())
    }
}

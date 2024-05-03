//
// Copyright © 2021 ZettaScale Technology <contact@zettascale.tech>
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
pub use self::builder::RuntimeBuilder;

mod load;

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

use crate::{
    instance::{DataFlowInstance, InstanceStatus},
    loader::Loader,
    InstanceState,
};

/// A Zenoh-Flow runtime manages a subset of the nodes of [DataFlowInstance]\(s\).
///
/// In order to start a data flow, a Zenoh-Flow runtime should first be created and then tasked with loading and
/// starting the nodes *it is responsible for*.
///
/// It is important to note here that, on its own, a Zenoh-Flow runtime will not contact other runtimes to coordinate
/// the start of a data flow. If a data flow spawns on multiple runtimes, one needs to start each separately. Otherwise,
/// the Zenoh-Flow daemon structure has been especially designed to address this use-case.
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

/// This enumeration defines the errors one can face when trying to access a [DataFlowInstance].
#[derive(Error, Debug)]
pub enum DataFlowErr {
    #[error("found no data flow with the provided id")]
    NotFound,
    /// A [DataFlowInstance] in a failed state cannot be manipulated, only deleted.
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
    /// Create a new builder in order to construct a `Runtime`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use zenoh_flow_runtime::Runtime;
    /// # async_std::task::block_on(async {
    /// let runtime = Runtime::builder("demo")
    ///     .build()
    ///     .await
    ///     .expect("Failed to build the Runtime");
    /// # });
    /// ```
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

    /// Returns a shared pointer over the [HLC] used by this Runtime.
    pub fn hlc(&self) -> Arc<HLC> {
        self.hlc.clone()
    }

    /// Returns the [InstanceState] of the [DataFlowInstance]\(s\) managed by this Zenoh-Flow runtime.
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

    /// Returns a shared pointer over the Zenoh [session](Session) used by this Runtime.
    #[cfg(feature = "zenoh")]
    pub fn session(&self) -> Arc<Session> {
        self.session.clone()
    }

    /// Returns the [DataFlowRecord] associated with the provided instance.
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

    /// Returns the [status](InstanceStatus) of the provided data flow instance or [None] if this runtime does not
    /// manage this instance.
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
    /// Data Flows that are in a failed state cannot be started or aborted. They can provide their status or be deleted.
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

    /// Attempts to (re-)start the [DataFlowInstance] identified by the provided `id`.
    ///
    /// Note that this method is idempotent: calling it on an already running data flow will do nothing.
    ///
    /// # Errors
    ///
    /// This method can fail for the following reason:
    /// - no data flow with the provided id was found,
    /// - the data flow is in a failed state,
    /// - the data flow is restarted and the [on_resume] method of one of the nodes (managed by the runtime) failed.
    ///
    /// [on_resume]: zenoh_flow_nodes::prelude::Node::on_resume()
    #[tracing::instrument(name = "start", skip(self, id), fields(instance = %id))]
    pub async fn try_start_instance(&self, id: &InstanceId) -> Result<()> {
        let instance = self.try_get_instance(id).await?;
        let mut instance_guard = instance.write().await;

        instance_guard.start(&self.hlc).await?;

        tracing::info!("started");

        Ok(())
    }

    /// Attempts to abort the [DataFlowInstance] identified by the provided `id`.
    ///
    /// Note that this method is idempotent: calling it on an already aborted data flow will do nothing.
    ///
    /// # Errors
    ///
    /// This method can fail for the following reasons:
    /// - no data flow with the provided id was found,
    /// - the data flow is in a failed state.
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

    /// Attempts to delete the [DataFlowInstance] identified by the provided `id`.
    ///
    /// # Errors
    ///
    /// This method can fail for the following reasons:
    /// - no data flow with the provided id was found,
    /// - another action is currently being performed on the data flow.
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

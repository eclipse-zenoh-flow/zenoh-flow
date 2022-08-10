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

use crate::async_std::sync::Arc;
use crate::error::ZFError;
use crate::runtime::dataflow::instance::io::{Input, Output};
use crate::runtime::dataflow::instance::runners::{Runner, RunnerKind};
use crate::runtime::dataflow::node::OperatorLoaded;
use crate::runtime::InstanceContext;
use crate::traits::Operator;
use crate::types::{Configuration, Context, NodeId, PortId, ZFResult};
use async_std::task::JoinHandle;
use async_trait::async_trait;
use futures::future::{AbortHandle, Abortable, Aborted};
use std::collections::HashMap;
use std::time::Instant;

#[cfg(target_family = "unix")]
use libloading::os::unix::Library;
#[cfg(target_family = "windows")]
use libloading::Library;

/// The `OperatorRunner` is the component in charge of executing the operator.
/// It contains all the runtime information for the operator, the graph instance.
///
/// Do not reorder the fields in this struct.
/// Rust drops fields in a struct in the same order they are declared.
/// Ref: <https://doc.rust-lang.org/reference/destructors.html>
/// We need the state to be dropped before the operator/lib, otherwise we
/// will have a SIGSEV.
pub struct OperatorRunner {
    pub(crate) id: NodeId,
    pub(crate) context: Context,
    pub(crate) configuration: Option<Configuration>,
    pub(crate) inputs: HashMap<PortId, Input>,
    pub(crate) outputs: HashMap<PortId, Output>,
    pub(crate) operator: Arc<dyn Operator>,
    pub(crate) _library: Option<Arc<Library>>,
    pub(crate) handle: Option<JoinHandle<Result<ZFError, Aborted>>>,
    pub(crate) abort_handle: Option<AbortHandle>,
    pub(crate) callbacks_senders_handle: Option<JoinHandle<Result<ZFError, Aborted>>>,
    pub(crate) callbacks_senders_abort_handle: Option<AbortHandle>,
    pub(crate) callbacks_receivers_handle: Option<JoinHandle<Result<ZFError, Aborted>>>,
    pub(crate) callbacks_receivers_abort_handle: Option<AbortHandle>,
}

impl OperatorRunner {
    /// Tries to create a new `OperatorRunner` using the given
    /// [`InstanceContext`](`InstanceContext`), [`OperatorLoaded`](`OperatorLoaded`)
    /// and [`OperatorIO`](`OperatorIO`).
    ///
    /// # Errors
    /// If fails if the output is not connected.
    pub fn new(
        instance_context: Arc<InstanceContext>,
        operator: OperatorLoaded,
        inputs: HashMap<PortId, Input>,
        outputs: HashMap<PortId, Output>,
    ) -> Self {
        // TODO Check that all ports are used.
        Self {
            id: operator.id,
            context: Context::new(instance_context),
            configuration: operator.configuration,
            inputs,
            outputs,
            operator: operator.operator,
            _library: operator.library,
            handle: None,
            abort_handle: None,
            callbacks_senders_handle: None,
            callbacks_senders_abort_handle: None,
            callbacks_receivers_handle: None,
            callbacks_receivers_abort_handle: None,
        }
    }
}

#[async_trait]
impl Runner for OperatorRunner {
    fn get_id(&self) -> NodeId {
        self.id.clone()
    }
    fn get_kind(&self) -> RunnerKind {
        RunnerKind::Operator
    }

    async fn stop(&mut self) -> ZFResult<()> {
        // Stop is idempotent, if the node was already stopped,
        // do nothing and return Ok(())

        if let Some(abort_handle) = self.abort_handle.take() {
            abort_handle.abort()
        }

        if let Some(handle) = self.handle.take() {
            log::trace!("Operator handler finished with {:?}", handle.await);
        }

        Ok(())
    }

    async fn is_running(&self) -> bool {
        self.handle.is_some()
            || self.callbacks_receivers_handle.is_some()
            || self.callbacks_senders_handle.is_some()
    }

    async fn start(&mut self) -> ZFResult<()> {
        // Start is idempotent, if the node was already started,
        // do nothing and return Ok(())
        if self.handle.is_some() && self.abort_handle.is_some() {
            log::warn!(
                "[Operator: {}] Trying to start while it is already started, aborting",
                self.id
            );
            return Ok(());
        }

        log::trace!("[Operator: {}] Starting", self.id);

        let iteration = self
            .operator
            .setup(
                &mut self.context,
                &self.configuration,
                self.inputs.clone(),
                self.outputs.clone(),
            )
            .await?;

        /* Callbacks */
        // Senders
        let cb_senders = std::mem::take(&mut self.context.callback_senders);
        if !cb_senders.is_empty() {
            let c_id = self.id.clone();

            let callbacks_senders_loop = async move {
                let mut cbs: Vec<_> = cb_senders
                    .iter()
                    .map(|callback| Box::pin(callback.trigger()))
                    .collect();

                loop {
                    let (res, _, remainings) = futures::future::select_all(cbs).await;
                    cbs = remainings;
                    match res {
                        Err(e) => {
                            log::error!("[Source: {c_id}] {:?}", e);
                            return e;
                        }
                        Ok(index) => {
                            cbs.push(Box::pin(cb_senders[index].trigger()));
                        }
                    }

                    async_std::task::yield_now().await;
                }
            };

            let (cb_abort_handle, cb_abort_registration) = AbortHandle::new_pair();
            let cb_handle = async_std::task::spawn(Abortable::new(
                callbacks_senders_loop,
                cb_abort_registration,
            ));
            self.callbacks_senders_handle = Some(cb_handle);
            self.callbacks_senders_abort_handle = Some(cb_abort_handle);
        }

        // Receivers
        let cb_receivers = std::mem::take(&mut self.context.callback_receivers);
        if !cb_receivers.is_empty() {
            let c_id = Arc::clone(&self.id);
            let callbacks_receivers_loop = async move {
                let mut cbs: Vec<_> = cb_receivers
                    .iter()
                    .map(|callback| Box::pin(callback.run()))
                    .collect();

                loop {
                    let (res, _, remainings) = futures::future::select_all(cbs).await;
                    cbs = remainings;
                    match res {
                        Err(e) => {
                            log::error!("[Source: {c_id}] {:?}", e);
                            return e;
                        }
                        Ok(index) => {
                            cbs.push(Box::pin(cb_receivers[index].run()));
                        }
                    }

                    async_std::task::yield_now().await;
                }
            };

            let (cb_abort_handle, cb_abort_registration) = AbortHandle::new_pair();
            let cb_handle = async_std::task::spawn(Abortable::new(
                callbacks_receivers_loop,
                cb_abort_registration,
            ));
            self.callbacks_receivers_handle = Some(cb_handle);
            self.callbacks_receivers_abort_handle = Some(cb_abort_handle);
        }

        /* Streams */
        if let Some(iteration) = iteration {
            let c_id = self.id.clone();
            let run_loop = async move {
                let mut instant: Instant;
                loop {
                    instant = Instant::now();
                    if let Err(e) = iteration.call().await {
                        log::error!("[Operator: {c_id}] {:?}", e);
                        return e;
                    }

                    log::trace!(
                        "[Operator: {c_id}] iteration took: {}ms",
                        instant.elapsed().as_millis()
                    );

                    async_std::task::yield_now().await;
                }
            };

            let (abort_handle, abort_registration) = AbortHandle::new_pair();
            let handle = async_std::task::spawn(Abortable::new(run_loop, abort_registration));

            self.handle = Some(handle);
            self.abort_handle = Some(abort_handle);
        }

        Ok(())
    }
}

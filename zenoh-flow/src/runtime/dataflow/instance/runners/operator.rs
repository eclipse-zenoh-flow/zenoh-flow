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

use crate::prelude::{Inputs, Outputs};
use crate::runtime::dataflow::instance::runners::{Runner, RunnerKind};
use crate::runtime::dataflow::node::OperatorLoaded;
use crate::runtime::InstanceContext;
use crate::traits::Operator;
use crate::types::{Configuration, Context, NodeId};
use crate::zfresult::Error;
use crate::Result as ZFResult;
use async_std::task::JoinHandle;
use async_trait::async_trait;
use futures::future::{AbortHandle, Abortable, Aborted};
use std::sync::Arc;
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
    pub(crate) inputs: Inputs,
    pub(crate) outputs: Outputs,
    pub(crate) operator: Arc<dyn Operator>,
    pub(crate) _library: Option<Arc<Library>>,
    pub(crate) handle: Option<JoinHandle<Result<Error, Aborted>>>,
    pub(crate) abort_handle: Option<AbortHandle>,
    pub(crate) outputs_callbacks_handle: Option<JoinHandle<Result<Error, Aborted>>>,
    pub(crate) outputs_callbacks_abort_handle: Option<AbortHandle>,
    pub(crate) inputs_callbacks_handle: Option<JoinHandle<Result<Error, Aborted>>>,
    pub(crate) inputs_callbacks_abort_handle: Option<AbortHandle>,
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
        inputs: Inputs,
        outputs: Outputs,
    ) -> Self {
        // TODO Check that all ports are used.
        Self {
            id: operator.id,
            context: Context::new(&instance_context),
            configuration: operator.configuration,
            inputs,
            outputs,
            operator: operator.operator,
            _library: operator.library,
            handle: None,
            abort_handle: None,
            outputs_callbacks_handle: None,
            outputs_callbacks_abort_handle: None,
            inputs_callbacks_handle: None,
            inputs_callbacks_abort_handle: None,
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
            abort_handle.abort();
            if let Some(handle) = self.handle.take() {
                log::trace!("Operator handler finished with {:?}", handle.await);
            }
        }

        if let Some(cb_senders_handle) = self.outputs_callbacks_abort_handle.take() {
            cb_senders_handle.abort();
            if let Some(handle) = self.outputs_callbacks_handle.take() {
                log::trace!(
                    "Operator callback sender handler finished with {:?}",
                    handle.await
                );
            }
        }

        if let Some(cb_receivers_handle) = self.inputs_callbacks_abort_handle.take() {
            cb_receivers_handle.abort();
            if let Some(handle) = self.inputs_callbacks_handle.take() {
                log::trace!(
                    "Operator callback receiver handler finished with {:?}",
                    handle.await
                );
            }
        }

        Ok(())
    }

    async fn is_running(&self) -> bool {
        self.handle.is_some()
            || self.inputs_callbacks_handle.is_some()
            || self.outputs_callbacks_handle.is_some()
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
        let outputs_callbacks = std::mem::take(&mut self.context.outputs_callbacks);
        if !outputs_callbacks.is_empty() {
            let operator_id = self.id.clone();

            let callbacks_loop = async move {
                let mut running_callbacks = outputs_callbacks
                    .iter()
                    .enumerate()
                    .map(|(index, callback)| Box::pin(callback.run(index)))
                    .collect::<Vec<_>>();

                loop {
                    let (res, _, remainings) = futures::future::select_all(running_callbacks).await;

                    running_callbacks = remainings;
                    match res {
                        Ok(index) => {
                            let finished_callback = &outputs_callbacks[index];
                            running_callbacks.push(Box::pin(finished_callback.run(index)));
                        }
                        Err(e) => {
                            log::error!(
                                "[Operator: {}] Output callback error: {:?}",
                                operator_id,
                                e
                            );
                            return e;
                        }
                    };
                }
            };

            let (cb_abort_handle, cb_abort_registration) = AbortHandle::new_pair();
            let cb_handle =
                async_std::task::spawn(Abortable::new(callbacks_loop, cb_abort_registration));

            self.outputs_callbacks_handle = Some(cb_handle);
            self.outputs_callbacks_abort_handle = Some(cb_abort_handle);
        }

        let inputs_callbacks = std::mem::take(&mut self.context.inputs_callbacks);
        if !inputs_callbacks.is_empty() {
            let operator_id = self.id.clone();

            let callbacks_loop = async move {
                let mut running_callbacks = inputs_callbacks
                    .iter()
                    .enumerate()
                    .map(|(index, callback)| Box::pin(callback.run(index)))
                    .collect::<Vec<_>>();

                loop {
                    let (res, _, remainings) = futures::future::select_all(running_callbacks).await;

                    running_callbacks = remainings;

                    match res {
                        Ok(index) => {
                            let finished_callback = &inputs_callbacks[index];
                            running_callbacks.push(Box::pin(finished_callback.run(index)));
                        }
                        Err(e) => {
                            log::error!(
                                "[Operator: {}] Input callback error: {:?}",
                                operator_id,
                                e
                            );
                            return e;
                        }
                    }
                }
            };

            let (cb_abort_handle, cb_abort_registration) = AbortHandle::new_pair();
            let cb_handle =
                async_std::task::spawn(Abortable::new(callbacks_loop, cb_abort_registration));
            self.inputs_callbacks_handle = Some(cb_handle);
            self.inputs_callbacks_abort_handle = Some(cb_abort_handle);
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

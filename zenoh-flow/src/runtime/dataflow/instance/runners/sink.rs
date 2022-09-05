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

use crate::prelude::Inputs;
use crate::runtime::dataflow::instance::runners::{Runner, RunnerKind};
use crate::runtime::dataflow::node::SinkLoaded;
use crate::runtime::InstanceContext;
use crate::traits::Sink;
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

/// The `SinkRunner` is the component in charge of executing the sink.
/// It contains all the runtime information for the sink, the graph instance.
///
/// Do not reorder the fields in this struct.
/// Rust drops fields in a struct in the same order they are declared.
/// Ref: <https://doc.rust-lang.org/reference/destructors.html>
/// We need the state to be dropped before the sink/lib, otherwise we
/// will have a SIGSEV.
pub struct SinkRunner {
    pub(crate) id: NodeId,
    pub(crate) context: Context,
    pub(crate) configuration: Option<Configuration>,
    pub(crate) inputs: Inputs,
    pub(crate) sink: Arc<dyn Sink>,
    pub(crate) _library: Option<Arc<Library>>,
    pub(crate) handle: Option<JoinHandle<Result<Error, Aborted>>>,
    pub(crate) abort_handle: Option<AbortHandle>,
    pub(crate) inputs_callbacks_handle: Option<JoinHandle<Result<Error, Aborted>>>,
    pub(crate) inputs_callbacks_abort_handle: Option<AbortHandle>,
}

impl SinkRunner {
    /// Tries to create a new `SinkRunner` using the given
    /// [`InstanceContext`](`InstanceContext`), [`SinkLoaded`](`SinkLoaded`)
    /// and [`OperatorIO`](`OperatorIO`).
    ///
    /// # Errors
    /// If fails if the input is not connected.
    pub fn new(instance_context: Arc<InstanceContext>, sink: SinkLoaded, inputs: Inputs) -> Self {
        Self {
            id: sink.id,
            configuration: sink.configuration,
            context: Context::new(instance_context),
            inputs,
            sink: sink.sink,
            _library: sink.library,
            handle: None,
            abort_handle: None,
            inputs_callbacks_handle: None,
            inputs_callbacks_abort_handle: None,
        }
    }
}

#[async_trait]
impl Runner for SinkRunner {
    fn get_id(&self) -> NodeId {
        self.id.clone()
    }
    fn get_kind(&self) -> RunnerKind {
        RunnerKind::Sink
    }

    async fn is_running(&self) -> bool {
        self.handle.is_some() || self.inputs_callbacks_handle.is_some()
    }

    async fn stop(&mut self) -> ZFResult<()> {
        // Stop is idempotent, if the node was already stopped,
        // do nothing and return Ok(())

        if let Some(abort_handle) = self.abort_handle.take() {
            abort_handle.abort();

            if let Some(handle) = self.handle.take() {
                log::trace!("Sink handler finished with {:?}", handle.await);
            }
        }

        if let Some(abort_sender_handle) = self.inputs_callbacks_abort_handle.take() {
            abort_sender_handle.abort();

            if let Some(handle) = self.inputs_callbacks_handle.take() {
                log::trace!("Source callbacks handler finished with {:?}", handle.await);
            }
        }

        Ok(())
    }

    async fn start(&mut self) -> ZFResult<()> {
        // Start is idempotent, if the node was already started,
        // do nothing and return Ok(())
        if self.handle.is_some() && self.abort_handle.is_some() {
            log::warn!(
                "[Sink: {}] Trying to start while it is already started, aborting",
                self.id
            );
            return Ok(());
        }

        log::trace!("[Sink: {}] Starting", self.id);

        let iteration = self
            .sink
            .setup(&mut self.context, &self.configuration, self.inputs.clone())
            .await?;

        /* Callbacks */
        let inputs_callbacks = std::mem::take(&mut self.context.inputs_callbacks);
        if !inputs_callbacks.is_empty() {
            let sink_id = Arc::clone(&self.id);

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
                            log::error!("[Sink: {}] Input callback error: {:?}", sink_id, e);
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
                        log::error!("[Sink: {c_id}] {:?}", e);
                        return e;
                    }

                    log::trace!(
                        "[Sink: {c_id}] iteration took: {}ms",
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

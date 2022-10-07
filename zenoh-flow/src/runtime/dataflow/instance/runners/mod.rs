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

pub mod connector;
pub mod operator;
pub mod replay;
pub mod sink;
pub mod source;

use std::sync::Arc;
use std::time::Instant;

use crate::prelude::NodeId;
use crate::traits::Node;
use crate::types::io::{CallbackInput, CallbackOutput};
use crate::zfresult::Error;
use crate::Result as ZFResult;
use async_std::task::JoinHandle;
use async_trait::async_trait;
use futures::future::{AbortHandle, Abortable, Aborted};

/// Type of the Runner.
///
/// The runner is the one actually running the nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunnerKind {
    Source,
    Operator,
    Sink,
    Connector,
}

/// Action to be taken depending on the result of the run.
pub enum RunAction {
    RestartRun(Option<Error>),
    Stop,
}

/// This traits abstracts the runners, it provides the functions that are
/// common across the runners.
///
///
#[async_trait]
pub trait Runner: Send + Sync {
    /// The actual run where the magic happens.
    ///
    /// # Errors
    /// It can fail to indicate that something went wrong when executing the node.
    async fn start(&mut self) -> ZFResult<()>;

    /// Returns the type of the runner.
    fn get_kind(&self) -> RunnerKind;

    /// Returns the `NodeId` of the runner.
    fn get_id(&self) -> NodeId;

    /// Checks if the `Runner` is running.
    async fn is_running(&self) -> bool;

    /// Stops the runner.
    async fn stop(&mut self) -> ZFResult<()>;
}

/// TODO(J-Loudet) Improve documentation.
///
/// A Runner2 takes care of running a node. Regardless of what it is. No need for a trait, no need
/// for a separation between the types of nodes.
pub(crate) struct Runner2 {
    pub(crate) node: Option<Arc<dyn Node>>,
    pub(crate) inputs_callbacks: Vec<CallbackInput>,
    pub(crate) outputs_callbacks: Vec<CallbackOutput>,
    pub(crate) run_loop_handle: Option<JoinHandle<Result<Error, Aborted>>>,
    pub(crate) run_loop_abort_handle: Option<AbortHandle>,
    pub(crate) outputs_callbacks_handle: Option<JoinHandle<Result<Error, Aborted>>>,
    pub(crate) outputs_callbacks_abort_handle: Option<AbortHandle>,
    pub(crate) inputs_callbacks_handle: Option<JoinHandle<Result<Error, Aborted>>>,
    pub(crate) inputs_callbacks_abort_handle: Option<AbortHandle>,
}

impl Runner2 {
    pub(crate) fn new(
        node: Option<Arc<dyn Node>>,
        inputs_callbacks: Vec<CallbackInput>,
        outputs_callbacks: Vec<CallbackOutput>,
    ) -> Self {
        Self {
            node,
            inputs_callbacks,
            outputs_callbacks,
            run_loop_handle: None,
            run_loop_abort_handle: None,
            outputs_callbacks_handle: None,
            outputs_callbacks_abort_handle: None,
            inputs_callbacks_handle: None,
            inputs_callbacks_abort_handle: None,
        }
    }

    /// TODO(J-Loudet) Improve documentation.
    ///
    /// First, `start` is idempotent: no matter how many times this method is called, if the node is
    /// running then nothing happens.
    ///
    /// This method creates as many `AbortHandle` as required to run what the user provided: (i) an
    /// iteration, (ii) input_callbacks, (iii) output_callbacks.
    ///
    /// Considering all are present, we spawn a task for each and call their respective methods in
    /// an infinite loop.
    ///
    ///
    /// # Issues
    ///
    /// FIXME Assuming there are several callbacks for the same kind (i.e. input or output), if one
    ///       of these callbacks never finishes, then all the others will starve. For instance, if
    ///       an input callbacks sleeps (std::thread::sleep) for 10 seconds, then the other inputs
    ///       won’t have access to the CPU for these 10 seconds.
    ///       A possible fix would be to spawn a thread per callback. What’s the impact?
    pub(crate) fn start(&mut self) {
        if self.is_running() {
            log::warn!("Called `start` while node is ALREADY running. Returning.");
            return;
        }

        if let Some(node) = &self.node {
            let node = Arc::clone(node);
            let run_loop = async move {
                let mut instant: Instant;
                loop {
                    instant = Instant::now();
                    if let Err(e) = node.iteration().await {
                        log::error!("Iteration error: {:?}", e);
                        return e;
                    }

                    log::trace!("iteration took: {}ms", instant.elapsed().as_millis());

                    async_std::task::yield_now().await;
                }
            };

            let (abort_handle, abort_registration) = AbortHandle::new_pair();
            let handle = async_std::task::spawn(Abortable::new(run_loop, abort_registration));

            self.run_loop_handle = Some(handle);
            self.run_loop_abort_handle = Some(abort_handle);
        }

        if !self.outputs_callbacks.is_empty() {
            // TODO How is that clone "safe"? The underlying channels are "job stealing" so only the
            // last copy actually gets the data?
            let outputs_callbacks = self.outputs_callbacks.clone();
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
                            log::error!("Output callback error: {:?}", e);
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

        if !self.inputs_callbacks.is_empty() {
            // TODO How is that clone "safe"? The underlying channels are "job stealing" so only the
            // last copy actually gets the data?
            let inputs_callbacks = self.inputs_callbacks.clone();
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
                            log::error!("Input callback error: {:?}", e);
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
    }

    /// TODO(J-Loudet) Improve documentation.
    ///
    /// `pause` is idempotent: if the node is not running then calling stop will produce no result.
    ///
    /// `pause` takes all handles, stops the associated tasks & replaces them with `None`.
    ///
    /// `pause` and not "stop" because we do not drop the `dyn Node` so its state is not lost.
    pub(crate) async fn _pause(&mut self) -> ZFResult<()> {
        if !self.is_running() {
            log::warn!("Called `stop` while node is NOT running. Returning.");
            return Ok(()); // TODO Return an error instead?
        }

        if let Some(abort_handle) = self.run_loop_abort_handle.take() {
            abort_handle.abort();
            if let Some(handle) = self.run_loop_handle.take() {
                log::trace!("Handler finished with {:?}", handle.await);
            }
        }

        if let Some(cb_senders_handle) = self.outputs_callbacks_abort_handle.take() {
            cb_senders_handle.abort();
            if let Some(handle) = self.outputs_callbacks_handle.take() {
                log::trace!("Output callback handler finished with {:?}", handle.await);
            }
        }

        if let Some(cb_receivers_handle) = self.inputs_callbacks_abort_handle.take() {
            cb_receivers_handle.abort();
            if let Some(handle) = self.inputs_callbacks_handle.take() {
                log::trace!("Input callback handler finished with {:?}", handle.await);
            }
        }

        Ok(())
    }

    /// Tell if the node is running.
    ///
    /// To do so we check if an abort handle was set. If so, then a task was spawned and the node is
    /// indeed running.
    pub(crate) fn is_running(&self) -> bool {
        self.run_loop_handle.is_some()
            || self.inputs_callbacks_handle.is_some()
            || self.outputs_callbacks_handle.is_some()
    }
}

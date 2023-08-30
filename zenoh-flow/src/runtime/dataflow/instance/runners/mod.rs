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

pub mod connector;

use crate::traits::Node;
use crate::zfresult::Error;
use crate::Result as ZFResult;
use async_std::task::JoinHandle;
use futures::future::{AbortHandle, Abortable, Aborted};
use std::sync::Arc;
use std::time::Instant;

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

/// A `Runner` takes care of running a `Node`.
///
/// It spawns an abortable task in which the `iteration` is called in a loop, indefinitely.
pub(crate) struct Runner {
    pub(crate) node: Arc<dyn Node>,
    pub(crate) run_loop_handle: Option<JoinHandle<Result<Error, Aborted>>>,
    pub(crate) run_loop_abort_handle: Option<AbortHandle>,
}

impl Runner {
    pub(crate) fn new(node: Arc<dyn Node>) -> Self {
        Self {
            node,
            run_loop_handle: None,
            run_loop_abort_handle: None,
        }
    }

    /// Start the `Runner`, spawning an abortable task.
    ///
    /// `start` is idempotent and will do nothing if the node is already running.
    pub(crate) fn start(&mut self) {
        if self.is_running() {
            log::warn!("Called `start` while node is ALREADY running. Returning.");
            return;
        }

        let node = self.node.clone();
        let run_loop = async move {
            let mut instant: Instant;
            loop {
                instant = Instant::now();
                log::trace!("Iteration start: {:?}", instant);
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

    /// Stop the execution of a `Node`.
    ///
    /// We will call `abort` on the `AbortHandle` and then `await` the `JoinHandle`. As per its
    /// documentation, `abort` will not forcefully interrupt an execution if the corresponding task
    /// is being polled on another thread.
    ///
    /// `stop` is idempotent and will do nothing if the node is not running.
    pub(crate) async fn stop(&mut self) -> ZFResult<()> {
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

        Ok(())
    }

    /// Tell if the node is running.
    ///
    /// To do so we check if an `AbortHandle` was set. If so, then a task was spawned and the node
    /// is indeed running.
    pub(crate) fn is_running(&self) -> bool {
        self.run_loop_handle.is_some()
    }
}

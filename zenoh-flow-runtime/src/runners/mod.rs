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

pub(crate) mod builtin;

#[cfg(feature = "zenoh")]
pub(crate) mod connectors;

use async_std::task::JoinHandle;
use futures::future::{AbortHandle, Abortable, Aborted};
use std::sync::Arc;
use std::time::Instant;
use zenoh_flow_commons::NodeId;
use zenoh_flow_nodes::prelude::Node;

/// A `Runner` takes care of running a `Node`.
///
/// It spawns an abortable task in which the `iteration` is called in a loop, indefinitely.
pub(crate) struct Runner {
    pub(crate) id: NodeId,
    pub(crate) node: Arc<dyn Node>,
    pub(crate) run_loop_handle: Option<JoinHandle<std::result::Result<anyhow::Error, Aborted>>>,
    pub(crate) run_loop_abort_handle: Option<AbortHandle>,
}

impl Runner {
    pub(crate) fn new(id: NodeId, node: Arc<dyn Node>) -> Self {
        Self {
            id,
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
            tracing::warn!(
                "[{}] Called `start` while node is already running. Returning.",
                self.id
            );
            return;
        }

        let node = self.node.clone();
        let id = self.id.clone();
        let run_loop = async move {
            let mut instant: Instant;
            loop {
                instant = Instant::now();
                if let Err(e) = node.iteration().await {
                    tracing::error!("[{}] Iteration error: {:?}", id, e);
                    return e;
                }

                tracing::trace!(
                    "[{}] iteration took: {}µs",
                    id,
                    instant.elapsed().as_micros()
                );

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
    pub(crate) async fn abort(&mut self) {
        if !self.is_running() {
            tracing::warn!(
                "[{}] Called `abort` while node is NOT running. Returning.",
                self.id
            );
            return;
        }

        if let Some(abort_handle) = self.run_loop_abort_handle.take() {
            abort_handle.abort();
            if let Some(handle) = self.run_loop_handle.take() {
                tracing::trace!("[{}] Handler finished with {:?}", self.id, handle.await);
            }
        }
    }

    /// Tell if the node is running.
    ///
    /// To do so we check if an `AbortHandle` was set. If so, then a task was spawned and the node
    /// is indeed running.
    pub(crate) fn is_running(&self) -> bool {
        self.run_loop_handle.is_some()
    }
}

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

use anyhow::Context;
use async_std::task::JoinHandle;
use libloading::Library;
use std::sync::Arc;
use std::time::Instant;
use tracing::Instrument;
use zenoh_flow_commons::{NodeId, Result};
use zenoh_flow_nodes::prelude::Node;

enum State {
    Uninitialized,
    Initialized,
}

/// A `Runner` takes care of running a `Node`.
///
/// Each Runner runs in a separate task.
pub(crate) struct Runner {
    pub(crate) id: NodeId,
    node: Arc<dyn Node>,
    state: State,
    handle: Option<JoinHandle<()>>,
    // The `_library` field is used solely for its `Arc`. We need to keep track of how many `Runners` are using the
    // `Library` such that once that number reaches 0, we drop the library.
    //
    // The `Option` exists because only user-implemented nodes have a `Library`. For example, built-in Zenoh Source /
    // Sink and the connectors have no `Library`.
    _library: Option<Arc<Library>>,
}

impl Runner {
    /// Creates a runner, spawning a [Task](async_std::task::Task) that will poll, in a loop, the `iteration` of the
    /// underlying [Node].
    //
    // The control logic is relatively simple:
    // 1. When `start` is called, create a task that will poll indefinitely, in a loop, the `iteration` method of the
    //    Node.
    // 2. Keep a reference to that task through its JoinHandle so we can call `cancel` to stop it.
    pub(crate) fn new(id: NodeId, node: Arc<dyn Node>, library: Option<Arc<Library>>) -> Self {
        Self {
            id,
            node,
            state: State::Uninitialized,
            handle: None,
            _library: library,
        }
    }

    /// Returns `true` if the Runner is running, i.e. if the `iteration` of the [Node] it wraps is being polled in a
    /// loop.
    pub(crate) fn is_running(&self) -> bool {
        self.handle.is_some()
    }

    /// Starts the runner: run the `iteration` method of the [Node] it wraps in a loop.
    ///
    /// This method is also idempotent: if the runner is already running, nothing will happen.
    pub(crate) async fn start(&mut self) -> Result<()> {
        if self.is_running() {
            return Ok(());
        }

        if matches!(self.state, State::Initialized) {
            self.node
                .on_resume()
                .await
                .with_context(|| format!("{}: call to `on_resume` failed", self.id))?;
        }

        let id = self.id.clone();
        let node = self.node.clone();
        let iteration_span = tracing::trace_span!("iteration", node = %id);

        self.handle = Some(async_std::task::spawn(
            async move {
                let mut instant;
                let mut iteration;
                loop {
                    instant = Instant::now();
                    iteration = node.iteration().await;
                    tracing::trace!("duration: {}µs", instant.elapsed().as_micros());
                    if let Err(e) = iteration {
                        tracing::error!("{:?}", e);
                    }

                    async_std::task::yield_now().await;
                }
            }
            .instrument(iteration_span),
        ));

        self.state = State::Initialized;
        Ok(())
    }

    /// Aborts the runner: stop the execution of its `iteration` method at its nearest `await` point.
    ///
    /// This method is idempotent: if the runner is not running, nothing will happen.
    ///
    /// # Warning
    ///
    /// This method will *drop* the future driving the execution of the current `iteration`. This will effectively
    /// cancel it at its nearest next `.await` point in its code.
    ///
    /// What this also means is that there is a possibility to leave the node in an **inconsistent state**. For
    /// instance, modified values that are not saved between several `.await` points would be lost if the node is
    /// aborted.
    pub(crate) async fn abort(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.cancel().await;
            self.node.on_abort().await;
        }
    }
}

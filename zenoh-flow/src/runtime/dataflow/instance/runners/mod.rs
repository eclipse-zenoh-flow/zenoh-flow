//
// Copyright (c) 2017, 2021 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//

pub mod connector;
pub mod operator;
pub mod replay;
pub mod sink;
pub mod source;

use crate::async_std::prelude::*;
use crate::async_std::sync::Arc;
use crate::async_std::task::JoinHandle;

use crate::runtime::dataflow::instance::link::{LinkReceiver, LinkSender};
use crate::runtime::message::Message;
use crate::runtime::InstanceContext;
use crate::types::{NodeId, ZFResult};
use crate::{PortId, PortType, ZFError};
use async_trait::async_trait;
use futures_lite::future::FutureExt;
use std::collections::HashMap;
use std::ops::Deref;
use std::pin::Pin;
use std::task::{Context, Poll};
use zenoh_sync::Signal;

/// Type of the Runner.
///
/// The runner is the one actually running the nodes.
#[derive(Clone, Debug, PartialEq)]
pub enum RunnerKind {
    Source,
    Operator,
    Sink,
    Connector,
}

/// A runner manager is created when a `Runner` is started.
///
/// The runner manager is used to send commands to the runner.
pub struct RunnerManager {
    stopper: Signal,
    handler: JoinHandle<ZFResult<()>>,
    runner: Arc<dyn Runner>,
    ctx: InstanceContext,
}

impl RunnerManager {
    /// Creates a new `RunnerManager` associated with the given `runner`.
    ///
    /// It is able to communicate via the `stopper` and the `handler`.
    pub fn new(
        stopper: Signal,
        handler: JoinHandle<ZFResult<()>>,
        runner: Arc<dyn Runner>,
        ctx: InstanceContext,
    ) -> Self {
        Self {
            stopper,
            handler,
            runner,
            ctx,
        }
    }

    /// Stops the associated runner.
    ///
    /// # Errors
    /// An error variant is returned in case the runner is already stopped.
    pub async fn kill(&self) -> ZFResult<()> {
        if self.runner.is_recording().await {
            self.runner.stop_recording().await?;
        }
        self.stopper.trigger();
        Ok(())
    }

    /// Returns a reference to the handler.
    ///
    /// The handler can be used to verify the exit value
    /// of a `Runner`.
    ///
    /// # Errors
    /// An error variant is returned in case the run failed.
    pub fn get_handler(&self) -> &JoinHandle<ZFResult<()>> {
        &self.handler
    }

    /// Starts the recording of the associated `Runner`.
    ///
    /// # Errors
    /// Fails if the `Runner` is not a source.
    pub async fn start_recording(&self) -> ZFResult<String> {
        self.runner.start_recording().await
    }

    /// Stops the recording for the associated `Runner`.
    ///
    /// # Errors
    /// Fails if the `Runner` is not a source.
    pub async fn stop_recording(&self) -> ZFResult<String> {
        self.runner.stop_recording().await
    }

    /// Returns a reference to the instance context.
    pub fn get_context(&self) -> &InstanceContext {
        &self.ctx
    }
}

impl Deref for RunnerManager {
    type Target = Arc<dyn Runner>;

    fn deref(&self) -> &Self::Target {
        &self.runner
    }
}

impl Future for RunnerManager {
    type Output = ZFResult<()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        self.handler.poll(ctx)
    }
}

/// Action to be taken depending on the result of the run.
pub enum RunAction {
    RestartRun(Option<ZFError>),
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
    /// It can fail to indicate that something went wrong when executing the
    /// node.
    async fn run(&self) -> ZFResult<()>;

    /// Adds an input to the runner.
    ///
    /// # Errors
    /// It may fail if the runner is not supposed to have that input or
    /// if it does not expect any input (eg. Source)
    async fn add_input(&self, input: LinkReceiver<Message>) -> ZFResult<()>;

    /// Adds an output to the runner.
    ///
    /// # Errors
    /// It may fail if the runner is not supposed to have that output or
    /// if it does not expect any outputs (e.g.  Sink)
    async fn add_output(&self, output: LinkSender<Message>) -> ZFResult<()>;

    /// Finalizes the node
    ///
    /// # Errors
    /// Returns an error variant if the finalize fails.
    async fn clean(&self) -> ZFResult<()>;

    /// Returns the type of the runner.
    fn get_kind(&self) -> RunnerKind;

    /// Returns the `NodeId` of the runner.
    fn get_id(&self) -> NodeId;

    /// Returns the inputs of the `Runner`.
    fn get_inputs(&self) -> HashMap<PortId, PortType>;

    /// Returns the outputs of the `Runner`.
    fn get_outputs(&self) -> HashMap<PortId, PortType>;

    /// Returns the output link of the `Runner.
    async fn get_outputs_links(&self) -> HashMap<PortId, Vec<LinkSender<Message>>>;

    /// Returns the input link of the `Runner`.
    async fn take_input_links(&self) -> HashMap<PortId, LinkReceiver<Message>>;

    /// Starts the recording of the `Runner`
    ///
    /// # Errors
    /// Fails if the `Runner` is not a source
    async fn start_recording(&self) -> ZFResult<String>;

    /// Stops the recording of the `Runner`.
    ///
    /// # Errors
    /// Fails it the `Runner` is not a source.
    async fn stop_recording(&self) -> ZFResult<String>;

    /// Checks if the `Runner` is recording.
    ///
    /// # Errors
    /// Always `false` if the runner is not a source.
    async fn is_recording(&self) -> bool;

    /// Checks if the `Runner` is running.
    async fn is_running(&self) -> bool;

    /// Stops the runner.
    async fn stop(&self);
}

/// A `NodeRunner` wraps the `Runner and associates it
/// with an `InstanceContext`
#[derive(Clone)]
pub struct NodeRunner {
    inner: Arc<dyn Runner>,
    ctx: InstanceContext,
}

impl NodeRunner {
    /// Creates a new `NodeRunner`.
    pub fn new(inner: Arc<dyn Runner>, ctx: InstanceContext) -> Self {
        Self { inner, ctx }
    }

    /// Run the node in a stoppable fashion.
    ///
    ///  # Errors
    /// An error variant is returned in case the run returns an error.
    fn run_stoppable(&self, signal: Signal) -> ZFResult<()> {
        async fn run(runner: &NodeRunner) -> RunAction {
            match runner.run().await {
                Ok(_) => RunAction::Stop,
                Err(e) => RunAction::RestartRun(Some(e)),
            }
        }

        async fn stop(signal: Signal) -> RunAction {
            signal.wait().await;
            RunAction::Stop
        }
        async_std::task::block_on(async move {
            loop {
                let cloned_signal = signal.clone();
                match stop(cloned_signal).race(run(self)).await {
                    RunAction::RestartRun(e) => {
                        log::error!(
                            "[Node: {}] The run loop exited with {:?}, restarting…",
                            self.get_id(),
                            e
                        );
                    }
                    RunAction::Stop => {
                        log::trace!(
                            "[Node: {}] Received kill command, killing runner",
                            self.get_id()
                        );
                        self.stop().await;
                        return Ok(());
                    }
                }
            }
        })
    }

    /// Starts the node, returning the `RunnerManager` to stop it.
    pub fn start(&self) -> RunnerManager {
        let signal = Signal::new();
        let cloned_self = self.clone();
        let cloned_signal = signal.clone();

        let h = async_std::task::spawn_blocking(move || cloned_self.run_stoppable(cloned_signal));

        RunnerManager::new(signal, h, self.inner.clone(), self.ctx.clone())
    }
}

impl Deref for NodeRunner {
    type Target = Arc<dyn Runner>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

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

use crate::error::ZFError;
use crate::types::{NodeId, ZFResult};
use async_trait::async_trait;

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

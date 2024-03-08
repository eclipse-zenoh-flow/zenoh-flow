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

use crate::runners::Runner;

use std::{collections::HashMap, fmt::Display, ops::Deref};

use serde::{Deserialize, Serialize};
use uhlc::{Timestamp, HLC};
use zenoh_flow_commons::{NodeId, Result, RuntimeId};
use zenoh_flow_records::DataFlowRecord;

/// A `DataFlowInstance` is the ready-to-run (possibly running) instance of a [data flow](DataFlowRecord).
///
/// A Zenoh-Flow [runtime](crate::Runtime) will provide access to each instance in a concurrency safe manner:
/// considering the distributed nature of Zenoh-Flow, concurrent requests on the same instance can be made.
pub struct DataFlowInstance {
    pub(crate) state: InstanceState,
    pub(crate) record: DataFlowRecord,
    pub(crate) runners: HashMap<NodeId, Runner>,
}

/// The different states of a [DataFlowInstance].
///
/// Note that *a state is tied to a Zenoh-Flow [runtime]*: if a data flow is distributed across multiple Zenoh-Flow
/// runtimes, their respective state for the same instance could be different (but should eventually converge).
///
/// [runtime]: crate::Runtime
#[derive(Clone, Deserialize, Serialize, Debug)]
pub enum InstanceState {
    Creating(Timestamp),
    /// A [runtime] listing a [DataFlowInstance] in the `Loaded` state successfully instantiated all the nodes it manages
    /// and is ready to start them.
    ///
    /// A `Loaded` data flow can be started or deleted.
    ///
    /// [runtime]: crate::Runtime
    Loaded(Timestamp),
    /// A [runtime] listing a [DataFlowInstance] in the `Running` state has (re)started all the nodes it manages.
    ///
    /// A `Running` data flow can be aborted or deleted.
    ///
    /// [runtime]: crate::Runtime
    Running(Timestamp),
    /// A [runtime] listing a [DataFlowInstance] in the `Aborted` state has abruptly stopped all the nodes it manages.
    ///
    /// An `Aborted` data flow can be restarted or deleted.
    ///
    /// [runtime]: crate::Runtime
    Aborted(Timestamp),
    Failed((Timestamp, String)),
}

impl Display for InstanceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InstanceState::Creating(ts) => write!(f, "Creation started on {}", ts.get_time()),
            InstanceState::Loaded(ts) => write!(f, "Loaded on {}", ts.get_time()),
            InstanceState::Running(ts) => write!(f, "Running since {}", ts.get_time()),
            InstanceState::Aborted(ts) => write!(f, "Aborted on {}", ts.get_time()),
            InstanceState::Failed((ts, reason)) => {
                write!(f, "Failed on {} with:\n{}", ts.get_time(), reason)
            }
        }
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct InstanceStatus {
    pub runtime_id: RuntimeId,
    pub state: InstanceState,
    pub nodes: Vec<NodeId>,
}

impl Deref for DataFlowInstance {
    type Target = DataFlowRecord;

    fn deref(&self) -> &Self::Target {
        &self.record
    }
}

impl DataFlowInstance {
    pub fn new(record: DataFlowRecord, hlc: &HLC) -> Self {
        Self {
            state: InstanceState::Creating(hlc.new_timestamp()),
            record,
            runners: HashMap::default(),
        }
    }

    pub async fn start(&mut self, hlc: &HLC) -> Result<()> {
        for (node_id, runner) in self.runners.iter_mut() {
            runner.start().await?;
            tracing::trace!("Started node < {} >", node_id);
        }

        self.state = InstanceState::Running(hlc.new_timestamp());
        Ok(())
    }

    pub async fn abort(&mut self, hlc: &HLC) {
        for (node_id, runner) in self.runners.iter_mut() {
            runner.abort().await;
            tracing::trace!("Aborted node < {} >", node_id);
        }

        self.state = InstanceState::Aborted(hlc.new_timestamp());
    }

    pub fn state(&self) -> &InstanceState {
        &self.state
    }

    pub fn status(&self, runtime_id: &RuntimeId) -> InstanceStatus {
        InstanceStatus {
            runtime_id: runtime_id.clone(),
            state: self.state.clone(),
            nodes: self
                .runners
                .keys()
                .filter(|&node_id| {
                    !(self.senders().contains_key(node_id)
                        || self.receivers().contains_key(node_id))
                })
                .cloned()
                .collect(),
        }
    }
}

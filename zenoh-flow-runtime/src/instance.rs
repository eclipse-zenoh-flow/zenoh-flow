//
// Copyright (c) 2021 - 2024 ZettaScale Technology
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

use std::{collections::HashMap, fmt::Display, ops::Deref};

use serde::{Deserialize, Serialize};
use uhlc::{Timestamp, HLC};
use zenoh_flow_commons::{NodeId, Result, RuntimeId};
use zenoh_flow_records::DataFlowRecord;

use crate::runners::Runner;

/// A `DataFlowInstance` keeps track of the parts of a data flow managed by the Zenoh-Flow runtime.
///
/// A `DataFlowInstance` structure is thus *local* to a Zenoh-Flow runtime. For a data flow that spawns on multiple
/// runtimes, there will be one such structure at each runtime.
///
/// All instances will share the same [record](DataFlowRecord) but their internal state will differ.
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
    /// A [runtime] listing a [DataFlowInstance] in the `Creating` state is in the process of loading all the nodes it
    /// manages.
    ///
    /// [runtime]: crate::Runtime
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
    /// A [runtime] listing a [DataFlowInstance] in the `Failed` state failed to load at least one of the nodes of this
    /// instance it manages.
    ///
    /// A data flow in the `Failed` state can only be deleted.
    ///
    /// [runtime]: crate::Runtime
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

/// The `InstanceStatus` provides information about the data flow instance.
///
/// It details:
/// - from which runtime this information comes from (through its [identifier](RuntimeId)),
/// - the [state](InstanceState) of the data flow instance,
/// - the list of nodes (through their [identifier](NodeId)) the runtime manages --- and thus for which the state
///   applies.
///
/// This information is what is displayed by the `zfctl` tool when requesting the status of a data flow instance.
#[derive(Deserialize, Serialize, Debug)]
pub struct InstanceStatus {
    /// The identifier of the [runtime](crate::Runtime) this information comes from.
    pub runtime_id: RuntimeId,
    /// The state of the data flow instance --- on this runtime.
    pub state: InstanceState,
    /// The nodes managed by this runtime, for which the state applies.
    pub nodes: Vec<NodeId>,
}

impl Deref for DataFlowInstance {
    type Target = DataFlowRecord;

    fn deref(&self) -> &Self::Target {
        &self.record
    }
}

impl DataFlowInstance {
    /// Creates a new `DataFlowInstance`, setting its state to [Creating](InstanceState::Creating).
    pub(crate) fn new(record: DataFlowRecord, hlc: &HLC) -> Self {
        Self {
            state: InstanceState::Creating(hlc.new_timestamp()),
            record,
            runners: HashMap::default(),
        }
    }

    /// (re-)Starts the `DataFlowInstance`.
    ///
    /// The [hlc](HLC) is required to keep track of when this call was made.
    ///
    /// # Errors
    ///
    /// This method can fail when attempting to re-start: when re-starting a data flow, the method
    /// [on_resume] is called for each node and is faillible.
    ///
    /// [on_resume]: zenoh_flow_nodes::prelude::Node::on_resume()
    pub async fn start(&mut self, hlc: &HLC) -> Result<()> {
        for (node_id, runner) in self.runners.iter_mut() {
            runner.start().await?;
            tracing::trace!("Started node < {} >", node_id);
        }

        self.state = InstanceState::Running(hlc.new_timestamp());
        Ok(())
    }

    /// Aborts the `DataFlowInstance`.
    ///
    /// The [hlc](HLC) is required to keep track of when this call was made.
    pub async fn abort(&mut self, hlc: &HLC) {
        for (node_id, runner) in self.runners.iter_mut() {
            runner.abort().await;
            tracing::trace!("Aborted node < {} >", node_id);
        }

        self.state = InstanceState::Aborted(hlc.new_timestamp());
    }

    /// Returns the [state](InstanceState) of this `DataFlowInstance`.
    pub fn state(&self) -> &InstanceState {
        &self.state
    }

    /// Returns the [status](InstanceStatus) of this `DataFlowInstance`.
    ///
    /// This structure was intended as a way to retrieve and display information about the instance. This is what the
    /// `zfctl` tool leverages for its `instance status` command.
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

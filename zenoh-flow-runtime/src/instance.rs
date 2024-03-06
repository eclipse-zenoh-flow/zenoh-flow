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
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display, ops::Deref};
use zenoh_flow_commons::{NodeId, Result, RuntimeId};
use zenoh_flow_records::DataFlowRecord;

pub struct DataFlowInstance {
    state: InstanceState,
    pub(crate) record: DataFlowRecord,
    pub(crate) runners: HashMap<NodeId, Runner>,
}

#[derive(Clone, Copy, Deserialize, Serialize, Debug)]
pub enum InstanceState {
    Loaded,
    Running,
    Aborted,
}

impl Display for InstanceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let repr = match self {
            InstanceState::Loaded => "Loaded",
            InstanceState::Running => "Running",
            InstanceState::Aborted => "Aborted",
        };

        write!(f, "{}", repr)
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
    pub fn new(record: DataFlowRecord) -> Self {
        Self {
            state: InstanceState::Loaded,
            record,
            runners: HashMap::default(),
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        for (node_id, runner) in self.runners.iter_mut() {
            runner.start().await?;
            tracing::trace!("Started node < {} >", node_id);
        }

        self.state = InstanceState::Running;
        Ok(())
    }

    pub async fn abort(&mut self) {
        for (node_id, runner) in self.runners.iter_mut() {
            runner.abort().await;
            tracing::trace!("Aborted node < {} >", node_id);
        }

        self.state = InstanceState::Aborted;
    }

    pub fn state(&self) -> &InstanceState {
        &self.state
    }

    pub fn status(&self, runtime_id: &RuntimeId) -> InstanceStatus {
        InstanceStatus {
            runtime_id: runtime_id.clone(),
            state: self.state,
            nodes: self
                .mapping()
                .get(runtime_id)
                .map(|node_ids| {
                    node_ids
                        .iter()
                        .filter(|&node_id| {
                            !(self.senders().contains_key(node_id)
                                || self.receivers().contains_key(node_id))
                        })
                        .cloned()
                        .collect()
                })
                .unwrap_or_default(),
        }
    }
}

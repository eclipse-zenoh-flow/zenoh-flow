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
use std::{collections::HashMap, ops::Deref};
use zenoh_flow_commons::NodeId;
use zenoh_flow_records::DataFlowRecord;

pub struct DataFlowInstance {
    status: InstanceStatus,
    pub(crate) record: DataFlowRecord,
    pub(crate) runners: HashMap<NodeId, Runner>,
}

#[derive(Clone, Copy, Deserialize, Serialize, Debug)]
pub enum InstanceStatus {
    Loaded,
    Running,
    Aborted,
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
            status: InstanceStatus::Loaded,
            record,
            runners: HashMap::default(),
        }
    }

    pub fn start(&mut self) {
        for (node_id, runner) in self.runners.iter_mut() {
            runner.start();
            tracing::trace!("Started node < {} >", node_id);
        }

        self.status = InstanceStatus::Running;
    }

    pub async fn abort(&mut self) {
        for (node_id, runner) in self.runners.iter_mut() {
            runner.abort().await;
            tracing::trace!("Aborted node < {} >", node_id);
        }

        self.status = InstanceStatus::Aborted;
    }

    pub fn status(&self) -> &InstanceStatus {
        &self.status
    }
}

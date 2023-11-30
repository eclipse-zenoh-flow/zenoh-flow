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
use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
};
use zenoh_flow_commons::NodeId;
use zenoh_flow_records::DataFlowRecord;

pub struct DataFlowInstance {
    pub(crate) record: DataFlowRecord,
    pub(crate) runners: HashMap<NodeId, Runner>,
}

impl Deref for DataFlowInstance {
    type Target = DataFlowRecord;

    fn deref(&self) -> &Self::Target {
        &self.record
    }
}

impl DataFlowInstance {
    pub fn start(&mut self) {
        for (node_id, runner) in self.runners.iter_mut() {
            runner.start();
            tracing::trace!("Started node < {} >", node_id);
        }
    }

    pub async fn abort(&mut self) {
        for (node_id, runner) in self.runners.iter_mut() {
            runner.abort().await;
            tracing::trace!("Aborted node < {} >", node_id);
        }
    }
}

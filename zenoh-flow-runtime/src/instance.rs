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
    pub fn start_sources(&mut self) {
        self.start_nodes(self.sources.keys().cloned().collect());
    }

    pub async fn stop_sources(&mut self) {
        self.stop_nodes(self.sources.keys().cloned().collect())
            .await;
    }

    pub fn start_operators(&mut self) {
        self.start_nodes(self.receivers.keys().cloned().collect());
        self.start_nodes(self.operators.keys().cloned().collect());
        self.start_nodes(self.senders.keys().cloned().collect());
    }

    pub async fn stop_operators(&mut self) {
        self.stop_nodes(self.senders.keys().cloned().collect())
            .await;
        self.stop_nodes(self.operators.keys().cloned().collect())
            .await;
        self.stop_nodes(self.receivers.keys().cloned().collect())
            .await;
    }

    pub fn start_sinks(&mut self) {
        self.start_nodes(self.sinks.keys().cloned().collect());
    }

    pub async fn stop_sinks(&mut self) {
        self.stop_nodes(self.sinks.keys().cloned().collect()).await;
    }

    fn start_nodes(&mut self, nodes: HashSet<NodeId>) {
        for (_, runner) in self
            .runners
            .iter_mut()
            .filter(|(runner_id, _)| nodes.contains(runner_id))
        {
            runner.start()
        }
    }

    async fn stop_nodes(&mut self, nodes: HashSet<NodeId>) {
        for (_, runner) in self
            .runners
            .iter_mut()
            .filter(|(runner_id, _)| nodes.contains(runner_id))
        {
            runner.stop().await
        }
    }
}

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

use serde::{Deserialize, Serialize};
use std::time::Duration;
use uhlc::Timestamp;

use crate::{model::loops::LoopDescriptor, NodeId};

/// A `LoopContext` is associated to each message that goes through a Loop.
///
/// It gives the following information:
/// - the ingress / egress,
/// - an iteration counter (if the Loop is finite),
/// - the starting timestamp of the first iteration,
/// - the starting timestamp of the current iteration,
/// - the duration of the last iteration (after the first full iteration is performed).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoopContext {
    pub(crate) ingress: NodeId,
    pub(crate) egress: NodeId,
    pub(crate) iteration: LoopIteration,
    pub(crate) timestamp_start_first_iteration: Timestamp,
    pub(crate) timestamp_start_current_iteration: Option<Timestamp>,
    pub(crate) duration_last_iteration: Option<Duration>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum LoopIteration {
    Finite(usize),
    Infinite,
}

impl LoopContext {
    pub(crate) fn new(descriptor: &LoopDescriptor, start: Timestamp) -> Self {
        let iteration = match descriptor.is_infinite {
            true => LoopIteration::Infinite,
            false => LoopIteration::Finite(0),
        };

        Self {
            ingress: descriptor.ingress.clone(),
            egress: descriptor.egress.clone(),
            iteration,
            timestamp_start_first_iteration: start,
            timestamp_start_current_iteration: Some(start),
            duration_last_iteration: None,
        }
    }

    pub(crate) fn update_ingress(&mut self, now: Timestamp) {
        self.timestamp_start_current_iteration = Some(now);
    }

    pub(crate) fn update_egress(&mut self, now: Timestamp) {
        if let LoopIteration::Finite(counter) = self.iteration {
            self.iteration = LoopIteration::Finite(counter + 1);
        }

        self.duration_last_iteration = self
            .timestamp_start_current_iteration
            .map(|timestamp| now.get_diff_duration(&timestamp));

        self.timestamp_start_current_iteration = None;
    }

    pub fn get_ingress(&self) -> &NodeId {
        &self.ingress
    }

    pub fn get_egress(&self) -> &NodeId {
        &self.egress
    }

    pub fn get_iteration(&self) -> LoopIteration {
        self.iteration
    }

    pub fn get_timestamp_start_first_iteration(&self) -> Timestamp {
        self.timestamp_start_first_iteration
    }

    pub fn get_timestamp_start_current_iteration(&self) -> Option<Timestamp> {
        self.timestamp_start_current_iteration
    }

    pub fn get_duration_last_iteration(&self) -> Option<Duration> {
        self.duration_last_iteration
    }
}

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

/// The LoopIteration specifies if the loop is finite or infinite.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum LoopIteration {
    Finite(u64),
    Infinite,
}

impl LoopContext {
    /// Creates a new `LoopContext` from a [`LoopDescriptor`](`LoopDescriptor`)
    /// and a `Timestamp`.
    ///
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

    /// Updates the start timestamp for the current iteration.
    /// This is called when the iteration begins.
    pub(crate) fn update_ingress(&mut self, now: Timestamp) {
        self.timestamp_start_current_iteration = Some(now);
    }

    /// Updates the end timestamp for the current iteration.
    /// This is called when the iteration ends.
    pub(crate) fn update_egress(&mut self, now: Timestamp) {
        if let LoopIteration::Finite(counter) = self.iteration {
            let (iteration, has_overflowed) = counter.overflowing_add(1);
            if has_overflowed {
                log::error!("LoopIteration counter overflowed for Loop: {:?}", self);
            }
            self.iteration = LoopIteration::Finite(iteration);
        }

        self.duration_last_iteration = self
            .timestamp_start_current_iteration
            .map(|timestamp| now.get_diff_duration(&timestamp));

        self.timestamp_start_current_iteration = None;
    }

    /// Returns the ingress node for the loop.
    pub fn get_ingress(&self) -> &NodeId {
        &self.ingress
    }

    /// Returns the egress node for the loop.
    pub fn get_egress(&self) -> &NodeId {
        &self.egress
    }

    /// Returns the [`LoopIteration`](`LoopIteration`).
    pub fn get_iteration(&self) -> LoopIteration {
        self.iteration
    }

    /// Returns the timestamp for the first iteration.
    pub fn get_timestamp_start_first_iteration(&self) -> Timestamp {
        self.timestamp_start_first_iteration
    }

    /// Returns the initial timestamp for the current iteration.
    pub fn get_timestamp_start_current_iteration(&self) -> Option<Timestamp> {
        self.timestamp_start_current_iteration
    }

    /// Returns the duration of the previous iteration.
    pub fn get_duration_last_iteration(&self) -> Option<Duration> {
        self.duration_last_iteration
    }
}

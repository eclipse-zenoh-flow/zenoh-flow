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

use crate::model::deadline::E2EDeadlineRecord;
use crate::model::{InputDescriptor, OutputDescriptor};
use crate::{NodeId, PortId};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use uhlc::Timestamp;

/// A structure containing all the information regarding a missed, local, deadline.
///
/// - `start`: the `Instant` at which the execution started,
/// - `deadline`: the `std::time::Duration` of the deadline,
/// - `elapsed`: the `std::time::Duration` of the execution.
#[derive(Clone)]
pub struct LocalDeadlineMiss {
    pub start: Instant,
    pub deadline: Duration,
    pub elapsed: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct E2EDeadlineMiss {
    pub duration: Duration,
    pub from: OutputDescriptor,
    pub to: InputDescriptor,
    pub start: Timestamp,
    pub end: Timestamp,
}

impl E2EDeadlineMiss {
    pub fn new(deadline: &E2EDeadline, end: &Timestamp) -> Self {
        Self {
            duration: deadline.duration,
            from: deadline.from.clone(),
            to: deadline.to.clone(),
            start: deadline.start,
            end: *end,
        }
    }
}

/// An `E2EDeadline` represents the maximum time that can elapse between the nodes `from` and `to`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct E2EDeadline {
    pub duration: Duration,
    pub from: OutputDescriptor,
    pub to: InputDescriptor,
    pub start: Timestamp,
}

impl PartialEq<E2EDeadlineRecord> for E2EDeadline {
    fn eq(&self, other: &E2EDeadlineRecord) -> bool {
        self.duration == other.duration && self.from == other.from && self.to == other.to
    }
}

impl E2EDeadline {
    /// Creates a new deadline from a `DeadlineRecord` and a starting `Timestamp`.
    pub fn new(record: E2EDeadlineRecord, start: Timestamp) -> Self {
        Self {
            duration: record.duration,
            from: record.from,
            to: record.to,
            start,
        }
    }

    /// Check if the deadline was violated.
    ///
    /// A deadline is violated if the time difference between its `self.start` and the provided
    /// timestamp `now` is strictly greater than its `self.duration`.
    pub fn check(
        &self,
        node_id: &NodeId,
        port_id: &PortId,
        now: &Timestamp,
    ) -> Option<E2EDeadlineMiss> {
        if *node_id == self.to.node && *port_id == self.to.input {
            let diff_duration = now.get_diff_duration(&self.start);
            if diff_duration > self.duration {
                log::warn!(
                    r#"Deadline miss detected:
    From: {} . {}
    To  : {} . {}
      Start            : {} (s)
      Now              : {} (s)
      Time difference  : {} (us)
      Deadline duration: {} (us)
"#,
                    self.from.node,
                    self.from.output,
                    self.to.node,
                    self.to.input,
                    self.start.get_time().to_duration().as_secs_f64(),
                    now.get_time().to_duration().as_secs_f64(),
                    diff_duration.as_micros(),
                    self.duration.as_micros()
                );
                return Some(E2EDeadlineMiss::new(self, now));
            }
        }

        None
    }
}

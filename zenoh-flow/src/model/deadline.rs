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

use crate::model::{InputDescriptor, OutputDescriptor};
use crate::runtime::deadline::E2EDeadline;
use crate::DurationDescriptor;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// An End to End deadline is a deadline on a sub graph.
///
/// The descriptor is its representation within the
/// [`DataFlowDescriptor`](`DataFlowDescriptor`)
///
/// Example:
///
/// ```yaml
/// from:
///    node: AI-Magic-Face-Detection
///    output: Faces
/// to:
///    node: AI-Magic-Face-Recognition
///    input: Faces
/// duration:
///    length: 250
///     unit: ms
/// ```
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct E2EDeadlineDescriptor {
    pub(crate) from: OutputDescriptor,
    pub(crate) to: InputDescriptor,
    pub(crate) duration: DurationDescriptor,
}

/// An `E2EDeadlineRecord` is an instance of an [`E2EDeadlineDescriptor`](`E2EDeadlineDescriptor`)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct E2EDeadlineRecord {
    pub(crate) from: OutputDescriptor,
    pub(crate) to: InputDescriptor,
    pub(crate) duration: Duration,
}

impl From<E2EDeadlineDescriptor> for E2EDeadlineRecord {
    fn from(desc: E2EDeadlineDescriptor) -> Self {
        Self {
            from: desc.from,
            to: desc.to,
            duration: desc.duration.to_duration(),
        }
    }
}

impl PartialEq<E2EDeadline> for E2EDeadlineRecord {
    fn eq(&self, other: &E2EDeadline) -> bool {
        self.from == other.from && self.to == other.to && self.duration == other.duration
    }
}

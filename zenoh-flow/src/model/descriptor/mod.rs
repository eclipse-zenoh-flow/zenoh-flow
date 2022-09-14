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

pub mod dataflow;
pub use dataflow::{DataFlowDescriptor, FlattenDataFlowDescriptor};
pub mod link;
pub use link::{
    CompositeInputDescriptor, CompositeOutputDescriptor, InputDescriptor, LinkDescriptor,
    OutputDescriptor, PortDescriptor,
};
pub mod node;
pub use node::{
    CompositeOperatorDescriptor, NodeDescriptor, SimpleOperatorDescriptor, SinkDescriptor,
    SourceDescriptor,
};
pub mod validator;

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// The unit of duration used in different descriptors.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DurationUnit {
    #[serde(alias = "s")]
    #[serde(alias = "second")]
    #[serde(alias = "seconds")]
    Second,
    #[serde(alias = "ms")]
    #[serde(alias = "millisecond")]
    #[serde(alias = "milliseconds")]
    Millisecond,
    #[serde(alias = "us")]
    #[serde(alias = "µs")]
    #[serde(alias = "microsecond")]
    #[serde(alias = "microseconds")]
    Microsecond,
}

/// The descriptor for a duration.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DurationDescriptor {
    #[serde(alias = "duration")]
    pub(crate) length: u64,
    pub(crate) unit: DurationUnit,
}

impl DurationDescriptor {
    /// Converts the [`DurationDescriptor`](`DurationDescriptor`) to a [`Duration`](`Duration`).
    pub fn to_duration(&self) -> Duration {
        match self.unit {
            DurationUnit::Second => Duration::from_secs(self.length),
            DurationUnit::Millisecond => Duration::from_millis(self.length),
            DurationUnit::Microsecond => Duration::from_micros(self.length),
        }
    }
}

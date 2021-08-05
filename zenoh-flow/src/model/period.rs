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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ZFPeriodDescriptor {
    pub duration: u64,
    pub unit: String,
}

impl ZFPeriodDescriptor {
    pub const SUPPORTED_UNITS: [&'static str; 3] = ["us", "ms", "s"];

    /// Converts the period to a `std::core::Duration`.
    ///
    /// ## Panics
    ///
    /// This method panics if `self.unit` is not supported, i.e. it is not "us", "ms" or "s".
    pub fn to_duration(&self) -> Duration {
        match self.unit.to_lowercase().as_str() {
            "us" => Duration::from_micros(self.duration),
            "ms" => Duration::from_millis(self.duration),
            "s" => Duration::from_secs(self.duration),
            _ => {
                log::error!("Unsupported unit type: {:?}", self.unit);
                panic!("Unsupported unit type: {:?}", self.unit)
            }
        }
    }
}

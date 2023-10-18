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

use std::fmt::Display;

use crate::deserialize::{deserialize_size, deserialize_time};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct SharedMemoryConfiguration {
    pub(crate) number_elements: Option<usize>,
    #[serde(deserialize_with = "deserialize_size")]
    pub(crate) element_size: Option<usize>,
    #[serde(deserialize_with = "deserialize_time")]
    pub(crate) backoff: Option<u64>,
}

// TODO@J-Loudet
impl Display for SharedMemoryConfiguration {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct SharedMemoryParameters {
    pub number_elements: usize,
    // Size, in bytes, of a single element.
    pub element_size: usize,
    // Duration, in nanoseconds, to wait before retrying the last operation.
    pub backoff: u64,
}

// TODO@J-Loudet
impl Display for SharedMemoryParameters {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl SharedMemoryParameters {
    pub fn from_configuration(configuration: &SharedMemoryConfiguration, default: &Self) -> Self {
        Self {
            number_elements: configuration
                .number_elements
                .unwrap_or(default.number_elements),
            element_size: configuration.element_size.unwrap_or(default.element_size),
            backoff: configuration.backoff.unwrap_or(default.backoff),
        }
    }
}

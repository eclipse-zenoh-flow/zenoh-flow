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

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SharedMemoryConfiguration {
    /// Size, converted in bytes, of the shared memory buffer.
    #[serde(deserialize_with = "deserialize_size")]
    pub size: usize,
    /// Duration, converted in nanoseconds, to wait before retrying the last operation.
    #[serde(deserialize_with = "deserialize_time")]
    pub backoff: u64,
}

// TODO@J-Loudet
impl Display for SharedMemoryConfiguration {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

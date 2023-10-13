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

//! This module exposes the functions [deserialize_size] and [deserialize_time] that are used
//! throughout Zenoh-Flow to "parse" values used to express time or size.
//!
//! The external crates [bytesize] and [humantime] are leveraged for these purposes.

use std::str::FromStr;

use serde::Deserializer;

pub fn deserialize_size<'de, D>(deserializer: D) -> std::result::Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    match serde::de::Deserialize::deserialize(deserializer) {
        Ok(buf) => Ok(Some(
            bytesize::ByteSize::from_str(buf)
                .map_err(|_| {
                    serde::de::Error::custom(format!("Unable to parse value as bytes {buf}"))
                })?
                .as_u64() as usize,
        )),
        Err(_) => {
            // log::warn!("failed to deserialize size: {:?}", e);
            Ok(None)
        }
    }
}

pub fn deserialize_time<'de, D>(deserializer: D) -> std::result::Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    match serde::de::Deserialize::deserialize(deserializer) {
        Ok::<&str, _>(buf) => {
            let ht = (buf)
                .parse::<humantime::Duration>()
                .map_err(serde::de::Error::custom)?;
            Ok(Some(ht.as_nanos() as u64))
        }
        Err(_) => {
            // log::warn!("failed to deserialize time: {:?}", e);
            Ok(None)
        }
    }
}

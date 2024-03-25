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

use serde::Deserializer;
use std::{str::FromStr, sync::Arc};
use zenoh_keyexpr::OwnedKeyExpr;

/// Deserialise, from a String, an `Arc<str>` that is guaranteed to be a valid Zenoh-Flow [NodeId](crate::NodeId) or
/// [PortId](crate::PortId).
///
/// # Errors
///
/// The deserialisation will fail if:
/// - the String is empty,
/// - the String contains any of the symbols: * # $ ? >
/// - the String is not a valid Zenoh key expression in its canonical form (see [autocanonize]).
///
/// [autocanonize]: zenoh_keyexpr::OwnedKeyExpr::autocanonize
pub fn deserialize_id<'de, D>(deserializer: D) -> std::result::Result<Arc<str>, D::Error>
where
    D: Deserializer<'de>,
{
    let id: String = serde::de::Deserialize::deserialize(deserializer)?;
    if id.contains(['*', '#', '$', '?', '>']) {
        return Err(serde::de::Error::custom(format!(
            r#"
Identifiers (for nodes or ports) in Zenoh-Flow must *not* contain any of the characters: '*', '#', '$', '?', '>'.
The identifier < {} > does not satisfy that condition.

These characters, except for '>', have a special meaning in Zenoh and they could negatively impact Zenoh-Flow's
behaviour.

The character '>' is used as a separator when flattening a composite operator. Allowing it could also negatively impact
Zenoh-Flow's behaviour.
"#,
            id
        )));
    }

    OwnedKeyExpr::autocanonize(id.clone()).map_err(|e| {
        serde::de::Error::custom(format!(
            r#"
Identifiers (for nodes or ports) in Zenoh-Flow *must* be valid key-expressions in their canonical form.
The identifier < {} > does not satisfy that condition.

Caused by:
{:?}
"#,
            id, e
        ))
    })?;

    Ok(id.into())
}

/// Deserialise a bytes size leveraging the [bytesize] crate.
///
/// This allows parsing, for instance, "1Ko" into "1024" bytes. For more example, see the [bytesize] crate.
///
/// # Errors
///
/// See the [bytesize] documentation.
pub fn deserialize_size<'de, D>(deserializer: D) -> std::result::Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    let size_str: String = serde::de::Deserialize::deserialize(deserializer)?;
    let size_u64 = bytesize::ByteSize::from_str(&size_str)
        .map_err(|e| {
            serde::de::Error::custom(format!(
                "Unable to parse value as bytes {size_str}:\n{:?}",
                e
            ))
        })?
        .as_u64();

    usize::try_from(size_u64).map_err(|e| serde::de::Error::custom(format!(
        "Unable to convert < {} > into a `usize`. Maybe check the architecture of the target device?\n{:?}",
        size_u64, e
    )))
}

/// Deserialise a duration in *microseconds* leveraging the [humantime] crate.
///
/// This allows parsing, for instance, "1ms" as 1000 microseconds.
///
/// # Errors
///
/// See the [humantime] documentation.
pub fn deserialize_time<'de, D>(deserializer: D) -> std::result::Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let buf: &str = serde::de::Deserialize::deserialize(deserializer)?;
    let time_u128 = buf
        .parse::<humantime::Duration>()
        .map_err(serde::de::Error::custom)?
        .as_micros();

    u64::try_from(time_u128).map_err(|e| {
        serde::de::Error::custom(format!(
            "Unable to convert < {} > into a `u64`. Maybe lower the value?\n{:?}",
            time_u128, e
        ))
    })
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use crate::NodeId;

    #[derive(Deserialize, Debug)]
    pub struct TestStruct {
        pub id: NodeId,
    }

    #[test]
    fn test_deserialize_id() {
        let json_str = r#"
{
  "id": "my//chunk"
}
"#;
        assert!(serde_json::from_str::<TestStruct>(json_str).is_err());

        let json_str = r#"
{
  "id": "my*chunk"
}
"#;
        assert!(serde_json::from_str::<TestStruct>(json_str).is_err());

        let json_str = r##"
{
  "id": "#chunk"
}
"##;
        assert!(serde_json::from_str::<TestStruct>(json_str).is_err());

        let json_str = r#"
{
  "id": "?chunk"
}
"#;
        assert!(serde_json::from_str::<TestStruct>(json_str).is_err());

        let json_str = r#"
{
  "id": "$chunk"
}
"#;
        assert!(serde_json::from_str::<TestStruct>(json_str).is_err());

        let json_str = r#"
{
  "id": "my>chunk"
}
"#;
        assert!(serde_json::from_str::<TestStruct>(json_str).is_err());

        let json_str = r#"
{
  "id": "my/chunk/is/alright"
}
"#;
        assert!(serde_json::from_str::<TestStruct>(json_str).is_ok());
    }
}

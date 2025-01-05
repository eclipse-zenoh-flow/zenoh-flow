//
// Copyright © 2021 ZettaScale Technology <contact@zettascale.tech>
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

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use serde::{Deserialize, Deserializer, Serialize};
use zenoh_flow_commons::PortId;
use zenoh_keyexpr::OwnedKeyExpr;

/// A `ZenohSourceDescriptor` encapsulates one or more subscriber(s).
///
/// For each key expression provided, an output with the exact same value will be generated.
///
/// # Caveats: canonical key expressions
///
/// Zenoh only works with canonical key expressions. Hence, Zenoh-Flow will automatically "convert" the provided key
/// expressions into their canonical form.
///
/// If two key expressions, for the same sink, match to the same canonical form a warning message will be logged.
///
/// # Examples
///
/// ```yaml
/// zenoh-subscribers:
///   "cmd_vel": "rt/*/cmd_vel"
///   "status": "rt/*/status"
/// ```
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct ZenohSourceDescriptor {
    pub description: Option<Arc<str>>,
    #[serde(deserialize_with = "deserialize_canon", alias = "zenoh-subscribers")]
    pub subscribers: HashMap<PortId, OwnedKeyExpr>,
}

/// A `ZenohSinkDescriptor` encapsulates one or more publisher(s).
///
/// For each key expression provided, an output with the exact same value will be generated.
///
/// # Caveats: canonical key expressions
///
/// Zenoh only works with canonical key expressions. Hence, Zenoh-Flow will automatically "convert" the provided key
/// expressions into their canonical form.
///
/// If two key expressions, for the same sink, match to the same canonical form a warning message will be logged.
///
/// # Examples
///
/// ```yaml
/// description: My zenoh sink
/// zenoh-publishers:
///   cmd_vel: rt/cmd_vel
///   status: rt/status
/// ```
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct ZenohSinkDescriptor {
    pub description: Option<Arc<str>>,
    #[serde(deserialize_with = "deserialize_canon", alias = "zenoh-publishers")]
    pub publishers: HashMap<PortId, OwnedKeyExpr>,
}

// Transforms a HashMap<String, String> into a HashMap<PortId, OwnedKeyExpr>.
fn deserialize_canon<'de, D>(
    deserializer: D,
) -> std::result::Result<HashMap<PortId, OwnedKeyExpr>, D::Error>
where
    D: Deserializer<'de>,
{
    let key_expressions: HashMap<String, String> =
        serde::de::Deserialize::deserialize(deserializer)?;
    let mut h_map = HashMap::with_capacity(key_expressions.len());
    let mut h_set = HashSet::with_capacity(key_expressions.len());

    for (port_id, key_expr) in key_expressions {
        let owned_canon_ke = OwnedKeyExpr::autocanonize(key_expr.clone()).map_err(|e| {
            serde::de::Error::custom(format!(
                "Failed to autocanonize key expression < {} >:\n{:?}",
                key_expr.clone(),
                e
            ))
        })?;

        if !h_set.insert(owned_canon_ke.clone()) {
            let (duplicate, _) = h_map
                .iter()
                .find(|(_, owned_ke)| owned_canon_ke == **owned_ke)
                .unwrap();
            tracing::warn!(
                r#"
The following two key expressions share the same canonical form ( {} ):
- {}
- {}

They will thus **both** receive the same publications.
If this is a desired behaviour, you can safely ignore this message.

For more details, see:
https://github.com/eclipse-zenoh-flow/roadmap/blob/main/rfcs/ALL/Key%20Expressions.md#canon-forms
"#,
                owned_canon_ke,
                key_expr,
                duplicate,
            );
        }

        h_map.insert(port_id.into(), owned_canon_ke);
    }

    Ok(h_map)
}

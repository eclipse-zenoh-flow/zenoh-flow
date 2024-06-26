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

use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::merge::IMergeOverwrite;

/// A `Configuration` is a recursive key-value structure that allows modifying the behaviour of a node without altering
/// its implementation.
///
/// It is effectively a re-export of [serde_json::Value].
///
/// # Declaration, propagation and merging
///
/// Zenoh-Flow allows users to declare a configuration at 3 locations:
/// - at the top-level of a data flow descriptor,
/// - at the top-level of a composite operator descriptor,
/// - in a node (be it within a data flow descriptor, a composite descriptor or in its dedicated file).
///
/// If a configuration is declared at a top-level it is propagated to all the nodes it includes. Hence, a declaration at
/// the top-level of a data flow is propagated to all the nodes it contains.
///
/// When two configuration keys collide, the configuration with the highest order is kept. The priorities are (from
/// highest to lowest):
/// - the configuration in a node within a data flow descriptor,
/// - the configuration at the top-level of a data flow descriptor,
/// - the configuration in a node within a composite operator descriptor,
/// - the configuration at the top-level of a composite operator descriptor,
/// - the configuration in a dedicated file of a node.
///
/// Hence, configuration at the data flow level are propagating to all nodes, possibly overwriting default values. The
/// same rules apply at the composite operator level. If a node should have a slightly different setting compared to all
/// others, then, thanks to these priorities, only that node needs to be tweaked (either in the data flow or in the
/// composite operator).
///
/// # Examples
///
/// - YAML
///
///   ```yaml
///   configuration:
///     name: "John Doe",
///     age: 43,
///     phones:
///       - "+44 1234567"
///       - "+44 2345678"
///   ```
///
/// - JSON
///
///   ```json
///   "configuration": {
///     "name": "John Doe",
///     "age": 43,
///     "phones": [
///         "+44 1234567",
///         "+44 2345678"
///     ]
///   }
///   ```
//
// NOTE: we take the `serde_json` representation because:
// - JSON is the most supported representation when going online,
// - a `serde_json::Value` can be converted to a `serde_yaml::Value` whereas the opposite is not true (YAML introduces
//   "tags" which are not supported by JSON).
#[derive(Default, Deserialize, Debug, Serialize, Clone, PartialEq, Eq)]
pub struct Configuration(serde_json::Value);

impl Deref for Configuration {
    type Target = serde_json::Value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Configuration {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl IMergeOverwrite for Configuration {
    fn merge_overwrite(self, other: Self) -> Self {
        if self == Configuration::default() {
            return other;
        }

        if other == Configuration::default() {
            return self;
        }

        match (self.as_object(), other.as_object()) {
            (Some(this), Some(other)) => {
                let mut other = other.clone();
                let mut this = this.clone();

                other.append(&mut this);
                Configuration(other.into())
            }
            (_, _) => unreachable!(
                "We are checking, when deserialising, that a Configuration is a JSON object."
            ),
        }
    }
}

impl From<serde_json::Value> for Configuration {
    fn from(value: serde_json::Value) -> Self {
        Self(value)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_merge_configurations() {
        let global = Configuration(json!({ "a": { "nested": true }, "b": ["an", "array"] }));
        let local = Configuration(json!({ "a": { "not-nested": false }, "c": 1 }));

        assert_eq!(
            global.clone().merge_overwrite(local),
            Configuration(json!({ "a": { "nested": true }, "b": ["an", "array"], "c": 1 }))
        );

        assert_eq!(
            global,
            global.clone().merge_overwrite(Configuration::default())
        );
        assert_eq!(
            global,
            Configuration::default().merge_overwrite(global.clone())
        );
        assert_eq!(
            Configuration::default(),
            Configuration::default().merge_overwrite(Configuration::default())
        )
    }
}

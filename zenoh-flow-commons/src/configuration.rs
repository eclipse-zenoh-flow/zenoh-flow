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

use crate::merge::IMergeOverwrite;
use serde::{Deserialize, Serialize};
use std::{ops::Deref, sync::Arc};

/// A `Configuration` is a recursive key-value structure that allows modifying the behavior of a
/// node without altering its implementation.
///
/// It is effectively a re-export of [serde_json::Value].
///
/// # Examples
///
/// ## YAML
///
/// ```yaml
/// configuration:
///   name: "John Doe",
///   age: 43,
///   phones:
///     - "+44 1234567"
///     - "+44 2345678"
/// ```
///
/// ## JSON
///
/// ```json
/// "configuration": {
///   "name": "John Doe",
///   "age": 43,
///   "phones": [
///       "+44 1234567",
///       "+44 2345678"
///   ]
/// }
/// ```
//
// NOTE: we take the `serde_json` representation because:
// - JSON is the most supported representation when going online,
// - a `serde_json::Value` can be converted to a `serde_yaml::Value` whereas the opposite is not
//   true (YAML introduces "tags" which are not supported by JSON).
#[derive(Default, Deserialize, Debug, Serialize, Clone, PartialEq, Eq)]
pub struct Configuration(Arc<serde_json::Value>);

impl Deref for Configuration {
    type Target = serde_json::Value;

    fn deref(&self) -> &Self::Target {
        &self.0
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
                Configuration(Arc::new(other.into()))
            }
            (_, _) => unreachable!(
                "We are checking, when deserializing, that a Configuration is a JSON object."
            ),
        }
    }
}

impl From<serde_json::Value> for Configuration {
    fn from(value: serde_json::Value) -> Self {
        Self(Arc::new(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_merge_configurations() {
        let global = Configuration(Arc::new(
            json!({ "a": { "nested": true }, "b": ["an", "array"] }),
        ));
        let local = Configuration(Arc::new(json!({ "a": { "not-nested": false }, "c": 1 })));

        assert_eq!(
            global.clone().merge_overwrite(local),
            Configuration(Arc::new(
                json!({ "a": { "nested": true }, "b": ["an", "array"], "c": 1 })
            ))
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

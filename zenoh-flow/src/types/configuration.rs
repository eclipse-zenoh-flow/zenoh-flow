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

/// The generic configuration of a graph node.
/// It is a re-export of `serde_json::Value`
pub type Configuration = serde_json::Value;

pub(crate) trait Merge {
    /// Returns the result of the merge of `self` with `other`, overwriting common entries with the
    /// values found in `self`.
    fn merge_overwrite(self, other: Self) -> Self;
}

impl Merge for Option<Configuration> {
    fn merge_overwrite(self, other: Self) -> Self {
        match (self, other) {
            (None, None) => None,
            (None, Some(other)) => Some(other),
            (Some(s), None) => Some(s),
            (Some(mut s), Some(mut other)) => {
                if let (Some(s_obj), Some(o_obj)) = (s.as_object_mut(), other.as_object_mut()) {
                    o_obj.append(s_obj);
                    std::mem::swap(o_obj, s_obj);
                } else {
                    log::warn!(
                        "Could not merge configurations: (1) < {} > with (2) < {} >. Keeping only (1).",
                        s,
                        other
                    );
                }

                Some(s)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_merge_configurations() {
        let global = Some(json!({ "a": { "nested": true }, "b": ["an", "array"] }));
        let local = Some(json!({ "a": { "not-nested": false }, "c": 1 }));

        assert_eq!(
            global.clone().merge_overwrite(local.clone()),
            Some(json!({ "a": { "nested": true }, "b": ["an", "array"], "c": 1 }))
        );

        assert_eq!(None.merge_overwrite(local.clone()), local);
        assert_eq!(global.clone().merge_overwrite(None), global);
    }
}

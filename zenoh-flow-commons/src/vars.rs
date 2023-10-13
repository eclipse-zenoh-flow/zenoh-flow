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
use crate::Result;
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::rc::Rc;

/// `Vars` is an internal structure that we use to expand the "mustache variables" in a descriptor
/// file.
///
/// Mustache variables take the form: "{{ var }}..." where the number of spaces after the '{{' and
/// before the '}}' do not matter.
///
/// We first parse the descriptor file to only extract the `vars` section and build a `HashMap` out of it.
///
/// We then load the descriptor file as a template and "render" it, substituting every "mustache
/// variable" with its corresponding value in the HashMap.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct Vars {
    #[serde(default)]
    vars: Rc<HashMap<Rc<str>, Rc<str>>>,
}

impl IMergeOverwrite for Vars {
    fn merge_overwrite(self, other: Self) -> Self {
        let mut merged = (*other.vars).clone();
        merged.extend((*self.vars).clone().into_iter());

        Self {
            vars: Rc::new(merged),
        }
    }
}

impl<const N: usize> From<[(&str, &str); N]> for Vars {
    fn from(value: [(&str, &str); N]) -> Self {
        Self {
            vars: Rc::new(
                value
                    .into_iter()
                    .map(|(k, v)| (k.into(), v.into()))
                    .collect::<HashMap<Rc<str>, Rc<str>>>(),
            ),
        }
    }
}

impl Vars {
    /// TODO@J-Loudet Documentation.
    pub fn expand_mustache(&self, data: &str) -> Result<String> {
        Ok(ramhorns::Template::new(data)
            .context(format!(
                "Failed to create a ramhorns::Template from:\n{}",
                data
            ))?
            .render(&self.vars))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_overwrite() {
        let map = Vars::from([("BASE_DIR", "/home/zenoh-flow/dev"), ("BUILD", "release")]);

        let placeholder = Vars::from([
            ("BASE_DIR", "/path/to/zenoh-flow"),
            ("BUILD", "debug"),
            ("RUST_LOG", "zenoh_flow_trace"),
        ]);

        let expected = Vars::from([
            ("BASE_DIR", "/home/zenoh-flow/dev"),
            ("BUILD", "release"),
            ("RUST_LOG", "zenoh_flow_trace"),
        ]);

        assert_eq!(expected, map.clone().merge_overwrite(placeholder));

        assert_eq!(map, map.clone().merge_overwrite(Vars::default()));

        assert_eq!(map, Vars::default().merge_overwrite(map.clone()));
    }
}

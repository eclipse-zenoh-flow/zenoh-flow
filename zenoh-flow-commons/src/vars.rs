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

use crate::IMergeOverwrite;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Deref;
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

impl Deref for Vars {
    type Target = HashMap<Rc<str>, Rc<str>>;

    fn deref(&self) -> &Self::Target {
        &self.vars
    }
}

impl IMergeOverwrite for Vars {
    fn merge_overwrite(self, other: Self) -> Self {
        let mut merged = (*other.vars).clone();
        merged.extend((*self.vars).clone());

        Self {
            vars: Rc::new(merged),
        }
    }
}

impl<T: AsRef<str>, U: AsRef<str>, const N: usize> From<[(T, U); N]> for Vars {
    fn from(value: [(T, U); N]) -> Self {
        Self {
            vars: Rc::new(
                value
                    .into_iter()
                    .map(|(k, v)| (k.as_ref().into(), v.as_ref().into()))
                    .collect::<HashMap<Rc<str>, Rc<str>>>(),
            ),
        }
    }
}

impl<T: AsRef<str>, U: AsRef<str>> From<Vec<(T, U)>> for Vars {
    fn from(value: Vec<(T, U)>) -> Self {
        Self {
            vars: Rc::new(
                value
                    .into_iter()
                    .map(|(k, v)| (k.as_ref().into(), v.as_ref().into()))
                    .collect::<HashMap<Rc<str>, Rc<str>>>(),
            ),
        }
    }
}

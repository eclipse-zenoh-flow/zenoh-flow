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

use anyhow::{bail, Context};
use serde::Deserialize;
use url::Url;
use zenoh_flow_commons::{try_parse_from_file, Result, Vars};

pub(crate) fn try_load_descriptor<N>(url: &Url, vars: Vars) -> Result<(N, Vars)>
where
    N: for<'a> Deserialize<'a>,
{
    match url.scheme() {
        "file" => try_parse_from_file::<N>(url.path(), vars).context(format!(
            "Failed to load descriptor from file:\n{}",
            url.path()
        )),
        _ => bail!("Unsupported URL scheme < {} >", url.scheme(),),
    }
}

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

use anyhow::{bail, Context};
use serde::Deserialize;
use url::Url;
use zenoh_flow_commons::{try_parse_from_file, Result, Vars};

pub(crate) fn try_load_descriptor<N>(uri: &str, vars: Vars) -> Result<(N, Vars)>
where
    N: for<'a> Deserialize<'a>,
{
    let url = Url::parse(uri).context(format!("Failed to parse uri:\n{}", uri))?;

    match url.scheme() {
        "file" => try_parse_from_file::<N>(url.path(), vars).context(format!(
            "Failed to load descriptor from file:\n{}",
            url.path()
        )),
        _ => bail!(
            "Failed to parse uri, unsupported scheme < {} > found:\n{}",
            url.scheme(),
            uri
        ),
    }
}

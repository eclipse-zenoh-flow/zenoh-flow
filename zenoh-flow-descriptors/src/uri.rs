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

use crate::vars::Vars;

use std::io::Read;
use std::path::PathBuf;

use anyhow::{bail, Context};
use serde::Deserialize;
use url::Url;
use zenoh_flow_commons::{IMergeOverwrite, Result};

pub(crate) fn try_load_descriptor<N>(uri: &str, vars: Vars) -> Result<(N, Vars)>
where
    N: for<'a> Deserialize<'a>,
{
    let url = Url::parse(uri).context(format!("Failed to parse uri:\n{}", uri))?;

    match url.scheme() {
        "file" => try_load_descriptor_from_file::<N>(url.path(), vars).context(format!(
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

pub(crate) fn deserializer<N>(path: &PathBuf) -> Result<fn(&str) -> Result<N>>
where
    N: for<'a> Deserialize<'a>,
{
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("json") => Ok(|buf| {
            serde_json::from_str::<N>(buf)
                .context(format!("Failed to deserialize from JSON:\n{}", buf))
        }),
        Some("yml") | Some("yaml") => Ok(|buf| {
            serde_yaml::from_str::<N>(buf)
                .context(format!("Failed to deserialize from YAML:\n{}", buf))
        }),
        Some(extension) => bail!(
            r###"
Unsupported file extension < {} > in:
   {:?}

Currently supported file extensions are:
- .json
- .yml
- .yaml
"###,
            extension,
            path
        ),
        None => bail!("Missing file extension in path:\n{}", path.display()),
    }
}

pub(crate) fn try_load_descriptor_from_file<N>(path: &str, vars: Vars) -> Result<(N, Vars)>
where
    N: for<'a> Deserialize<'a>,
{
    let mut path_buf = PathBuf::new();

    #[cfg(test)]
    {
        // When running the test on the CI we cannot know the path of the clone of Zenoh-Flow. By
        // using relative paths (w.r.t. the manifest dir) in the tests and, only in tests, prepend
        // the paths with this environment variable we obtain a correct absolute path.
        path_buf.push(env!("CARGO_MANIFEST_DIR"));
        path_buf.push(
            path.strip_prefix('/')
                .expect("Failed to remove leading '/'"),
        );
    }

    #[cfg(not(test))]
    path_buf.push(path);

    let path = std::fs::canonicalize(&path_buf).context(format!(
        "Failed to canonicalize path (did you put an absolute path?):\n{}",
        path_buf.display()
    ))?;

    let mut buf = String::default();
    std::fs::File::open(path.clone())
        .context(format!("Failed to open file:\n{}", path_buf.display()))?
        .read_to_string(&mut buf)
        .context(format!(
            "Failed to read the content of file:\n{}",
            path_buf.display()
        ))?;

    let merged_vars = vars
        .merge_overwrite(deserializer::<Vars>(&path)?(&buf).context("Failed to deserialize Vars")?);
    let expanded_buf = merged_vars.expand_mustache(&buf)?;

    Ok((
        (deserializer::<N>(&path))?(&expanded_buf)
            .context(format!("Failed to deserialize {}", &path.display()))?,
        merged_vars,
    ))
}

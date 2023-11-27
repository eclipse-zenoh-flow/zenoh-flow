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

use crate::{IMergeOverwrite, Result, Vars};
use anyhow::{bail, Context};
use serde::Deserialize;
use std::path::PathBuf;
use std::{ffi::OsStr, io::Read};

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

pub fn try_load_from_file<N>(path: impl AsRef<OsStr>, vars: Vars) -> Result<(N, Vars)>
where
    N: for<'a> Deserialize<'a>,
{
    let path_buf = std::fs::canonicalize(path.as_ref()).context(format!(
        "Failed to canonicalize path (did you put an absolute path?):\n{}",
        path.as_ref().to_string_lossy()
    ))?;

    let mut buf = String::default();
    std::fs::File::open(&path_buf)
        .context(format!("Failed to open file:\n{}", path_buf.display()))?
        .read_to_string(&mut buf)
        .context(format!(
            "Failed to read the content of file:\n{}",
            path_buf.display()
        ))?;

    let merged_vars = vars.merge_overwrite(
        deserializer::<Vars>(&path_buf)?(&buf).context("Failed to deserialize Vars")?,
    );

    let expanded_buf = ramhorns::Template::new(buf.as_str())
        .context(format!(
            "Failed to create a ramhorns::Template from\n:{}",
            &buf
        ))?
        .render(&*merged_vars);

    Ok((
        (deserializer::<N>(&path_buf))?(&expanded_buf)
            .context(format!("Failed to deserialize {}", &path_buf.display()))?,
        merged_vars,
    ))
}

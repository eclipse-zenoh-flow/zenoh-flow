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
use handlebars::Handlebars;
use serde::Deserialize;
use std::io::Read;
use std::path::{Path, PathBuf};

/// Given the [Path] of a file, return the function we should call to deserialize an instance of `N`.
///
/// This function will look at the extension of the [Path] to decide on a deserializer.
///
/// # Errors
///
/// This function will fail if the extension of the [Path] is not supported. For now, the only supported extensions are:
/// - ".yml"
/// - ".yaml"
/// - ".json"
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
            r#"
Unsupported file extension < {} > in:
   {:?}

Currently supported file extensions are:
- .json
- .yml
- .yaml
"#,
            extension,
            path
        ),
        None => bail!("Missing file extension in path:\n{}", path.display()),
    }
}

/// Attempts to parse an instance of `N` from the content of the file located at `path`, overwriting (or complementing)
/// the [Vars] declared in said file with the provided `vars`.
///
/// This function is notably used to parse a data flow descriptor. Two file types are supported, identified by their
/// extension:
/// - JSON (`.json` file extension)
/// - YAML (`.yaml` or `.yml` extensions)
///
/// This function does not impose writing *all* descriptor file(s), within the same data flow, in the same format.
///
/// # Errors
///
/// The parsing can fail for several reasons (listed in sequential order):
/// - the OS failed to [canonicalize](std::fs::canonicalize()) the path of the file,
/// - the OS failed to open (in read mode) the file,
/// - the extension of the file is not supported by Zenoh-Flow (i.e. it's neither a YAML file or a JSON file),
/// - parsing the [Vars] section failed (if there is one),
/// - expanding the variables located in the [Vars] section failed (if there are any) --- see the documentation
///   [handlebars] for a more complete list of reasons,
/// - parsing an instance of `N` failed.
pub fn try_parse_from_file<N>(path: impl AsRef<Path>, vars: Vars) -> Result<(N, Vars)>
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

    let mut handlebars = Handlebars::new();
    handlebars.set_strict_mode(true);

    let rendered_descriptor = handlebars
        // NOTE: We have to dereference `merged_vars` (this: `&(*merged_vars)`) and pass the contained `HashMap` such
        // that `handlebars` can correctly manipulate it.
        //
        // We have to have this indirection in the structure such that `serde` can correctly deserialise the descriptor.
        .render_template(buf.as_str(), &(*merged_vars))
        .context("Failed to expand descriptor")?;

    Ok((
        (deserializer::<N>(&path_buf))?(&rendered_descriptor)
            .context(format!("Failed to deserialize {}", &path_buf.display()))?,
        merged_vars,
    ))
}

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

use std::{path::PathBuf, str::FromStr};

use anyhow::{bail, Context};
use libloading::Library;
use url::Url;
use zenoh_flow_commons::{Configuration, Result};

use super::NodeSymbol;

fn try_load_node_from_file<N>(path: &str) -> Result<(Library, N)> {
    let path_buf = PathBuf::from_str(path)
        .context(format!("Failed to convert path to a `PathBuf`:\n{}", path))?;

    let path = std::fs::canonicalize(&path_buf).context(format!(
        "Failed to canonicalize path (did you put an absolute path?):\n{}",
        path_buf.display()
    ))?;

    let library_path = match path.extension().and_then(|ext| ext.to_str()) {
        Some(extension) => {
            if extension == std::env::consts::DLL_EXTENSION {
                path
            } else {
                todo!()
            }
        }
        None => bail!(
            "Cannot load library, missing file extension:\n{}",
            path.display()
        ),
    };

    todo!()
}

fn try_load_built_in_node<N>(resource: &str) -> Result<(Library, N)> {
    todo!()
}

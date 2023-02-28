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

use crate::prelude::ErrorKind;
use crate::runtime::dataflow::loader::{Middleware, NodeSymbol, URIStruct};
use crate::{bail, zferror, Result};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use url::Url;

/// Given a string representing a [`Url`](`url::Url`), transform it into a
/// [`URIStruct`](`zenoh_flow::runtime:dataflow::loader::URIStruct`).
///
/// Supported schemes:
/// - `file://`
/// - `builtin://`
///
/// # Errors
///
/// This function will return an error in the following situations:
/// - The provided string does not match the syntax of a [`Url`](`url::Url`).
/// - The scheme is not supported
/// - In case of `builtin://`, the URI struct does not match `<middleware>/[source|sink]`
/// - In case of `file://`, the resulting path cannot be [`canonicalized`](`std::fs::canonicalize`).
pub(crate) fn parse_uri(url_str: &str) -> Result<URIStruct> {
    let uri = Url::parse(url_str).map_err(|err| zferror!(ErrorKind::ParsingError, err))?;

    let uri_path = match uri.host_str() {
        Some(h) => format!("{}{}", h, uri.path()),
        None => uri.path().to_string(),
    };

    match uri.scheme() {
        "file" => Ok(URIStruct::File(try_make_file_path(&uri_path)?)),
        "builtin" => {
            let chunks: Vec<&str> = uri_path.split('/').collect();
            if chunks.len() != 2 {
                // Expected two chunks because of structure
                bail!(ErrorKind::ParsingError, "{uri_path} does not match the `builtin://` struture: <middleware>/[source|sink]");
            }
            let mw = Middleware::from_str(chunks[0])?;
            let kind = NodeSymbol::from_str(chunks[1])?;
            Ok(URIStruct::Builtin(mw, kind))
        }
        _ => {
            bail!(
                ErrorKind::ParsingError,
                "Shceme: {}:// is not supported. Supported ones are `file://` and `builtin://`",
                uri.scheme()
            );
        }
    }
}

/// Given a string representing a path, transform it into a
/// [`PathBuf`](`std::path::PathBuf`).
///
///
/// # Errors
///
/// This function will return an error in the following situations:
/// - The resulting path cannot be [`canonicalized`](`std::fs::canonicalize`).
pub(crate) fn try_make_file_path(file_path: &str) -> Result<PathBuf> {
    let mut path = PathBuf::new();
    #[cfg(test)]
    {
        // When running the test on the CI we cannot know the path of the clone of Zenoh-Flow. By
        // using relative paths (w.r.t. the manifest dir) in the tests and, only in tests, prepend
        // the paths with this environment variable we obtain a correct absolute path.
        path.push(env!("CARGO_MANIFEST_DIR"));
    }

    path.push(file_path);
    let path = std::fs::canonicalize(&path)
        .map_err(|e| zferror!(ErrorKind::IOError, "{}: {}", e, &path.to_string_lossy()))?;
    Ok(path)
}

/// Returns the file extension, if any.
pub(crate) fn get_file_extension(file: &Path) -> Option<String> {
    if let Some(ext) = file.extension() {
        if let Some(ext) = ext.to_str() {
            return Some(String::from(ext));
        }
    }
    None
}

/// Checks if the provided extension is that of a [`dynamic
/// library`](`std::env::consts::DLL_EXTENSION`).
pub(crate) fn is_dynamic_library(ext: &str) -> bool {
    if ext == std::env::consts::DLL_EXTENSION {
        return true;
    }
    false
}

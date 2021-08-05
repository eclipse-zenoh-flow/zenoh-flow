//
// Copyright (c) 2017, 2021 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//

use crate::{
    runtime::runner::{
        ZFOperatorDeclaration, ZFOperatorRunner, ZFSinkDeclaration, ZFSinkRunner,
        ZFSourceDeclaration, ZFSourceRunner,
    },
    types::{ZFError, ZFResult},
    utils::hlc::PeriodicHLC,
};
use async_std::sync::Arc;
use libloading::Library;
use std::collections::HashMap;
use uhlc::HLC;
use url::Url;

pub static CORE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub static RUSTC_VERSION: &str = env!("RUSTC_VERSION");

// OPERATOR

/// # Safety
///
/// TODO remove all copy-pasted code, make macros/functions instead
pub fn load_operator(
    hlc: Arc<HLC>,
    path: String,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<ZFOperatorRunner> {
    let uri = Url::parse(&path).map_err(|err| ZFError::ParsingError(format!("{}", err)))?;

    match uri.scheme() {
        "file" => unsafe { load_lib_operator(hlc, make_file_path(uri), configuration) },
        _ => Err(ZFError::Unimplemented),
    }
}

/// Load the library of the operator.
///
/// # Safety
///
/// This function dynamically loads an external library, things can go wrong:
/// - it will panick if the symbol `zfoperator_declaration` is not found,
/// - be sure to *trust* the code you are loading.
pub unsafe fn load_lib_operator(
    hlc: Arc<HLC>,
    path: String,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<ZFOperatorRunner> {
    log::debug!("Operator Loading {}", path);

    let library = Library::new(path)?;
    let decl = library
        .get::<*mut ZFOperatorDeclaration>(b"zfoperator_declaration\0")?
        .read();

    // version checks to prevent accidental ABI incompatibilities
    if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
        return Err(ZFError::VersionMismatch);
    }

    let operator = (decl.register)(configuration)?;

    let runner = ZFOperatorRunner::new(hlc, operator, Some(library));
    Ok(runner)
}

// SOURCE

pub fn load_source(
    hlc: PeriodicHLC,
    path: String,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<ZFSourceRunner> {
    let uri = Url::parse(&path).map_err(|err| ZFError::ParsingError(format!("{}", err)))?;

    match uri.scheme() {
        "file" => unsafe { load_lib_source(hlc, make_file_path(uri), configuration) },
        _ => Err(ZFError::Unimplemented),
    }
}

/// Load the library of a source.
///
/// # Safety
///
/// This function dynamically loads an external library, things can go wrong:
/// - it will panick if the symbol `zfsource_declaration` is not found,
/// - be sure to *trust* the code you are loading.
pub unsafe fn load_lib_source(
    hlc: PeriodicHLC,
    path: String,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<ZFSourceRunner> {
    log::debug!("Source Loading {}", path);
    let library = Library::new(path)?;
    let decl = library
        .get::<*mut ZFSourceDeclaration>(b"zfsource_declaration\0")?
        .read();

    // version checks to prevent accidental ABI incompatibilities
    if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
        return Err(ZFError::VersionMismatch);
    }

    let source = (decl.register)(configuration)?;

    let runner = ZFSourceRunner::new(hlc, source, Some(library));
    Ok(runner)
}

// SINK

pub fn load_sink(
    path: String,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<ZFSinkRunner> {
    let uri = Url::parse(&path).map_err(|err| ZFError::ParsingError(format!("{}", err)))?;

    match uri.scheme() {
        "file" => unsafe { load_lib_sink(make_file_path(uri), configuration) },
        _ => Err(ZFError::Unimplemented),
    }
}

/// Load the library of a sink.
///
/// # Safety
///
/// This function dynamically loads an external library, things can go wrong:
/// - it will panick if the symbol `zfsink_declaration` is not found,
/// - be sure to *trust* the code you are loading.
pub unsafe fn load_lib_sink(
    path: String,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<ZFSinkRunner> {
    log::debug!("Sink Loading {}", path);
    let library = Library::new(path)?;

    let decl = library
        .get::<*mut ZFSinkDeclaration>(b"zfsink_declaration\0")?
        .read();

    // version checks to prevent accidental ABI incompatibilities
    if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
        return Err(ZFError::VersionMismatch);
    }

    let sink = (decl.register)(configuration)?;

    let runner = ZFSinkRunner::new(sink, Some(library));
    Ok(runner)
}

pub fn make_file_path(uri: Url) -> String {
    match uri.host_str() {
        Some(h) => format!("{}{}", h, uri.path()),
        None => uri.path().to_string(),
    }
}

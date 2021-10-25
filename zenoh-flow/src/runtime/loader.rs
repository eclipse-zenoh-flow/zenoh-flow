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

use crate::runtime::runners::operator::OperatorDeclaration;
use crate::runtime::runners::sink::SinkDeclaration;
use crate::runtime::runners::source::SourceDeclaration;
use crate::{Operator, Sink, Source, ZFError, ZFResult};
use async_std::sync::Arc;
use libloading::Library;
use url::Url;

pub static CORE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub static RUSTC_VERSION: &str = env!("RUSTC_VERSION");

// OPERATOR

/// # Safety
///
/// TODO remove all copy-pasted code, make macros/functions instead
pub fn load_operator(path: &str) -> ZFResult<(Library, Arc<dyn Operator>)> {
    let uri = Url::parse(path).map_err(|err| ZFError::ParsingError(format!("{}", err)))?;

    match uri.scheme() {
        "file" => unsafe { load_lib_operator(&make_file_path(uri)) },
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
unsafe fn load_lib_operator(path: &str) -> ZFResult<(Library, Arc<dyn Operator>)> {
    log::debug!("Operator Loading {}", path);

    let library = Library::new(path)?;
    let decl = library
        .get::<*mut OperatorDeclaration>(b"zfoperator_declaration\0")?
        .read();

    // version checks to prevent accidental ABI incompatibilities
    if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
        return Err(ZFError::VersionMismatch);
    }

    Ok((library, (decl.register)()?))
}

// SOURCE

pub fn load_source(path: &str) -> ZFResult<(Library, Arc<dyn Source>)> {
    let uri = Url::parse(path).map_err(|err| ZFError::ParsingError(format!("{}", err)))?;

    match uri.scheme() {
        "file" => unsafe { load_lib_source(&make_file_path(uri)) },
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
unsafe fn load_lib_source(path: &str) -> ZFResult<(Library, Arc<dyn Source>)> {
    log::debug!("Source Loading {}", path);
    let library = Library::new(path)?;
    let decl = library
        .get::<*mut SourceDeclaration>(b"zfsource_declaration\0")?
        .read();

    // version checks to prevent accidental ABI incompatibilities
    if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
        return Err(ZFError::VersionMismatch);
    }

    Ok((library, (decl.register)()?))
}

// SINK

pub fn load_sink(path: &str) -> ZFResult<(Library, Arc<dyn Sink>)> {
    let uri = Url::parse(path).map_err(|err| ZFError::ParsingError(format!("{}", err)))?;

    match uri.scheme() {
        "file" => unsafe { load_lib_sink(&make_file_path(uri)) },
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
unsafe fn load_lib_sink(path: &str) -> ZFResult<(Library, Arc<dyn Sink>)> {
    log::debug!("Sink Loading {}", path);
    let library = Library::new(path)?;

    let decl = library
        .get::<*mut SinkDeclaration>(b"zfsink_declaration\0")?
        .read();

    // version checks to prevent accidental ABI incompatibilities
    if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
        return Err(ZFError::VersionMismatch);
    }

    Ok((library, (decl.register)()?))
}

pub fn make_file_path(uri: Url) -> String {
    match uri.host_str() {
        Some(h) => format!("{}{}", h, uri.path()),
        None => uri.path().to_string(),
    }
}

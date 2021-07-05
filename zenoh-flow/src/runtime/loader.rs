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

use crate::runtime::runner::{
    ZFOperatorDeclaration, ZFOperatorRunner, ZFSinkDeclaration, ZFSinkRunner, ZFSourceDeclaration,
    ZFSourceRunner,
};
use crate::types::{ZFError, ZFResult};
use libloading::Library;
use std::collections::HashMap;

pub static CORE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub static RUSTC_VERSION: &str = env!("RUSTC_VERSION");

// OPERATOR

/// # Safety
///
/// TODO remove all copy-pasted code, make macros/functions instead
pub unsafe fn load_operator(
    path: String,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<ZFOperatorRunner> {
    // This is unsafe because has to dynamically load a library
    let library = Library::new(path).unwrap();
    let decl = library
        .get::<*mut ZFOperatorDeclaration>(b"zfoperator_declaration\0")
        .unwrap()
        .read();

    // version checks to prevent accidental ABI incompatibilities
    if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
        return Err(ZFError::VersionMismatch);
    }

    let operator = (decl.register)(configuration)?;

    let runner = ZFOperatorRunner::new(operator, Some(library));
    Ok(runner)
}

// SOURCE

pub unsafe fn load_source(
    path: String,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<ZFSourceRunner> {
    let library = Library::new(path).unwrap();
    let decl = library
        .get::<*mut ZFSourceDeclaration>(b"zfsource_declaration\0")
        .unwrap()
        .read();

    // version checks to prevent accidental ABI incompatibilities
    if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
        return Err(ZFError::VersionMismatch);
    }

    let source = (decl.register)(configuration)?;

    let runner = ZFSourceRunner::new(source, Some(library));
    Ok(runner)
}

// SINK

pub unsafe fn load_sink(
    path: String,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<ZFSinkRunner> {
    log::debug!("Loading {}", path);
    let library = Library::new(path).unwrap();

    let decl = library
        .get::<*mut ZFSinkDeclaration>(b"zfsink_declaration\0")
        .unwrap()
        .read();

    // version checks to prevent accidental ABI incompatibilities
    if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
        return Err(ZFError::VersionMismatch);
    }

    let sink = (decl.register)(configuration)?;

    let runner = ZFSinkRunner::new(sink, Some(library));
    Ok(runner)
}

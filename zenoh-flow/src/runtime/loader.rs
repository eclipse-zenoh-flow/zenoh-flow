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

use crate::async_std::sync::Arc;
use crate::runtime::runner::{
    ZFOperatorDeclaration, ZFOperatorRunner, ZFSinkDeclaration, ZFSinkRunner, ZFSourceDeclaration,
    ZFSourceRunner, ZFZenohReceiverDeclaration, ZFZenohSenderDeclaration,
};
use crate::types::{ZFError, ZFResult};
use crate::ZFOperatorId;
use libloading::Library;
use std::collections::HashMap;
use zenoh::net::Session;

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
pub unsafe fn load_zenoh_receiver(
    path: String,
    session: Arc<Session>,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<(ZFOperatorId, ZFSourceRunner)> {
    load_source_or_receiver(path, Some(session), configuration)
}

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

unsafe fn load_source_or_receiver(
    path: String,
    zenoh_session: Option<Arc<Session>>,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<(ZFOperatorId, ZFSourceRunner)> {
    // This is unsafe because has to dynamically load a library
    // let library = Arc::new(Library::new(path).unwrap());
    // let mut registrar = ZFSourceRegistrar::new(Arc::clone(&library));

    // if let Some(session) = zenoh_session {
    //     let decl = library
    //         .get::<*mut ZFZenohReceiverDeclaration>(b"zfsource_declaration\0")
    //         .unwrap()
    //         .read();

    //     // version checks to prevent accidental ABI incompatibilities
    //     if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
    //         return Err(ZFError::VersionMismatch);
    //     }

    //     (decl.register)(&mut registrar, session, configuration)?;
    // } else {
    //     unimplemented!("To be removed...")
    // }

    // let (operator_id, proxy) = registrar.operator.unwrap();

    // let runner = ZFSourceRunner::new(proxy, library);
    // Ok((operator_id, runner))
    Err(ZFError::GenericError)
}

// CONNECTOR SENDER — Similar to a SINK (see below)
pub unsafe fn load_zenoh_sender(
    path: String,
    session: Arc<Session>,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<(ZFOperatorId, ZFSinkRunner)> {
    load_sink_or_sender(path, Some(session), configuration)
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

unsafe fn load_sink_or_sender(
    path: String,
    zenoh_session: Option<Arc<Session>>,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<(ZFOperatorId, ZFSinkRunner)> {
    // This is unsafe because has to dynamically load a library
    // log::debug!("Loading {}", path);
    // let library = Arc::new(Library::new(path).unwrap());
    // let mut registrar = ZFSinkRegistrar::new(Arc::clone(&library));

    // if let Some(session) = zenoh_session {
    //     let decl = library
    //         .get::<*mut ZFZenohSenderDeclaration>(b"zfsink_declaration\0")
    //         .unwrap()
    //         .read();

    //     // version checks to prevent accidental ABI incompatibilities
    //     if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
    //         return Err(ZFError::VersionMismatch);
    //     }

    //     (decl.register)(&mut registrar, session, configuration)?;
    // } else {
    //     let decl = library
    //         .get::<*mut ZFSinkDeclaration>(b"zfsink_declaration\0")
    //         .unwrap()
    //         .read();

    //     // version checks to prevent accidental ABI incompatibilities
    //     if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
    //         return Err(ZFError::VersionMismatch);
    //     }

    //     (decl.register)(&mut registrar, configuration)?;
    // }

    // let (operator_id, proxy) = registrar.operator.unwrap();

    // let runner = ZFSinkRunner::new_dynamic(proxy, library);
    // Ok((operator_id, runner))
    Err(ZFError::GenericError)
}

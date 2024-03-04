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

use crate::prelude::{Context, Inputs, Node, Outputs};

use std::{pin::Pin, sync::Arc};

use futures::Future;
use zenoh_flow_commons::{Configuration, Result};

/// Constant used to check if a node is compatible with the currently running Zenoh Flow daemon.
/// As nodes are dynamically loaded, this is to prevent (possibly cryptic) runtime error due to
/// incompatible API.
pub const CORE_VERSION: &str = env!("CARGO_PKG_VERSION");
/// Constant used to check if a node was compiled with the same version of the Rust compiler than
/// the currently running Zenoh Flow daemon.
/// As Rust is not ABI stable, this is to prevent (possibly cryptic) runtime errors.
pub const RUSTC_VERSION: &str = env!("RUSTC_VERSION");

/// Declaration expected in the library that will be loaded.
pub struct NodeDeclaration<C> {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub constructor: C,
}

/// `SourceFn` is the only signature we accept to construct a [`Source`](`crate::prelude::Source`).
pub type SourceFn = fn(
    Context,
    Configuration,
    Outputs,
) -> Pin<Box<dyn Future<Output = Result<Arc<dyn Node>>> + Send>>;

/// `OperatorFn` is the only signature we accept to construct an [`Operator`](`crate::prelude::Operator`).
pub type OperatorFn = fn(
    Context,
    Configuration,
    Inputs,
    Outputs,
) -> Pin<Box<dyn Future<Output = Result<Arc<dyn Node>>> + Send>>;

/// `SinkFn` is the only signature we accept to construct a [`Sink`](`crate::prelude::Sink`).
pub type SinkFn = fn(
    Context,
    Configuration,
    Inputs,
) -> Pin<Box<dyn Future<Output = Result<Arc<dyn Node>>> + Send>>;

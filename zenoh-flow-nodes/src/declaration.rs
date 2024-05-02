//
// Copyright (c) 2021 - 2024 ZettaScale Technology
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

use std::{pin::Pin, sync::Arc};

use futures::Future;
use zenoh_flow_commons::{Configuration, Result};

use crate::prelude::{Context, Inputs, Node, Outputs};

/// (⚙️️ *internal)* Constant used to check if a node is compatible with the Zenoh-Flow runtime managing it.
///
/// As nodes are dynamically loaded, this is to prevent (possibly cryptic) runtime error due to incompatible API.
///
/// This constant is used by the procedural macros [export_operator](crate::prelude::export_operator),
/// [export_source](crate::prelude::export_source) and [export_sink](crate::prelude::export_sink). A Zenoh-Flow runtime
/// will compare its value of this constant to the value that all node it will dynamically load expose.
pub const CORE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// (⚙️ *internal)* Constant used to check if a node was compiled with the same version of the Rust compiler than the
/// Zenoh-Flow runtime managing it.
///
/// As Rust is not ABI stable, this is to prevent (possibly cryptic) runtime errors.
///
/// This constant is used by the procedural macros [export_operator](crate::prelude::export_operator),
/// [export_source](crate::prelude::export_source) and [export_sink](crate::prelude::export_sink). A Zenoh-Flow runtime
/// will compare its value of this constant to the value that all node it will dynamically load expose.
pub const RUSTC_VERSION: &str = env!("RUSTC_VERSION");

/// (⚙️ *internal)* Declaration expected in the library that will be loaded.
///
///  This structure is automatically created by the procedural macros
/// [export_operator](crate::prelude::export_operator), [export_source](crate::prelude::export_source) and
/// [export_sink](crate::prelude::export_sink).
pub struct NodeDeclaration<C> {
    pub rustc_version: &'static str,
    pub core_version: &'static str,
    pub constructor: C,
}

/// (⚙️ *internal)* `SourceFn` is the only signature we accept to construct a [Source](crate::prelude::Source).
///
///  This function is automatically created by the procedural macro [export_source](crate::prelude::export_source).
pub type SourceFn = fn(
    Context,
    Configuration,
    Outputs,
) -> Pin<Box<dyn Future<Output = Result<Arc<dyn Node>>> + Send>>;

/// (⚙️ *internal)* `OperatorFn` is the only signature we accept to construct an [Operator](crate::prelude::Operator).
///
///  This function is automatically created by the procedural macro [export_operator](crate::prelude::export_operator).
pub type OperatorFn = fn(
    Context,
    Configuration,
    Inputs,
    Outputs,
) -> Pin<Box<dyn Future<Output = Result<Arc<dyn Node>>> + Send>>;

/// (⚙️ *internal)* `SinkFn` is the only signature we accept to construct a [Sink](crate::prelude::Sink).
///
///  This function is automatically created by the procedural macro [export_sink](crate::prelude::export_sink).
pub type SinkFn = fn(
    Context,
    Configuration,
    Inputs,
) -> Pin<Box<dyn Future<Output = Result<Arc<dyn Node>>> + Send>>;

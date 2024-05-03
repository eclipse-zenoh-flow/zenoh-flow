//
// Copyright © 2021 ZettaScale Technology <contact@zettascale.tech>
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

//! This crate exposes the traits and structures necessary to create Zenoh-Flow nodes.
//!
//! Items not exposed in the `prelude` are meant for internal usage within the Zenoh-Flow project.
//!
//! # [prelude]
//!
//! Application developers wishing to create a data flow should include the [prelude] in their code-base as it regroups
//! all the required structures and traits:
//!
//! ```
//! use zenoh_flow_nodes::prelude::*;
//! ```
//!
//! Next would be to implement, as different shared libraries, at least a [Source](crate::prelude::Source), a
//! [Sink](crate::prelude::Sink) and possibly some [Operators](crate::prelude::Operator). See their respective
//! documentation for examples.

pub(crate) mod context;
pub(crate) mod declaration;
pub(crate) mod io;
pub(crate) mod messages;
pub(crate) mod traits;

pub use self::{
    declaration::{NodeDeclaration, OperatorFn, SinkFn, SourceFn, CORE_VERSION, RUSTC_VERSION},
    io::{InputBuilder, OutputBuilder},
};

/// This module expose all the structures required to implement a Zenoh-Flow node.
///
/// It also re-exposes items from the [anyhow], [zenoh_flow_commons] and [zenoh_flow_derive] crates.
pub mod prelude {
    pub use anyhow::{anyhow, bail};
    pub use zenoh_flow_commons::{Configuration, Result};
    pub use zenoh_flow_derive::{export_operator, export_sink, export_source};

    pub use crate::{
        context::Context,
        io::{Input, InputRaw, Inputs, Output, OutputRaw, Outputs},
        messages::{Data, LinkMessage, Payload},
        traits::{Node, Operator, SendSyncAny, Sink, Source},
    };
}

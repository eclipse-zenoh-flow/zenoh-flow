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

pub(crate) mod declaration;
pub use declaration::{NodeDeclaration, OperatorFn, SinkFn, SourceFn, CORE_VERSION, RUSTC_VERSION};

pub(crate) mod context;
pub(crate) mod io;
pub(crate) mod messages;
pub(crate) mod traits;

pub mod prelude {
    pub use crate::context::Context;
    pub use crate::io::{Input, InputRaw, Inputs, Output, OutputRaw, Outputs};
    pub use crate::messages::{Data, LinkMessage};
    pub use crate::traits::{Node, Operator, SendSyncAny, Sink, Source};
    pub use anyhow::{anyhow, bail};
    pub use zenoh_flow_commons::{Configuration, Result};
    pub use zenoh_flow_derive::{export_operator, export_sink, export_source};
}

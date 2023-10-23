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

pub(crate) mod io;
pub(crate) mod messages;
pub(crate) mod traits;

pub mod prelude {
    pub use crate::io::{Input, Inputs, Output, Outputs};
    pub use crate::messages::{Data, DataMessage, LinkMessage, Message};
    pub use crate::traits::{Node, Operator, SendSyncAny, Sink, Source};
    pub use crate::Context;
    pub use zenoh_flow_commons::{Configuration, Result};
    pub use zenoh_flow_derive::{export_operator, export_sink, export_source};
}

use std::sync::Arc;
use uuid::Uuid;
use zenoh_flow_commons::RuntimeId;

/// TODO@J-Loudet
pub struct Context {
    pub flow_name: Arc<str>,
    pub flow_uuid: Uuid,
    pub runtime_id: RuntimeId,
}

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
    pub use crate::io::{Input, InputRaw, Inputs, Output, OutputRaw, Outputs};
    pub use crate::messages::{Data, DataMessage, LinkMessage, Message};
    pub use crate::traits::{Node, Operator, SendSyncAny, Sink, Source};
    pub use crate::Context;
    pub use anyhow::{anyhow, bail};
    pub use zenoh_flow_commons::{Configuration, Result};
    pub use zenoh_flow_derive::{export_operator, export_sink, export_source};
}

use std::sync::Arc;
use zenoh_flow_commons::{InstanceId, RuntimeId};

/// TODO@J-Loudet
#[derive(Clone, Debug)]
pub struct Context {
    pub(crate) flow_name: Arc<str>,
    pub(crate) instance_id: InstanceId,
    pub(crate) runtime_id: RuntimeId,
}

impl Context {
    pub fn new(flow_name: Arc<str>, instance_id: InstanceId, runtime_id: RuntimeId) -> Self {
        Self {
            flow_name,
            instance_id,
            runtime_id,
        }
    }

    pub fn name(&self) -> &str {
        self.flow_name.as_ref()
    }

    pub fn instance_id(&self) -> &InstanceId {
        &self.instance_id
    }

    pub fn runtime(&self) -> &RuntimeId {
        &self.runtime_id
    }
}

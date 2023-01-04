//
// Copyright (c) 2022 ZettaScale Technology
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

use crate::runtime::InstanceContext;
use crate::types::{FlowId, RuntimeId};
use std::ops::Deref;
use std::sync::Arc;
use uhlc::HLC;
use uuid::Uuid;
use zenoh::Session;

/// The `Context` is a structure to obtain information about the runtime, use the Hybrid Logical
/// Clock (HLC) of the runtime to generate timestamps and register callbacks.
///
/// The following runtime information are exposed through accessors:
/// - `runtime_name`: the name given to the runtime (in its configuration file);
/// - `runtime_uuid`: the generated unique identifier of the runtime;
/// - `flow_name`: the name given to the flow (in the descriptor file);
/// - `instance_id`: the generated unique identifier of this instanciation of the flow.
///
/// The HLC is directly accessible thanks to a `Deref` implementation.
#[derive(Clone)]
pub struct Context {
    pub(crate) runtime_name: RuntimeId,
    pub(crate) runtime_uuid: Uuid,
    pub(crate) flow_name: FlowId,
    pub(crate) instance_id: Uuid,
    pub(crate) hlc: Arc<HLC>,
    pub(crate) zenoh_session: Arc<Session>,
}

impl Context {
    pub(crate) fn new(instance_context: &InstanceContext) -> Self {
        Self {
            runtime_name: instance_context.runtime.runtime_name.clone(),
            runtime_uuid: instance_context.runtime.runtime_uuid,
            flow_name: instance_context.flow_id.clone(),
            instance_id: instance_context.instance_id,
            hlc: instance_context.runtime.hlc.clone(),
            zenoh_session: instance_context.runtime.session.clone(),
        }
    }

    /// Returns the (user given) name of the runtime in which the calling node is running.
    ///
    /// Note that, for the same instance of a flow (i.e. the `flow_id` and `instance_id` are equal),
    /// different nodes can be running on different runtimes.
    pub fn get_runtime_name(&self) -> &RuntimeId {
        &self.runtime_name
    }

    /// Returns the unique identifier of the runtime in which the calling node is running.
    ///
    /// Note that, for the same instance of a flow (i.e. the `flow_id` and `instance_id` are equal),
    /// different nodes can be running on different runtimes.
    pub fn get_runtime_uuid(&self) -> &Uuid {
        &self.runtime_uuid
    }

    /// Returns the (user given) name of the data flow.
    pub fn get_flow_name(&self) -> &FlowId {
        &self.flow_name
    }

    /// Returns the unique identifier of the running instance of the data flow.
    pub fn get_instance_id(&self) -> &Uuid {
        &self.instance_id
    }

    /// Returns a thread-safe reference over the Zenoh session used by the Zenoh-Flow daemon running
    /// the node.
    pub fn zenoh_session(&self) -> Arc<Session> {
        self.zenoh_session.clone()
    }
}

impl Deref for Context {
    type Target = HLC;

    fn deref(&self) -> &Self::Target {
        &self.hlc
    }
}

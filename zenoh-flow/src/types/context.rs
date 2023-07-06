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
use zenoh::prelude::ZenohId;
use zenoh::Session;

/// The `Context` is a structure to obtain information about the runtime, use the Hybrid Logical
/// Clock (HLC) of the runtime to generate timestamps and access runtime-wise shared memory configuration.
///
/// The following runtime information are exposed through accessors:
/// - `runtime_name`: the name given to the runtime (in its configuration file);
/// - `runtime_uuid`: the generated unique identifier of the runtime;
/// - `flow_name`: the name given to the flow (in the descriptor file);
/// - `instance_id`: the generated unique identifier of this instanciation of the flow.
/// - `shared_memory_element_size` :  the default size of each shared memory chunk
/// - `shared_memory_elements` : the default total number of shared memory chunks
/// - `shared_memory_backoff` : the default backoff time when no chunks are available
///
/// The HLC is directly accessible thanks to a `Deref` implementation.
#[derive(Clone)]
pub struct Context {
    instance_ctx: InstanceContext,
}

impl Context {
    pub(crate) fn new(instance_ctx: &InstanceContext) -> Self {
        Self {
            instance_ctx: instance_ctx.clone(),
        }
    }

    /// Returns the (user given) name of the runtime in which the calling node is running.
    ///
    /// Note that, for the same instance of a flow (i.e. the `flow_id` and `instance_id` are equal),
    /// different nodes can be running on different runtimes.
    pub fn get_runtime_name(&self) -> &RuntimeId {
        &self.instance_ctx.runtime.runtime_name
    }

    /// Returns the unique identifier of the runtime in which the calling node is running.
    ///
    /// Note that, for the same instance of a flow (i.e. the `flow_id` and `instance_id` are equal),
    /// different nodes can be running on different runtimes.
    pub fn get_runtime_uuid(&self) -> &ZenohId {
        &self.instance_ctx.runtime.runtime_uuid
    }

    /// Returns the (user given) name of the data flow.
    pub fn get_flow_name(&self) -> &FlowId {
        &self.instance_ctx.flow_id
    }

    /// Returns the unique identifier of the running instance of the data flow.
    pub fn get_instance_id(&self) -> &Uuid {
        &self.instance_ctx.instance_id
    }

    /// Returns a thread-safe reference over the Zenoh session used by the Zenoh-Flow daemon running
    /// the node.
    pub fn zenoh_session(&self) -> Arc<Session> {
        self.instance_ctx.runtime.session.clone()
    }

    /// Returns the default size of each shared memory chunk.
    pub fn shared_memory_element_size(&self) -> &usize {
        &self.instance_ctx.runtime.shared_memory_element_size
    }

    /// Returns the default number of shared memory chunks.
    pub fn shared_memory_elements(&self) -> &usize {
        &self.instance_ctx.runtime.shared_memory_elements
    }

    /// Returns the default backoff time when there are no shared memory chunk available.
    pub fn shared_memory_backoff(&self) -> &u64 {
        &self.instance_ctx.runtime.shared_memory_backoff
    }

    /// Returns wheteher the shared memory is enabled or not
    pub fn shared_memory_enabled(&self) -> &bool {
        &self.instance_ctx.runtime.use_shm
    }
}

impl Deref for Context {
    type Target = HLC;

    fn deref(&self) -> &Self::Target {
        &self.instance_ctx.runtime.hlc
    }
}

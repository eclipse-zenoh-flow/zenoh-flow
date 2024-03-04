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

use std::sync::Arc;

use zenoh_flow_commons::{InstanceId, RuntimeId};

/// The `Context` structure provides information about the data flow and the Zenoh-Flow runtime.
///
/// In particular, it allows accessing:
/// - [name](Context::name()) of the data flow,
/// - [instance id](Context::instance_id()) of this instance of the data flow,
/// - [runtime id](Context::runtime_id()) of the Zenoh-Flow runtime managing the **node**.
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

    /// Returns the name of the data flow.
    ///
    /// Note all instances of the same data flow will share the same `name`.
    pub fn name(&self) -> &str {
        self.flow_name.as_ref()
    }

    /// Returns the unique identifier of this instance of the data flow.
    pub fn instance_id(&self) -> &InstanceId {
        &self.instance_id
    }

    /// Returns the unique identifier of the Zenoh-Flow runtime managing the **node**.
    ///
    /// Note that, for the same instance, different nodes might return different runtime identifier if they are running
    /// on different Zenoh-Flow runtimes.
    pub fn runtime_id(&self) -> &RuntimeId {
        &self.runtime_id
    }
}

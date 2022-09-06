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
use crate::traits::{InputCallback, OutputCallback};
use crate::types::{CallbackInput, CallbackOutput, FlowId, Input, Output, RuntimeId};
use std::ops::Deref;
use std::sync::Arc;
use uhlc::HLC;
use uuid::Uuid;

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
///
/// TODO: `register_input_callback` & `register_output_callback` + documentation
pub struct Context {
    pub(crate) runtime_name: RuntimeId,
    pub(crate) runtime_uuid: Uuid,
    pub(crate) flow_name: FlowId,
    pub(crate) instance_id: Uuid,
    pub(crate) hlc: Arc<HLC>,
    pub(crate) inputs_callbacks: Vec<CallbackInput>,
    pub(crate) outputs_callbacks: Vec<CallbackOutput>,
}

impl Context {
    pub(crate) fn new(instance_context: &InstanceContext) -> Self {
        Self {
            runtime_name: Arc::clone(&instance_context.runtime.runtime_name),
            runtime_uuid: instance_context.runtime.runtime_uuid,
            flow_name: Arc::clone(&instance_context.flow_id),
            instance_id: instance_context.instance_id,
            hlc: Arc::clone(&instance_context.runtime.hlc),
            inputs_callbacks: vec![],
            outputs_callbacks: vec![],
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

    /// Register the `callback` to be called whenever the `input` receives data.
    ///
    /// Zenoh-Flow will execute the callback whenever new data is received. If additional
    /// constraints should be enforced, it is up to the developer to add them in the implementation
    /// of the callback (for instance, to add a minimum periodicity between consecutive executions).
    pub fn register_input_callback(&mut self, input: Input, callback: Box<dyn InputCallback>) {
        self.inputs_callbacks
            .push(CallbackInput { input, callback })
    }

    /// Register the `callback` to repeatedly call to send data on the `output`.
    ///
    /// Zenoh-Flow will continuously execute the callback so it is up to the developer to add
    /// constraints to the callback’s implementation (for instance, to make it periodic).
    pub fn register_output_callback(&mut self, output: Output, callback: Box<dyn OutputCallback>) {
        self.outputs_callbacks
            .push(CallbackOutput { output, callback })
    }
}

impl Deref for Context {
    type Target = HLC;

    fn deref(&self) -> &Self::Target {
        &self.hlc
    }
}

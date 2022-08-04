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

use std::collections::HashMap;
use std::time::Instant;

use crate::async_std::sync::Arc;
use crate::runtime::dataflow::instance::runners::{Runner, RunnerKind};
use crate::runtime::dataflow::node::SinkLoaded;
use crate::runtime::InstanceContext;
use crate::types::ZFResult;
use crate::{Configuration, Input, NodeId, PortId, Sink, ZFError};
use async_std::task::JoinHandle;
use async_trait::async_trait;
use futures::future::{AbortHandle, Abortable, Aborted};

#[cfg(target_family = "unix")]
use libloading::os::unix::Library;
#[cfg(target_family = "windows")]
use libloading::Library;

/// The `SinkRunner` is the component in charge of executing the sink.
/// It contains all the runtime information for the sink, the graph instance.
///
/// Do not reorder the fields in this struct.
/// Rust drops fields in a struct in the same order they are declared.
/// Ref: <https://doc.rust-lang.org/reference/destructors.html>
/// We need the state to be dropped before the sink/lib, otherwise we
/// will have a SIGSEV.
pub struct SinkRunner {
    pub(crate) id: NodeId,
    pub(crate) configuration: Option<Configuration>,
    pub(crate) context: InstanceContext,
    pub(crate) inputs: HashMap<PortId, Input>,
    pub(crate) sink: Arc<dyn Sink>,
    pub(crate) _library: Option<Arc<Library>>,
    pub(crate) handle: Option<JoinHandle<Result<ZFError, Aborted>>>,
    pub(crate) abort_handle: Option<AbortHandle>,
}

impl SinkRunner {
    /// Tries to create a new `SinkRunner` using the given
    /// [`InstanceContext`](`InstanceContext`), [`SinkLoaded`](`SinkLoaded`)
    /// and [`OperatorIO`](`OperatorIO`).
    ///
    /// # Errors
    /// If fails if the input is not connected.
    pub fn new(context: InstanceContext, sink: SinkLoaded, inputs: HashMap<PortId, Input>) -> Self {
        Self {
            id: sink.id,
            configuration: sink.configuration,
            context,
            inputs,
            sink: sink.sink,
            _library: sink.library,
            handle: None,
            abort_handle: None,
        }
    }

    // /// Starts the sink.
    // async fn start(&self) {
    //     *self.is_running.lock().await = true;
    // }

    // /// A single iteration of the run loop.
    // ///
    // /// # Errors
    // /// An error variant is returned in case of:
    // /// -  user returns an error
    // /// - link recv fails
    // ///
    // async fn iteration(&self, mut context: Context) -> ZFResult<Context> {
    //     // Guards are taken at the beginning of each iteration to allow interleaving.
    //     if let Some(link) = &*self.link.lock().await {
    //         let mut state = self.state.lock().await;

    //         let (_port_id, message) = link.recv().await?;
    //         let input = match message.as_ref() {
    //             Message::Data(data_message) => {
    //                 if let Err(error) = self
    //                     .context
    //                     .runtime
    //                     .hlc
    //                     .update_with_timestamp(&data_message.timestamp)
    //                 {
    //                     log::error!(
    //                         "[Sink: {}][HLC] Could not update HLC with timestamp {:?}: {:?}",
    //                         self.id,
    //                         data_message.timestamp,
    //                         error
    //                     );
    //                 }
    //                 data_message.clone()
    //             }

    //             Message::Control(_) => return Err(ZFError::Unimplemented),
    //         };

    //         self.sink.run(&mut context, &mut state, input).await?;
    //     }
    //     Ok(context)
    // }
}

#[async_trait]
impl Runner for SinkRunner {
    fn get_id(&self) -> NodeId {
        self.id.clone()
    }
    fn get_kind(&self) -> RunnerKind {
        RunnerKind::Sink
    }

    // async fn add_input(&self, input: LinkReceiver) -> ZFResult<()> {
    //     (*self.link.lock().await) = Some(input);
    //     Ok(())
    // }

    // async fn add_output(&self, _output: LinkSender) -> ZFResult<()> {
    //     Err(ZFError::SinkDoNotHaveOutputs)
    // }

    async fn clean(&self) -> ZFResult<()> {
        self.sink.finalize().await
    }

    // fn get_inputs(&self) -> HashMap<PortId, PortType> {
    //     let mut inputs = HashMap::with_capacity(1);
    //     inputs.insert(self.input.port_id.clone(), self.input.port_type.clone());
    //     inputs
    // }

    // fn get_outputs(&self) -> HashMap<PortId, PortType> {
    //     HashMap::with_capacity(0)
    // }

    // async fn get_outputs_links(&self) -> HashMap<PortId, Vec<LinkSender>> {
    //     HashMap::with_capacity(0)
    // }

    // async fn take_input_links(&self) -> HashMap<PortId, LinkReceiver> {
    //     let mut link_guard = self.link.lock().await;
    //     if let Some(link) = &*link_guard {
    //         let mut inputs = HashMap::with_capacity(1);
    //         inputs.insert(self.input.port_id.clone(), link.clone());
    //         *link_guard = None;
    //         return inputs;
    //     }
    //     HashMap::with_capacity(0)
    // }

    // async fn start_recording(&self) -> ZFResult<String> {
    //     Err(ZFError::Unsupported)
    // }

    // async fn stop_recording(&self) -> ZFResult<String> {
    //     Err(ZFError::Unsupported)
    // }

    // async fn is_recording(&self) -> bool {
    //     false
    // }

    async fn is_running(&self) -> bool {
        self.handle.is_some()
    }

    async fn stop(&mut self) -> ZFResult<()> {
        // Stop is idempotent, if the node was already stopped,
        // do nothing and return Ok(())

        if let Some(abort_handle) = self.abort_handle.take() {
            abort_handle.abort()
        }

        if let Some(handle) = self.handle.take() {
            log::trace!("Sink handler finished with {:?}", handle.await);
        }

        Ok(())
    }

    async fn start(&mut self) -> ZFResult<()> {
        // Start is idempotent, if the node was already started,
        // do nothing and return Ok(())
        if self.handle.is_some() && self.abort_handle.is_some() {
            log::warn!(
                "[Sink: {}] Trying to start while it is already started, aborting",
                self.id
            );
            return Ok(());
        }

        log::trace!("[Sink: {}] Starting", self.id);

        let iteration = self
            .sink
            .setup(&self.configuration, self.inputs.clone())
            .await?;

        let c_id = self.id.clone();
        let run_loop = async move {
            let mut instant: Instant;
            loop {
                instant = Instant::now();
                if let Err(e) = iteration.call().await {
                    log::error!("[Sink: {c_id}] {:?}", e);
                    return e;
                }

                log::trace!(
                    "[Sink: {c_id}] iteration took: {}ms",
                    instant.elapsed().as_millis()
                );

                async_std::task::yield_now().await;
            }
        };

        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let handle = async_std::task::spawn(Abortable::new(run_loop, abort_registration));

        self.handle = Some(handle);
        self.abort_handle = Some(abort_handle);
        Ok(())
    }
}

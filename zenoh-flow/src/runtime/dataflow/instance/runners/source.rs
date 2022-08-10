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

use crate::async_std::sync::Arc;
use crate::runtime::dataflow::instance::runners::{Runner, RunnerKind};
use crate::runtime::dataflow::node::SourceLoaded;
use crate::runtime::InstanceContext;
use crate::types::ZFResult;
use crate::{Configuration, Context, NodeId, Output, PortId, Source, ZFError};
use async_std::task::JoinHandle;
use async_trait::async_trait;
use futures::future::{AbortHandle, Abortable, Aborted};
use std::collections::HashMap;
use std::time::Instant;

#[cfg(target_family = "unix")]
use libloading::os::unix::Library;
#[cfg(target_family = "windows")]
use libloading::Library;

/// The `SourceRunner` is the component in charge of executing the source.
/// It contains all the runtime information for the source, the graph instance.
///
/// Do not reorder the fields in this struct.
/// Rust drops fields in a struct in the same order they are declared.
/// Ref: <https://doc.rust-lang.org/reference/destructors.html>
/// We need the state to be dropped before the source/lib, otherwise we
/// will have a SIGSEV.
pub struct SourceRunner {
    pub(crate) id: NodeId,
    pub(crate) configuration: Option<Configuration>,
    pub(crate) context: Context,
    pub(crate) outputs: HashMap<PortId, Output>,
    pub(crate) source: Arc<dyn Source>,
    pub(crate) _library: Option<Arc<Library>>,
    pub(crate) handle: Option<JoinHandle<Result<ZFError, Aborted>>>,
    pub(crate) abort_handle: Option<AbortHandle>,
    pub(crate) callbacks_handle: Option<JoinHandle<Result<ZFError, Aborted>>>,
    pub(crate) callbacks_abort_handle: Option<AbortHandle>,
}

impl SourceRunner {
    /// Tries to create a new `SourceRunner` using the given
    /// [`InstanceContext`](`InstanceContext`), [`SourceLoaded`](`SourceLoaded`)
    /// and [`OperatorIO`](`OperatorIO`).
    ///
    /// # Errors
    /// If fails if the output is not connected.
    pub fn new(
        instance_context: Arc<InstanceContext>,
        source: SourceLoaded,
        outputs: HashMap<PortId, Output>,
    ) -> Self {
        Self {
            id: source.id,
            context: Context::new(instance_context),
            configuration: source.configuration,
            outputs,
            source: source.source,
            _library: source.library,
            handle: None,
            abort_handle: None,
            callbacks_handle: None,
            callbacks_abort_handle: None,
        }
    }

    // /// Records the given `message`.
    // ///
    // /// # Errors
    // /// An error variant is returned in case of:
    // /// - unable to put on zenoh
    // /// - serialization fails
    // async fn record(&self, message: Arc<Message>) -> ZFResult<()> {
    //     log::trace!("ZenohLogger IN <= {:?} ", message);
    //     let recording = self.is_recording.lock().await;

    //     if !(*recording) {
    //         log::trace!("ZenohLogger Dropping!");
    //         return Ok(());
    //     }

    //     let resource_name_guard = self.current_recording_resource.lock().await;
    //     let resource_id_guard = self.current_recording_resource_id.lock().await;
    //     let resource_id = resource_id_guard.as_ref().ok_or(ZFError::Unimplemented)?;

    //     let serialized = message.serialize_bincode()?;
    //     log::trace!(
    //         "ZenohLogger - {:?} => {:?} ",
    //         resource_name_guard,
    //         serialized
    //     );
    //     self.context
    //         .runtime
    //         .session
    //         .put(&*resource_id, serialized)
    //         .congestion_control(CongestionControl::Block)
    //         .await?;

    //     Ok(())
    // }
}

#[async_trait]
impl Runner for SourceRunner {
    fn get_id(&self) -> NodeId {
        self.id.clone()
    }

    fn get_kind(&self) -> RunnerKind {
        RunnerKind::Source
    }

    // async fn start_recording(&self) -> ZFResult<String> {
    //     let mut is_recording_guard = self.is_recording.lock().await;
    //     if !(*is_recording_guard) {
    //         let ts_recording_start = self.context.runtime.hlc.new_timestamp();
    //         let resource_name = format!(
    //             "{}/{}",
    //             self.base_resource_name,
    //             ts_recording_start.get_time()
    //         );

    //         let recording_id = self
    //             .context
    //             .runtime
    //             .session
    //             .declare_expr(&resource_name)
    //             .await?;

    //         *(self.current_recording_resource_id.lock().await) = Some(recording_id);
    //         *(self.current_recording_resource.lock().await) = Some(resource_name.clone());

    //         let recording_metadata = RecordingMetadata {
    //             timestamp: ts_recording_start,
    //             port_id: self.output.port_id.clone(),
    //             node_id: self.id.clone(),
    //             flow_id: self.context.flow_id.clone(),
    //             instance_id: self.context.instance_id,
    //         };

    //         let message = Message::Control(ControlMessage::RecordingStart(recording_metadata));
    //         let serialized = message.serialize_bincode()?;
    //         log::trace!(
    //             "ZenohLogger - {} - Started recording at {:?}",
    //             resource_name,
    //             ts_recording_start
    //         );
    //         self.context
    //             .runtime
    //             .session
    //             .put(&resource_name, serialized)
    //             .await?;
    //         *is_recording_guard = true;
    //         return Ok(resource_name);
    //     }
    //     return Err(ZFError::AlreadyRecording);
    // }

    // async fn stop_recording(&self) -> ZFResult<String> {
    //     let mut is_recording_guard = self.is_recording.lock().await;
    //     if *is_recording_guard {
    //         let mut resource_name_guard = self.current_recording_resource.lock().await;

    //         let resource_name = resource_name_guard
    //             .as_ref()
    //             .ok_or(ZFError::Unimplemented)?
    //             .clone();

    //         let mut resource_id_guard = self.current_recording_resource_id.lock().await;
    //         let resource_id = resource_id_guard.ok_or(ZFError::Unimplemented)?;

    //         let ts_recording_stop = self.context.runtime.hlc.new_timestamp();
    //         let message = Message::Control(ControlMessage::RecordingStop(ts_recording_stop));
    //         let serialized = message.serialize_bincode()?;
    //         log::debug!(
    //             "ZenohLogger - {} - Stop recording at {:?}",
    //             resource_name,
    //             ts_recording_stop
    //         );
    //         self.context
    //             .runtime
    //             .session
    //             .put(resource_id, serialized)
    //             .await?;

    //         *is_recording_guard = false;
    //         *resource_name_guard = None;
    //         *resource_id_guard = None;

    //         self.context
    //             .runtime
    //             .session
    //             .undeclare_expr(resource_id)
    //             .await?;

    //         return Ok(resource_name);
    //     }
    //     return Err(ZFError::NotRecording);
    // }

    // async fn is_recording(&self) -> bool {
    //     *self.is_recording.lock().await
    // }

    async fn is_running(&self) -> bool {
        self.handle.is_some() || self.callbacks_handle.is_some()
    }

    async fn stop(&mut self) -> ZFResult<()> {
        // Stop is idempotent, if the node was already stopped,
        // do nothing and return Ok(())

        if let Some(abort_handle) = self.abort_handle.take() {
            abort_handle.abort()
        }

        if let Some(handle) = self.handle.take() {
            log::trace!("Source handler finished with {:?}", handle.await);
        }

        Ok(())
    }

    async fn start(&mut self) -> ZFResult<()> {
        // Start is idempotent, if the node was already started,
        // do nothing and return Ok(())
        if self.handle.is_some() && self.abort_handle.is_some() {
            log::warn!(
                "[Source: {}] Trying to start while it is already started, aborting",
                self.id
            );
            return Ok(());
        }

        log::trace!("[Source: {}] Starting", self.id);

        let iteration = self
            .source
            .setup(&mut self.context, &self.configuration, self.outputs.clone())
            .await?;

        /* Callbacks */

        let callbacks = std::mem::take(&mut self.context.callback_senders);
        if !callbacks.is_empty() {
            let c_id = self.id.clone();
            let callbacks_loop = async move {
                let mut cbs: Vec<_> = callbacks
                    .iter()
                    .map(|callback| Box::pin(callback.trigger()))
                    .collect();

                loop {
                    let (res, _, remainings) = futures::future::select_all(cbs).await;
                    cbs = remainings;
                    match res {
                        Err(e) => {
                            log::error!("[Source: {c_id}] {:?}", e);
                            return e;
                        }
                        Ok(index) => {
                            cbs.push(Box::pin(callbacks[index].trigger()));
                        }
                    }

                    async_std::task::yield_now().await;
                }
            };

            let (cb_abort_handle, cb_abort_registration) = AbortHandle::new_pair();
            let cb_handle =
                async_std::task::spawn(Abortable::new(callbacks_loop, cb_abort_registration));

            self.callbacks_handle = Some(cb_handle);
            self.callbacks_abort_handle = Some(cb_abort_handle);
        }

        /* Streams */
        if let Some(iteration) = iteration {
            let c_id = self.id.clone();

            let run_loop = async move {
                let mut instant: Instant;
                loop {
                    instant = Instant::now();
                    if let Err(e) = iteration.call().await {
                        log::error!("[Source: {c_id}] {:?}", e);
                        return e;
                    }

                    log::trace!(
                        "[Source: {c_id}] iteration took: {}ms",
                        instant.elapsed().as_millis()
                    );

                    async_std::task::yield_now().await;
                }
            };

            let (abort_handle, abort_registration) = AbortHandle::new_pair();
            let handle = async_std::task::spawn(Abortable::new(run_loop, abort_registration));

            self.handle = Some(handle);
            self.abort_handle = Some(abort_handle);
        }

        Ok(())
    }
}

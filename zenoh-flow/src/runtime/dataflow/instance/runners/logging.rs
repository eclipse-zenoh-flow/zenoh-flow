//
// Copyright (c) 2017, 2021 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//

use crate::async_std::sync::{Arc, Mutex};

use crate::runtime::dataflow::instance::link::LinkReceiver;

use crate::runtime::message::Message;
use crate::runtime::InstanceContext;
use crate::{ControlMessage, NodeId, PortId, RecordingMetadata, ZFError, ZFResult};

use futures_lite::FutureExt;

use super::RunAction;

pub struct LoggerManager {}

#[derive(Clone)]
pub struct ZenohLogger {
    pub(crate) id: NodeId,
    pub(crate) context: InstanceContext,
    pub(crate) base_resource_name: String,
    pub(crate) port_id: PortId,
    pub(crate) node_id: NodeId,
    pub(crate) current_recording_resource: Arc<Mutex<Option<String>>>,
    pub(crate) is_recording: Arc<Mutex<bool>>,
    pub(crate) link: Arc<LinkReceiver<Message>>,
}

impl ZenohLogger {
    pub fn try_new(
        id: NodeId,
        context: InstanceContext,
        base_resource_name: String,
        port_id: PortId,
        node_id: NodeId,
        link: LinkReceiver<Message>,
    ) -> ZFResult<Self> {
        Ok(Self {
            id,
            context,
            base_resource_name,
            port_id,
            node_id,
            current_recording_resource: Arc::new(Mutex::new(None)),
            is_recording: Arc::new(Mutex::new(false)),
            link: Arc::new(link),
        })
    }

    async fn run_stoppable(&self, stop: crate::async_std::channel::Receiver<()>) -> ZFResult<()> {
        loop {
            let run = async {
                match self.run().await {
                    Ok(_) => RunAction::RestartRun(None),
                    Err(e) => RunAction::RestartRun(Some(e)),
                }
            };
            let stopper = async {
                match stop.recv().await {
                    Ok(_) => RunAction::Stop,
                    Err(e) => RunAction::StopError(e),
                }
            };

            match run.race(stopper).await {
                RunAction::RestartRun(e) => {
                    log::error!(
                        "[Node: {}] The run loop exited with {:?}, restarting…",
                        self.get_id(),
                        e
                    );
                    continue;
                }
                RunAction::Stop => {
                    log::trace!(
                        "[Node: {}] Received kill command, killing runner",
                        self.get_id()
                    );
                    break Ok(());
                }
                RunAction::StopError(e) => {
                    log::error!(
                        "[Node {}] The stopper recv got an error: {}, exiting...",
                        self.get_id(),
                        e
                    );
                    break Err(e.into());
                }
            }
        }
    }
    pub fn start(&self) -> crate::async_std::channel::Sender<()> {
        let (s, r) = crate::async_std::channel::bounded::<()>(1);
        let cloned_self = self.clone();

        let _h = async_std::task::spawn(async move { cloned_self.run_stoppable(r).await });
        s
    }

    pub async fn start_recording(&self) -> ZFResult<()> {
        let mut guard = self.is_recording.lock().await;

        let ts_recoding_start = self.context.runtime.hlc.new_timestamp();
        let resource_name = format!(
            "{}/{}",
            self.base_resource_name,
            ts_recoding_start.get_time().to_string()
        );

        *(self.current_recording_resource.lock().await) = Some(resource_name.clone());

        let recording_metadata = RecordingMetadata {
            timestamp: ts_recoding_start,
            port_id: self.port_id.clone(),
            node_id: self.node_id.clone(),
            flow_id: self.context.flow_id.clone(),
            instance_id: self.context.instance_id,
        };

        let message = Message::Control(ControlMessage::RecordingStart(recording_metadata));
        let serialized = message.serialize_bincode()?;
        log::debug!(
            "ZenohLogger - {} - Started recoding at {:?}",
            resource_name,
            ts_recoding_start
        );
        self.context
            .runtime
            .session
            .write(&resource_name.into(), serialized.into())
            .await?;
        *guard = true;
        Ok(())
    }

    pub async fn stop_recording(&self) -> ZFResult<()> {
        let mut guard = self.is_recording.lock().await;
        let mut resource_name_guard = self.current_recording_resource.lock().await;

        let resource_name = resource_name_guard
            .as_ref()
            .ok_or(ZFError::Unimplemented)?
            .clone();

        let ts_recoding_stop = self.context.runtime.hlc.new_timestamp();
        let message = Message::Control(ControlMessage::RecordingStop(ts_recoding_stop));
        let serialized = message.serialize_bincode()?;
        log::debug!(
            "ZenohLogger - {} - Stop recoding at {:?}",
            resource_name,
            ts_recoding_stop
        );
        self.context
            .runtime
            .session
            .write(&resource_name.into(), serialized.into())
            .await?;

        *guard = false;
        *resource_name_guard = None;
        Ok(())
    }

    fn get_id(&self) -> NodeId {
        self.id.clone()
    }

    async fn run(&self) -> ZFResult<()> {
        while let Ok((_, message)) = self.link.recv().await {
            log::debug!("ZenohLogger IN <= {:?} ", message);
            let recording = self.is_recording.lock().await;

            if !(*recording) {
                log::debug!("ZenohLogger Dropping!");
                continue;
            }

            let resource_name_guard = self.current_recording_resource.lock().await;
            let resource_name = resource_name_guard
                .as_ref()
                .ok_or(ZFError::Unimplemented)?
                .clone();

            let serialized = message.serialize_bincode()?;
            log::debug!("ZenohLogger - {} => {:?} ", resource_name, serialized);
            self.context
                .runtime
                .session
                .write(&resource_name.clone().into(), serialized.into())
                .await?;
        }

        Err(ZFError::Disconnected)
    }
}

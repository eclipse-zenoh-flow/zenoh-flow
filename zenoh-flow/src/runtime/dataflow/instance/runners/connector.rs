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

use crate::async_std::sync::{Arc, Mutex};
use crate::model::connector::ZFConnectorRecord;
use crate::runtime::dataflow::instance::runners::{Runner, RunnerKind};
use crate::runtime::InstanceContext;
use crate::{Input, Message, NodeId, Output, PortId, ZFError, ZFResult};
use async_trait::async_trait;
use futures::StreamExt;
use std::collections::HashMap;
use zenoh::prelude::*;
use zenoh::publication::CongestionControl;

/// The `ZenohSender` is the connector that sends the data to Zenoh
/// when nodes are running on different runtimes.
#[derive(Clone)]
pub struct ZenohSender {
    pub(crate) id: NodeId,
    pub(crate) instance_context: Arc<InstanceContext>,
    pub(crate) record: ZFConnectorRecord,
    pub(crate) is_running: Arc<Mutex<bool>>,
    pub(crate) link: Input,
    pub(crate) key_expr: ExprId,
}

impl ZenohSender {
    /// Creates a new `ZenohSender` with the given parameters.
    ///
    /// # Errors
    /// An error variant is returned if the link is not supposed to be
    /// connected to this node.
    /// Or if the resource declaration in Zenoh fails.
    pub fn try_new(
        instance_context: Arc<InstanceContext>,
        record: ZFConnectorRecord,
        mut inputs: HashMap<PortId, Input>,
    ) -> ZFResult<Self> {
        let port_id = record.link_id.port_id.clone();
        let input = inputs.remove(&port_id).ok_or_else(|| {
            ZFError::IOError(format!(
                "Link < {} > was not created for Connector < {} >.",
                &port_id, &record.id
            ))
        })?;

        // Declaring the resource to reduce network overhead.
        let key_expr = instance_context
            .runtime
            .session
            .declare_expr(&record.resource)
            .wait()?;

        Ok(Self {
            id: record.id.clone(),
            instance_context,
            record,
            is_running: Arc::new(Mutex::new(false)),
            link: input,
            key_expr,
        })
    }

    // /// Starts the the sender.
    // async fn start(&self) {
    //     *self.is_running.lock().await = true;
    // }

    /// A single sender iteration
    ///
    /// # Errors
    /// An error variant is returned if:
    /// - serialization fails
    /// - zenoh put fails
    /// - link recv fails
    async fn iteration(&self) -> ZFResult<()> {
        log::debug!("ZenohSender - {} - Started", self.record.resource);
        // if let Some(link) = &*self.link.lock().await {
        while let Ok(message) = self.link.recv_async().await {
            log::trace!("ZenohSender IN <= {:?} ", message);

            let serialized = message.serialize_bincode()?;
            log::trace!("ZenohSender - {}=>{:?} ", self.record.resource, serialized);
            self.instance_context
                .runtime
                .session
                .put(&self.key_expr, serialized)
                .congestion_control(CongestionControl::Block)
                .await?;
        }
        // } else {
        //     return Err(ZFError::Disconnected);
        // }
        Ok(())
    }
}

#[async_trait]
impl Runner for ZenohSender {
    fn get_id(&self) -> NodeId {
        self.id.clone()
    }

    fn get_kind(&self) -> RunnerKind {
        RunnerKind::Connector
    }

    async fn start(&mut self) -> ZFResult<()> {
        *self.is_running.lock().await = true;

        // Looping on iteration, each iteration is a single
        // run of the source, as a run can fail in case of error it
        // stops and returns the error to the caller (the RunnerManager)

        loop {
            match self.iteration().await {
                Ok(_) => {
                    log::trace!("[ZenohSender: {}] iteration ok", self.id);
                    continue;
                }
                Err(e) => {
                    log::error!(
                        "[ZenohSender: {}] iteration failed with error: {}",
                        self.id,
                        e
                    );
                    self.stop().await?;
                    break Err(e);
                }
            }
        }
    }

    // fn get_outputs(&self) -> HashMap<PortId, PortType> {
    //     let mut outputs = HashMap::with_capacity(1);
    //     outputs.insert(
    //         self.record.link_id.port_id.clone(),
    //         self.record.link_id.port_type.clone(),
    //     );
    //     outputs
    // }

    // fn get_inputs(&self) -> HashMap<PortId, PortType> {
    //     HashMap::with_capacity(0)
    // }

    // async fn add_input(&self, input: LinkReceiver) -> ZFResult<()> {
    //     *(self.link.lock().await) = Some(input);
    //     Ok(())
    // }

    // async fn add_output(&self, _output: LinkSender) -> ZFResult<()> {
    //     Err(ZFError::SenderDoNotHaveOutputs)
    // }

    // async fn get_outputs_links(&self) -> HashMap<PortId, Vec<LinkSender>> {
    //     HashMap::with_capacity(0)
    // }

    // async fn take_input_links(&self) -> HashMap<PortId, LinkReceiver> {
    //     let mut link_guard = self.link.lock().await;
    //     if let Some(link) = &*link_guard {
    //         let mut inputs = HashMap::with_capacity(1);
    //         inputs.insert(self.record.link_id.port_id.clone(), link.clone());
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
        *self.is_running.lock().await
    }

    async fn stop(&mut self) -> ZFResult<()> {
        self.instance_context
            .runtime
            .session
            .undeclare_expr(self.key_expr)
            .await?;

        *self.is_running.lock().await = false;
        Ok(())
    }

    async fn clean(&self) -> ZFResult<()> {
        Ok(())
    }
}

/// A `ZenohReceiver` receives the messages from Zenoh when nodes are running
/// on different runtimes.
#[derive(Clone)]
pub struct ZenohReceiver {
    pub(crate) id: NodeId,
    pub(crate) instance_context: Arc<InstanceContext>,
    pub(crate) record: ZFConnectorRecord,
    pub(crate) is_running: Arc<Mutex<bool>>,
    pub(crate) key_expr: ExprId,
    // pub(crate) link: Arc<Mutex<Option<LinkSender>>>,
    pub(crate) link: Output,
}

impl ZenohReceiver {
    /// Creates a new `ZenohReceiver` with the given parametes.
    ///
    /// # Errors
    /// An error variant is returned if the link is not supposed to be
    /// connected to this node.
    pub fn try_new(
        instance_context: Arc<InstanceContext>,
        record: ZFConnectorRecord,
        mut outputs: HashMap<PortId, Output>,
    ) -> ZFResult<Self> {
        let port_id = record.link_id.port_id.clone();
        let output = outputs.remove(&port_id).ok_or_else(|| {
            ZFError::IOError(format!(
                "Link < {} > was not created for Connector < {} >.",
                &port_id, &record.id
            ))
        })?;

        let key_expr = instance_context
            .runtime
            .session
            .declare_expr(&record.resource)
            .wait()?;

        Ok(Self {
            id: record.id.clone(),
            instance_context,
            record,
            key_expr,
            is_running: Arc::new(Mutex::new(false)),
            link: output,
        })
    }
}

#[async_trait]
impl Runner for ZenohReceiver {
    fn get_id(&self) -> NodeId {
        self.id.clone()
    }

    fn get_kind(&self) -> RunnerKind {
        RunnerKind::Connector
    }

    async fn start(&mut self) -> ZFResult<()> {
        *self.is_running.lock().await = true;

        let res = {
            log::debug!("ZenohReceiver - {} - Started", self.record.resource);
            // if let Some(link) = &*self.link.lock().await {
            let mut subscriber = self
                .instance_context
                .runtime
                .session
                .subscribe(&self.key_expr)
                .await?;

            while let Some(msg) = subscriber.receiver().next().await {
                log::trace!("ZenohSender - {}<={:?} ", self.record.resource, msg);
                let de: Message = bincode::deserialize(&msg.value.payload.contiguous())
                    .map_err(|_| ZFError::DeseralizationError)?;
                log::trace!("ZenohSender - OUT =>{:?} ", de);
                self.link.send_to_all_async(de).await?;
            }
            // }

            Err(ZFError::Disconnected)
        };

        self.stop().await?;
        res
    }

    // fn get_inputs(&self) -> HashMap<PortId, PortType> {
    //     HashMap::with_capacity(0)
    // }

    // fn get_outputs(&self) -> HashMap<PortId, PortType> {
    //     let mut inputs = HashMap::with_capacity(1);
    //     inputs.insert(
    //         self.record.link_id.port_id.clone(),
    //         self.record.link_id.port_type.clone(),
    //     );
    //     inputs
    // }
    // async fn add_output(&self, output: LinkSender) -> ZFResult<()> {
    //     (*self.link.lock().await) = Some(output);
    //     Ok(())
    // }

    // async fn add_input(&self, _input: LinkReceiver) -> ZFResult<()> {
    //     Err(ZFError::ReceiverDoNotHaveInputs)
    // }

    // async fn get_outputs_links(&self) -> HashMap<PortId, Vec<LinkSender>> {
    //     let link_guard = self.link.lock().await;
    //     if let Some(link) = &*link_guard {
    //         let mut outputs = HashMap::with_capacity(1);
    //         outputs.insert(self.record.link_id.port_id.clone(), vec![link.clone()]);
    //         return outputs;
    //     }
    //     HashMap::with_capacity(0)
    // }

    // async fn take_input_links(&self) -> HashMap<PortId, LinkReceiver> {
    //     HashMap::with_capacity(0)
    // }

    async fn clean(&self) -> ZFResult<()> {
        Ok(())
    }

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
        *self.is_running.lock().await
    }

    async fn stop(&mut self) -> ZFResult<()> {
        self.instance_context
            .runtime
            .session
            .undeclare_expr(self.key_expr)
            .await?;

        *self.is_running.lock().await = false;
        Ok(())
    }
}

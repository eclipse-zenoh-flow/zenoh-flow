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
use crate::model::connector::ZFConnectorRecord;
use crate::runtime::dataflow::instance::runners::{Runner, RunnerKind};
use crate::runtime::InstanceContext;
use crate::{Input, Message, NodeId, Output, PortId, ZFError, ZFResult};
use async_std::task::JoinHandle;
use async_trait::async_trait;
use futures::stream::{AbortHandle, Abortable, Aborted};
use futures::StreamExt;
use std::collections::HashMap;
use zenoh::prelude::*;
use zenoh::publication::CongestionControl;

/// The `ZenohSender` is the connector that sends the data to Zenoh
/// when nodes are running on different runtimes.
pub struct ZenohSender {
    pub(crate) id: NodeId,
    pub(crate) instance_context: Arc<InstanceContext>,
    pub(crate) record: ZFConnectorRecord,
    pub(crate) handle: Option<JoinHandle<Result<ZFResult<()>, Aborted>>>,
    pub(crate) abort_handle: Option<AbortHandle>,
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
            handle: None,
            abort_handle: None,
            link: input,
            key_expr,
        })
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
        // Start is idempotent, if the node was already started,
        // do nothing and return Ok(())
        if self.handle.is_some() && self.abort_handle.is_some() {
            log::warn!(
                "[Connector: {}] Trying to start while it is already started, aborting",
                self.id
            );
            return Ok(());
        }

        // Looping on iteration, each iteration is a single
        // run of the source, as a run can fail in case of error it
        // stops and returns the error to the caller (the RunnerManager)

        let c_record = self.record.clone();
        let c_link = self.link.clone();
        let c_keyexpr = self.key_expr.clone();
        let c_instance_ctx = self.instance_context.clone();
        let c_id = self.id.clone();

        let run_loop = async move {
            loop {
                let res: ZFResult<()> = {
                    log::debug!("[ZenohSender: {c_id}] - {} - Started", c_record.resource);
                    // if let Some(link) = &*self.link.lock().await {
                    while let Ok(message) = c_link.recv_async().await {
                        log::trace!("[ZenohSender: {c_id}] IN <= {:?} ", message);

                        let serialized = message.serialize_bincode()?;
                        log::trace!(
                            "[ZenohSender: {c_id}] - {}=>{:?} ",
                            c_record.resource,
                            serialized
                        );
                        c_instance_ctx
                            .runtime
                            .session
                            .put(&c_keyexpr, serialized)
                            .congestion_control(CongestionControl::Block)
                            .await?;
                    }
                    Ok(())
                };

                if let Err(e) = res {
                    log::error!("[ZenohSender: {}] iteration failed with error: {}", c_id, e);
                    return Err(e);
                }
                log::trace!("[ZenohSender: {}] iteration ok", c_id);
            }
        };

        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let handle = async_std::task::spawn(Abortable::new(run_loop, abort_registration));

        self.handle = Some(handle);
        self.abort_handle = Some(abort_handle);

        Ok(())
    }

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
            log::trace!("Operator handler finished with {:?}", handle.await);
        }

        self.instance_context
            .runtime
            .session
            .undeclare_expr(self.key_expr)
            .await?;

        Ok(())
    }

    async fn clean(&self) -> ZFResult<()> {
        Ok(())
    }
}

/// A `ZenohReceiver` receives the messages from Zenoh when nodes are running
/// on different runtimes.
pub struct ZenohReceiver {
    pub(crate) id: NodeId,
    pub(crate) instance_context: Arc<InstanceContext>,
    pub(crate) record: ZFConnectorRecord,
    pub(crate) handle: Option<JoinHandle<Result<ZFResult<()>, Aborted>>>,
    pub(crate) abort_handle: Option<AbortHandle>,
    pub(crate) key_expr: ExprId,
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
            handle: None,
            abort_handle: None,
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
        // Start is idempotent, if the node was already started,
        // do nothing and return Ok(())
        if self.handle.is_some() && self.abort_handle.is_some() {
            log::warn!(
                "[Connector: {}] Trying to start while it is already started, aborting",
                self.id
            );
            return Ok(());
        }

        let c_record = self.record.clone();
        let c_link = self.link.clone();
        let c_keyexpr = self.key_expr.clone();
        let c_instance_ctx = self.instance_context.clone();
        let c_id = self.id.clone();

        let run_loop = async move {
            log::debug!("[ZenohReceiver: {c_id}] - {} - Started", c_record.resource);
            let mut subscriber = c_instance_ctx.runtime.session.subscribe(&c_keyexpr).await?;

            while let Some(msg) = subscriber.receiver().next().await {
                log::trace!("[ZenohReceiver: {c_id}] - {}<={msg:?} ", c_record.resource);
                let de: Message = bincode::deserialize(&msg.value.payload.contiguous())
                    .map_err(|_| ZFError::DeseralizationError)?;
                log::trace!("[ZenohReceiver: {c_id}] - OUT =>{de:?} ");
                c_link.send_to_all_async(de).await?;
            }
            // }

            Err(ZFError::Disconnected)
        };

        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let handle = async_std::task::spawn(Abortable::new(run_loop, abort_registration));

        self.handle = Some(handle);
        self.abort_handle = Some(abort_handle);

        Ok(())
    }

    async fn clean(&self) -> ZFResult<()> {
        Ok(())
    }

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
            log::trace!("Operator handler finished with {:?}", handle.await);
        }

        self.instance_context
            .runtime
            .session
            .undeclare_expr(self.key_expr)
            .await?;

        Ok(())
    }
}

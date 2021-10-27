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

use crate::async_std::sync::{Arc, RwLock};
use crate::model::connector::ZFConnectorRecord;
use crate::runtime::dataflow::instance::link::{LinkReceiver, LinkSender};
use crate::runtime::dataflow::instance::runners::operator::OperatorIO;
use crate::runtime::message::Message;
use crate::runtime::RuntimeContext;
use crate::{NodeId, ZFError, ZFResult};
use futures::prelude::*;
use zenoh::net::{Reliability, SubInfo, SubMode};

#[derive(Clone)]
pub struct ZenohSender {
    pub(crate) id: NodeId,
    pub(crate) runtime_context: RuntimeContext,
    pub(crate) record: ZFConnectorRecord,
    pub(crate) link: Arc<RwLock<LinkReceiver<Message>>>,
}

impl ZenohSender {
    pub fn try_new(
        context: RuntimeContext,
        record: ZFConnectorRecord,
        io: OperatorIO,
    ) -> ZFResult<Self> {
        let (mut inputs, _) = io.take();
        let port_id: Arc<str> = record.link_id.port_id.clone().into();
        let link = inputs.remove(&port_id).ok_or_else(|| {
            ZFError::IOError(format!(
                "Link < {} > was not created for Connector < {} >.",
                &port_id, &record.id
            ))
        })?;

        Ok(Self {
            id: record.id.clone(),
            runtime_context: context,
            record,
            link: Arc::new(RwLock::new(link)),
        })
    }

    pub async fn run(&self) -> ZFResult<()> {
        log::debug!("ZenohSender - {} - Started", self.record.resource);
        let guard = self.link.read().await;
        while let Ok((_, message)) = (*guard).recv().await {
            log::debug!("ZenohSender IN <= {:?} ", message);

            let serialized = message.serialize_bincode()?;
            log::debug!("ZenohSender - {}=>{:?} ", self.record.resource, serialized);
            self.runtime_context
                .session
                .write(&self.record.resource.clone().into(), serialized.into())
                .await?;
        }

        Err(ZFError::Disconnected)
    }

    pub async fn add_input(&self, input: LinkReceiver<Message>) {
        *(self.link.write().await) = input;
    }
}

#[derive(Clone)]
pub struct ZenohReceiver {
    pub(crate) id: NodeId,
    pub(crate) runtime_context: RuntimeContext,
    pub(crate) record: ZFConnectorRecord,
    pub(crate) link: Arc<RwLock<LinkSender<Message>>>,
}

impl ZenohReceiver {
    pub fn try_new(
        context: RuntimeContext,
        record: ZFConnectorRecord,
        io: OperatorIO,
    ) -> ZFResult<Self> {
        let (_, mut outputs) = io.take();
        let port_id: Arc<str> = record.link_id.port_id.clone().into();
        let mut links = outputs.remove(&port_id).ok_or_else(|| {
            ZFError::IOError(format!(
                "Link < {} > was not created for Connector < {} >.",
                &port_id, &record.id
            ))
        })?;

        if links.len() != 1 {
            return Err(ZFError::IOError(format!(
                "Expected exactly one link for port < {} > for Connector < {} >, found: {}",
                &port_id,
                &record.id,
                links.len()
            )));
        }

        let link = links.remove(0);

        Ok(Self {
            id: record.id.clone(),
            runtime_context: context,
            record,
            link: Arc::new(RwLock::new(link)),
        })
    }

    pub async fn run(&self) -> ZFResult<()> {
        log::debug!("ZenohReceiver - {} - Started", self.record.resource);
        let guard = self.link.read().await;
        let sub_info = SubInfo {
            reliability: Reliability::Reliable,
            mode: SubMode::Push,
            period: None,
        };

        let mut subscriber = self
            .runtime_context
            .session
            .declare_subscriber(&self.record.resource.clone().into(), &sub_info)
            .await?;

        while let Some(msg) = subscriber.receiver().next().await {
            log::debug!("ZenohSender - {}<={:?} ", self.record.resource, msg);
            let de: Message = bincode::deserialize(&msg.payload.contiguous())
                .map_err(|_| ZFError::DeseralizationError)?;
            log::debug!("ZenohSender - OUT =>{:?} ", de);
            (*guard).send(Arc::new(de)).await?;
        }

        Err(ZFError::Disconnected)
    }

    pub async fn add_output(&self, output: LinkSender<Message>) {
        (*self.link.write().await) = output;
    }
}

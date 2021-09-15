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
use crate::runtime::graph::link::{LinkReceiver, LinkSender};
use crate::runtime::message::Message;
use crate::{ZFError, ZFResult};
use futures::prelude::*;
use zenoh::net::{Reliability, Session, SubInfo, SubMode};

#[derive(Clone)]
pub struct ZenohSender {
    pub session: Arc<Session>,
    pub resource: String,
    pub input: Arc<RwLock<Option<LinkReceiver<Message>>>>,
}

impl ZenohSender {
    pub fn new(
        session: Arc<Session>,
        resource: String,
        input: Option<LinkReceiver<Message>>,
    ) -> Self {
        Self {
            session,
            resource,
            input: Arc::new(RwLock::new(input)),
        }
    }

    pub async fn run(&self) -> ZFResult<()> {
        log::debug!("ZenohSender - {} - Started", self.resource);
        let guard = self.input.read().await;
        if let Some(input) = &*guard {
            while let Ok((_, message)) = input.recv().await {
                log::debug!("ZenohSender IN <= {:?} ", message);

                let serialized = message.serialize_bincode()?;
                log::debug!("ZenohSender - {}=>{:?} ", self.resource, serialized);
                self.session
                    .write(&self.resource.clone().into(), serialized.into())
                    .await?;
            }
            return Err(ZFError::Disconnected);
        }
        Err(ZFError::Disconnected)
    }

    pub async fn add_input(&self, input: LinkReceiver<Message>) {
        *(self.input.write().await) = Some(input);
    }
}

#[derive(Clone)]
pub struct ZenohReceiver {
    pub session: Arc<Session>,
    pub resource: String,
    pub output: Arc<RwLock<Option<LinkSender<Message>>>>,
}

impl ZenohReceiver {
    pub fn new(
        session: Arc<Session>,
        resource: String,
        output: Option<LinkSender<Message>>,
    ) -> Self {
        Self {
            session,
            resource,
            output: Arc::new(RwLock::new(output)),
        }
    }

    pub async fn run(&self) -> ZFResult<()> {
        log::debug!("ZenohReceiver - {} - Started", self.resource);
        let guard = self.output.read().await;
        if let Some(output) = &*guard {
            let sub_info = SubInfo {
                reliability: Reliability::Reliable,
                mode: SubMode::Push,
                period: None,
            };

            let mut subscriber = self
                .session
                .declare_subscriber(&self.resource.clone().into(), &sub_info)
                .await?;

            while let Some(msg) = subscriber.receiver().next().await {
                log::debug!("ZenohSender - {}<={:?} ", self.resource, msg);
                let de: Message = bincode::deserialize(&msg.payload.contiguous())
                    .map_err(|_| ZFError::DeseralizationError)?;
                log::debug!("ZenohSender - OUT =>{:?} ", de);
                output.send(Arc::new(de)).await?;
            }
            return Err(ZFError::Disconnected);
        }
        Err(ZFError::Disconnected)
    }

    pub async fn add_output(&self, output: LinkSender<Message>) {
        (*self.output.write().await) = Some(output);
    }
}

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


use crate::{ZFError, ZFResult};
use crate::runtime::graph::link::{ZFLinkReceiver, ZFLinkSender};
use crate::runtime::message::{Message, ZFMessage, ZFMsg};
use zenoh::net::{Session, SubInfo, Reliability, SubMode};
use crate::async_std::sync::Arc;
use futures::prelude::*;
use crate::serde::{Deserialize, Serialize};
use crate::types::ZFLinkId;


#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ZFZenohConnectorDescriptor {
    Sender(ZFZenohConnectorInfo),
    Receiver(ZFZenohConnectorInfo)
}

impl std::fmt::Display for ZFZenohConnectorDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ZFZenohConnectorDescriptor::Sender(s) => write!(f, "{} - Kind: Zenoh-Sender", s),
            ZFZenohConnectorDescriptor::Receiver(r) => write!(f, "{} - Kind: Zenoh-Receiver", r),
        }

    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFZenohConnectorInfo {
    pub name : String,
    pub resource: String,
    pub link_id : ZFLinkId,
    pub runtime: Option<String>,
}

impl std::fmt::Display for ZFZenohConnectorInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - {} ", self.name, self.resource)
    }
}


pub struct ZFZenohSender {
    pub session: Arc<Session>,
    pub resource: String,
    pub input: Option<ZFLinkReceiver<ZFMessage>>
}


impl ZFZenohSender {
    pub fn new(session : Arc<Session>, resource: String, input: Option<ZFLinkReceiver<ZFMessage>>) -> Self {
        Self {
            session,
            resource,
            input,
        }
    }


    pub async fn run(&mut self) -> ZFResult<()> {
        log::debug!("ZenohSender - {} - Started", self.resource);
        if let Some(mut input) = self.input.take() {
            while let Ok((_,msg)) = input.recv().await {
                log::debug!("ZenohSender IN <= {:?} ", msg);

                let serialized = match &msg.msg {
                    ZFMsg::Data(data_msg) => {
                        match data_msg {
                            Message::Deserialized(de) => {
                                let se = bincode::serialize(&**de).map_err(|_| ZFError::SerializationError)?;
                                let se_msg = ZFMessage{ts: msg.ts, msg: ZFMsg::Data(Message::new_serialized(se))};
                                bincode::serialize(&se_msg).map_err(|_| ZFError::SerializationError)?
                            },
                            _ => {
                                bincode::serialize(&*msg).map_err(|_| ZFError::SerializationError)?
                            }
                        }
                    },
                    _ => {
                        bincode::serialize(&*msg).map_err(|_| ZFError::SerializationError)?
                    }
                };
                log::debug!("ZenohSender - {}=>{:?} ", self.resource, serialized);
                self.session
                    .write(&self.resource.clone().into(), serialized.into())
                    .await?;
            }
            return Err(ZFError::Disconnected);
        }
        Err(ZFError::Disconnected)

    }


    pub fn add_input(&mut self, input: ZFLinkReceiver<ZFMessage>) {
        self.input = Some(input);
    }
}


pub struct ZFZenohReceiver {
    pub session: Arc<Session>,
    pub resource: String,
    pub output: Option<ZFLinkSender<ZFMessage>>
}


impl ZFZenohReceiver {
    pub fn new(session : Arc<Session>, resource: String, output: Option<ZFLinkSender<ZFMessage>>) -> Self {
        Self {
            session,
            resource,
            output,
        }
    }


    pub async fn run(&mut self) -> ZFResult<()> {

        log::debug!("ZenohReceiver - {} - Started", self.resource);

        if let Some(output) = &self.output {
            let sub_info = SubInfo {
                reliability: Reliability::Reliable,
                mode: SubMode::Push,
                period: None,
            };

            let mut subscriber = self.session
            .declare_subscriber(&self.resource.clone().into(), &sub_info)
            .await?;

            while let Some(msg) = subscriber.receiver().next().await {
                log::debug!("ZenohSender - {}<={:?} ", self.resource, msg);
                let de : ZFMessage = bincode::deserialize(&msg.payload.to_vec()).map_err(|_| ZFError::DeseralizationError)?;
                log::debug!("ZenohSender - OUT =>{:?} ", de);
                output.send(Arc::new(de)).await?;
            }
            return Err(ZFError::Disconnected);
        }
        Err(ZFError::Disconnected)

    }

    pub fn add_output(&mut self, output: ZFLinkSender<ZFMessage>) {
        self.output = Some(output);
    }
}
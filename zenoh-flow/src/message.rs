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

extern crate serde;

use crate::operator::DataTrait;
use async_std::sync::Arc;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

//TODO: improve
#[derive(Clone, Debug)]
pub enum Message {
    Serialized(Vec<u8>),
    Deserialized(Arc<Box<dyn DataTrait>>),
}

impl Message {
    pub fn new_serialized(data: Vec<u8>) -> Self {
        Self::Serialized(data)
    }

    pub fn serialized_data(&self) -> &[u8] {
        match self {
            Self::Serialized(data) => &data,
            _ => panic!(),
        }
    }

    pub fn new_deserialized(data: Arc<Box<dyn DataTrait>>) -> Self {
        Self::Deserialized(data)
    }

    pub fn deserialized_data(&self) -> Arc<Box<dyn DataTrait>> {
        match self {
            Self::Deserialized(data) => data.clone(),
            _ => panic!(),
        }
    }
}


#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZFCtrlMessage {
    ReadyToMigrate,
    ChangeMode(u8, u128),
    Watermark,
}

//TODO: improve, change name
#[derive(Clone, Debug)]
pub enum ZFMsg {
    Data(Message),
    Ctrl(ZFCtrlMessage),
}

//TODO: improve
#[derive(Clone, Debug)]
pub struct ZFMessage {
    pub ts: u128,
    pub msg: ZFMsg,
}

impl ZFMessage {
    pub fn new_deserialized(ts: u128, data: Arc<Box<dyn DataTrait>>) -> Self {
        Self {
            ts,
            msg: ZFMsg::Data(Message::new_deserialized(data)),
        }
    }

    pub fn from_data(data: Arc<Box<dyn DataTrait>>) -> Self {
        Self {
            ts: 0, //placeholder
            msg: ZFMsg::Data(Message::new_deserialized(data)),
        }
    }

    pub fn from_message(msg: Message) -> Self {
        Self {
            ts: 0, //placeholder
            msg: ZFMsg::Data(msg),
        }
    }

    // pub fn data(&self) -> &[u8] {
    //     match &self.msg {
    //         ZFMsg::Data(m) => m.data(),
    //         _ => panic!("Nope"),
    //     }
    // }

    pub fn timestamp(&self) -> &u128 {
        &self.ts
    }
}

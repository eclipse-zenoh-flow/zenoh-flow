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

use crate::DataTrait;
use async_std::sync::Arc;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use uhlc::Timestamp;

// TODO: improve
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZFDataMessage {
    Serialized(Arc<Vec<u8>>),
    #[serde(skip_serializing, skip_deserializing)]
    // Deserialized data is never serialized directly
    Deserialized(Arc<dyn DataTrait>),
}

impl ZFDataMessage {
    pub fn new_serialized(data: Arc<Vec<u8>>) -> Self {
        Self::Serialized(data)
    }

    pub fn serialized_data(&self) -> &[u8] {
        match self {
            Self::Serialized(data) => data,
            _ => panic!(),
        }
    }

    pub fn new_deserialized(data: Arc<dyn DataTrait>) -> Self {
        Self::Deserialized(data)
    }

    pub fn deserialized_data(&self) -> Arc<dyn DataTrait> {
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Message {
    Data(ZFDataMessage),
    Ctrl(ZFCtrlMessage),
}

impl Message {
    pub fn from_raw(data: Arc<Vec<u8>>) -> Self {
        Message::Data(ZFDataMessage::new_serialized(data))
    }

    pub fn from_data(data: Arc<dyn DataTrait>) -> Self {
        Message::Data(ZFDataMessage::new_deserialized(data))
    }
}

// TODO: improve
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZFMessage {
    pub timestamp: Timestamp,
    pub message: Message,
}

impl ZFMessage {
    pub fn from_raw(timestamp: Timestamp, data: Arc<Vec<u8>>) -> Self {
        Self {
            timestamp,
            message: Message::from_raw(data),
        }
    }

    pub fn from_data(timestamp: Timestamp, data: Arc<dyn DataTrait>) -> Self {
        Self {
            timestamp,
            message: Message::from_data(data),
        }
    }

    pub fn from_message(timestamp: Timestamp, msg: ZFDataMessage) -> Self {
        Self {
            timestamp,
            message: Message::Data(msg),
        }
    }
}

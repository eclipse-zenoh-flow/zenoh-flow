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

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub data: Vec<u8>,
}

impl Message {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZFCtrlMessage {
    ReadyToMigrate,
    ChangeMode(String, u128),
    Watermark(u128),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZFMsg {
    Data(Message),
    Ctrl(ZFCtrlMessage),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZFMessage {
    pub ts: u128,
    pub msg: ZFMsg,
}

impl ZFMessage {
    pub fn new_data(ts: u128, data: Vec<u8>) -> Self {
        Self {
            ts,
            msg: ZFMsg::Data(Message { data }),
        }
    }

    pub fn data(&self) -> &[u8] {
        match &self.msg {
            ZFMsg::Data(m) => m.data(),
            _ => panic!("Nope"),
        }
    }

    pub fn timestamp(&self) -> &u128 {
        &self.ts
    }
}

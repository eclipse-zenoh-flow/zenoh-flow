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

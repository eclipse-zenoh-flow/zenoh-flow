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

use crate::{Data, NodeOutput, ZFData, ZFError, ZFResult};
use async_std::sync::Arc;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use uhlc::Timestamp;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataMessage {
    pub data: Data,
    pub timestamp: Timestamp,
}

impl DataMessage {
    pub fn new(data: Data, timestamp: Timestamp) -> Self {
        Self { data, timestamp }
    }

    pub fn new_serialized(data: Arc<Vec<u8>>, timestamp: Timestamp) -> Self {
        Self {
            data: Data::Bytes(data),
            timestamp,
        }
    }

    pub fn new_deserialized(data: Arc<dyn ZFData>, timestamp: Timestamp) -> Self {
        Self {
            data: Data::Typed(data),
            timestamp,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ControlMessage {
    ReadyToMigrate,
    ChangeMode(u8, u128),
    Watermark,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Message {
    Data(DataMessage),
    Control(ControlMessage),
}

impl Message {
    pub fn from_node_output(output: NodeOutput, timestamp: Timestamp) -> Self {
        match output {
            NodeOutput::Control(c) => Self::Control(c),
            NodeOutput::Data(d) => match d {
                Data::Typed(d) => Self::Data(DataMessage::new_deserialized(d, timestamp)),
                Data::Bytes(sd) => Self::Data(DataMessage::new_serialized(sd, timestamp)),
            },
        }
    }

    pub fn from_serdedata(output: Data, timestamp: Timestamp) -> Self {
        match output {
            Data::Typed(data) => Self::Data(DataMessage::new_deserialized(data, timestamp)),
            Data::Bytes(data) => Self::Data(DataMessage::new_serialized(data, timestamp)),
        }
    }

    pub fn serialize_bincode(&self) -> ZFResult<Vec<u8>> {
        match &self {
            Message::Control(_) => {
                bincode::serialize(&self).map_err(|_| ZFError::SerializationError)
            }
            Message::Data(data_message) => match &data_message.data {
                Data::Bytes(_) => {
                    bincode::serialize(&self).map_err(|_| ZFError::SerializationError)
                }
                Data::Typed(_) => {
                    let serialized_data = data_message.data.as_bytes()?;
                    let serialized_message = Message::Data(DataMessage::new_serialized(
                        serialized_data,
                        data_message.timestamp,
                    ));

                    bincode::serialize(&serialized_message).map_err(|_| ZFError::SerializationError)
                }
            },
        }
    }
}

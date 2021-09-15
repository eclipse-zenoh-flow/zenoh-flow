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

use crate::{Data, ZFComponentOutput, ZFError, ZFResult};
use async_std::sync::Arc;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use uhlc::Timestamp;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SerDeData {
    Serialized(Arc<Vec<u8>>),
    #[serde(skip_serializing, skip_deserializing)]
    // Deserialized data is never serialized directly
    Deserialized(Arc<dyn Data>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataMessage {
    pub data: SerDeData,
    pub timestamp: Timestamp,
}

impl DataMessage {
    pub fn new(data: SerDeData, timestamp: Timestamp) -> Self {
        Self { data, timestamp }
    }

    pub fn new_serialized(data: Arc<Vec<u8>>, timestamp: Timestamp) -> Self {
        Self {
            data: SerDeData::Serialized(data),
            timestamp,
        }
    }

    pub fn new_deserialized(data: Arc<dyn Data>, timestamp: Timestamp) -> Self {
        Self {
            data: SerDeData::Deserialized(data),
            timestamp,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZFControlMessage {
    ReadyToMigrate,
    ChangeMode(u8, u128),
    Watermark,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZFMessage {
    Data(DataMessage),
    Control(ZFControlMessage),
}

impl ZFMessage {
    pub fn from_component_output(output: ZFComponentOutput, timestamp: Timestamp) -> Self {
        match output {
            ZFComponentOutput::Control(c) => Self::Control(c),
            ZFComponentOutput::Data(d) => Self::Data(DataMessage::new_deserialized(d, timestamp)),
        }
    }

    pub fn serialize_bincode(&self) -> ZFResult<Vec<u8>> {
        match &self {
            ZFMessage::Control(_) => {
                bincode::serialize(&self).map_err(|_| ZFError::SerializationError)
            }
            ZFMessage::Data(data_message) => match &data_message.data {
                SerDeData::Serialized(_) => {
                    bincode::serialize(&self).map_err(|_| ZFError::SerializationError)
                }
                SerDeData::Deserialized(de) => {
                    let serialized_data = Arc::new(
                        de.try_serialize()
                            .map_err(|_| ZFError::SerializationError)?,
                    );
                    let serialized_message = ZFMessage::Data(DataMessage::new_serialized(
                        serialized_data,
                        data_message.timestamp,
                    ));

                    bincode::serialize(&serialized_message).map_err(|_| ZFError::SerializationError)
                }
            },
        }
    }
}

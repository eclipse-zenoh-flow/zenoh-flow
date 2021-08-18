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

use crate::{DataTrait, ZFComponentOutput, ZFError, ZFResult};
use async_std::sync::Arc;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use uhlc::Timestamp;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ZFSerDeData {
    Serialized(Arc<Vec<u8>>),
    #[serde(skip_serializing, skip_deserializing)]
    // Deserialized data is never serialized directly
    Deserialized(Arc<dyn DataTrait>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ZFDataMessage {
    pub data: ZFSerDeData,
    pub timestamp: Timestamp,
}

impl ZFDataMessage {
    pub fn new(data: ZFSerDeData, timestamp: Timestamp) -> Self {
        Self { data, timestamp }
    }

    pub fn new_serialized(data: Arc<Vec<u8>>, timestamp: Timestamp) -> Self {
        Self {
            data: ZFSerDeData::Serialized(data),
            timestamp,
        }
    }

    pub fn new_deserialized(data: Arc<dyn DataTrait>, timestamp: Timestamp) -> Self {
        Self {
            data: ZFSerDeData::Deserialized(data),
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
    Data(ZFDataMessage),
    Control(ZFControlMessage),
}

impl ZFMessage {
    pub fn from_component_output(output: ZFComponentOutput, timestamp: Timestamp) -> Self {
        match output {
            ZFComponentOutput::Control(c) => Self::Control(c),
            ZFComponentOutput::Data(d) => Self::Data(ZFDataMessage::new_deserialized(d, timestamp)),
        }
    }

    pub fn serialize_bincode(&self) -> ZFResult<Vec<u8>> {
        match &self {
            ZFMessage::Control(_) => {
                bincode::serialize(&self).map_err(|_| ZFError::SerializationError)
            }
            ZFMessage::Data(data_message) => match &data_message.data {
                ZFSerDeData::Serialized(_) => {
                    bincode::serialize(&self).map_err(|_| ZFError::SerializationError)
                }
                ZFSerDeData::Deserialized(de) => {
                    let serialized_data =
                        Arc::new(bincode::serialize(&de).map_err(|_| ZFError::SerializationError)?);
                    let serialized_message = ZFMessage::Data(ZFDataMessage::new_serialized(
                        serialized_data,
                        data_message.timestamp.clone(),
                    ));

                    bincode::serialize(&serialized_message).map_err(|_| ZFError::SerializationError)
                }
            },
        }
    }
}

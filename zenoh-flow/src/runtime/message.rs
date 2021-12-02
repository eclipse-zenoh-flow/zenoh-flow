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

use crate::{Data, FlowId, NodeId, NodeOutput, PortId, ZFData, ZFError, ZFResult};
use async_std::sync::Arc;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, fmt::Debug};
use uhlc::Timestamp;
use uuid::Uuid;

use crate::runtime::deadline::E2EDeadline;

use super::deadline::E2EDeadlineMiss;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataMessage {
    pub(crate) data: Data,
    pub(crate) timestamp: Timestamp,
    pub(crate) end_to_end_deadlines: Vec<E2EDeadline>,
    pub(crate) missed_end_to_end_deadlines: Vec<E2EDeadlineMiss>,
}

impl DataMessage {
    pub fn new(data: Data, timestamp: Timestamp, end_to_end_deadlines: Vec<E2EDeadline>) -> Self {
        Self {
            data,
            timestamp,
            end_to_end_deadlines,
            missed_end_to_end_deadlines: vec![],
        }
    }

    /// Returns a mutable reference over the Data representation (i.e. `Bytes` or `Typed`).
    ///
    /// This method should be called in conjonction with `try_get::<Typed>()` in order to get a
    /// reference of the desired type. For instance:
    ///
    /// `let zf_usise: &ZFUsize = data_message.get_inner_data().try_get::<ZFUsize>()?;`
    ///
    /// Note that the prerequisite for the above code to work is that `ZFUsize` implements the
    /// traits: `ZFData` and `Deserializable`.
    pub fn get_inner_data(&mut self) -> &mut Data {
        &mut self.data
    }

    pub fn get_timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    pub fn get_missed_end_to_end_deadlines(&self) -> &[E2EDeadlineMiss] {
        self.missed_end_to_end_deadlines.as_slice()
    }

    pub fn new_serialized(
        data: Arc<Vec<u8>>,
        timestamp: Timestamp,
        end_to_end_deadlines: Vec<E2EDeadline>,
    ) -> Self {
        Self {
            data: Data::Bytes(data),
            timestamp,
            end_to_end_deadlines,
            missed_end_to_end_deadlines: vec![],
        }
    }

    pub fn new_deserialized(
        data: Arc<dyn ZFData>,
        timestamp: Timestamp,
        end_to_end_deadlines: Vec<E2EDeadline>,
    ) -> Self {
        Self {
            data: Data::Typed(data),
            timestamp,
            end_to_end_deadlines,
            missed_end_to_end_deadlines: vec![],
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordingMetadata {
    pub(crate) timestamp: Timestamp,
    pub(crate) port_id: PortId,
    pub(crate) node_id: NodeId,
    pub(crate) flow_id: FlowId,
    pub(crate) instance_id: Uuid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ControlMessage {
    // These messages are not yet defined, those are some ideas
    // ReadyToMigrate,
    // ChangeMode(u8, u128),
    // Watermark,
    RecordingStart(RecordingMetadata),
    RecordingStop(Timestamp),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Message {
    Data(DataMessage),
    Control(ControlMessage),
}

impl Message {
    pub fn from_node_output(
        output: NodeOutput,
        timestamp: Timestamp,
        end_to_end_deadlines: Vec<E2EDeadline>,
    ) -> Self {
        match output {
            NodeOutput::Control(c) => Self::Control(c),
            NodeOutput::Data(d) => match d {
                Data::Typed(d) => Self::Data(DataMessage::new_deserialized(
                    d,
                    timestamp,
                    end_to_end_deadlines,
                )),
                Data::Bytes(sd) => Self::Data(DataMessage::new_serialized(
                    sd,
                    timestamp,
                    end_to_end_deadlines,
                )),
            },
        }
    }

    pub fn from_serdedata(
        output: Data,
        timestamp: Timestamp,
        end_to_end_deadlines: Vec<E2EDeadline>,
    ) -> Self {
        match output {
            Data::Typed(data) => Self::Data(DataMessage::new_deserialized(
                data,
                timestamp,
                end_to_end_deadlines,
            )),
            Data::Bytes(data) => Self::Data(DataMessage::new_serialized(
                data,
                timestamp,
                end_to_end_deadlines,
            )),
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
                    let serialized_data = data_message.data.try_as_bytes()?;
                    let serialized_message = Message::Data(DataMessage::new_serialized(
                        serialized_data,
                        data_message.timestamp,
                        data_message.end_to_end_deadlines.clone(),
                    ));

                    bincode::serialize(&serialized_message).map_err(|_| ZFError::SerializationError)
                }
            },
        }
    }

    pub fn get_timestamp(&self) -> Timestamp {
        match self {
            Self::Control(ref ctrl) => match ctrl {
                ControlMessage::RecordingStart(ref rs) => rs.timestamp,
                ControlMessage::RecordingStop(ref ts) => *ts,
                // Commented because Control messages are not yet defined.
                // _ => Timestamp::new(NTP64(u64::MAX), Uuid::nil().into()),
            },
            Self::Data(data) => data.timestamp,
        }
    }
}

// Manual Ord implementation for message ordering when replay
impl Ord for Message {
    fn cmp(&self, other: &Self) -> Ordering {
        self.get_timestamp().cmp(&other.get_timestamp())
    }
}

impl PartialOrd for Message {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Message {
    fn eq(&self, other: &Self) -> bool {
        self.get_timestamp() == other.get_timestamp()
    }
}

impl Eq for Message {}

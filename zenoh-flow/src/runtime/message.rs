//
// Copyright (c) 2022 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

extern crate serde;

use crate::{Data, FlowId, NodeId, NodeOutput, PortId, ZFData, ZFError, ZFResult};
use async_std::sync::Arc;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, fmt::Debug};
use uhlc::Timestamp;
use uuid::Uuid;

/// Zenoh Flow data messages
/// It contains the actual data, the timestamp associated,
/// the end to end deadline, the end to end deadline misses and loop contexts.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataMessage {
    pub(crate) data: Data,
    pub(crate) timestamp: Timestamp,
}

impl DataMessage {
    /// Creates a new [`DataMessage`](`DataMessage`) with given `Data`,
    ///  `Timestamp` and `Vec<E2EDeadline>`.
    pub fn new(data: Data, timestamp: Timestamp) -> Self {
        Self { data, timestamp }
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

    /// Returns a reference to the data `Timestamp`.
    pub fn get_timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    /// Creates a new message from serialized data.
    /// This is used when the message is coming from Zenoh or from a non-rust
    /// node.
    pub fn new_serialized(data: Arc<Vec<u8>>, timestamp: Timestamp) -> Self {
        Self {
            data: Data::Bytes(data),
            timestamp,
        }
    }

    /// Creates a messages from `Typed` data.
    /// This is used when the data is generated from rust nodes.
    pub fn new_deserialized(data: Arc<dyn ZFData>, timestamp: Timestamp) -> Self {
        Self {
            data: Data::Typed(data),
            timestamp,
        }
    }
}

/// Metadata stored in Zenoh's time series storages.
/// It contains information about the recording.
/// Multiple [`RecordingMetadata`](`RecordingMetadata`) can be used
/// to synchronize the recording from different Ports.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordingMetadata {
    pub(crate) timestamp: Timestamp,
    pub(crate) port_id: PortId,
    pub(crate) node_id: NodeId,
    pub(crate) flow_id: FlowId,
    pub(crate) instance_id: Uuid,
}

/// Zenoh Flow control messages.
/// It contains the control messages used within Zenoh Flow.
/// For the time being only the `RecordingStart` and `RecordingStop` messages
/// have been defined,
/// *Note*: Most of messages are not yet defined.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ControlMessage {
    // These messages are not yet defined, those are some ideas
    // ReadyToMigrate,
    // ChangeMode(u8, u128),
    RecordingStart(RecordingMetadata),
    RecordingStop(Timestamp),
}

/// The Zenoh Flow message that is sent across `Link` and across Zenoh.
/// It contains either a [`DataMessage`](`DataMessage`) or
/// a [`ControlMessage`](`ControlMessage`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Message {
    Data(DataMessage),
    Control(ControlMessage),
    Watermark(Timestamp),
}

impl Message {
    /// Creates a `Message` from a [`NodeOutput`](`NodeOutput`)
    ///
    pub fn from_node_output(output: NodeOutput, timestamp: Timestamp) -> Self {
        match output {
            NodeOutput::Control(c) => Self::Control(c),
            NodeOutput::Data(d) => Self::from_serdedata(d, timestamp),
        }
    }

    /// Creates a `Message::Data` from [`Data`](`Data`).
    pub fn from_serdedata(output: Data, timestamp: Timestamp) -> Self {
        match output {
            Data::Typed(data) => Self::Data(DataMessage::new_deserialized(data, timestamp)),
            Data::Bytes(data) => Self::Data(DataMessage::new_serialized(data, timestamp)),
        }
    }

    /// Serializes the `Message` using bincode.
    ///
    /// # Errors
    /// An error variant is returned in case of:
    /// - fails to serialize
    pub fn serialize_bincode(&self) -> ZFResult<Vec<u8>> {
        match &self {
            Message::Control(_) | Message::Watermark(_) => {
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
                    ));

                    bincode::serialize(&serialized_message).map_err(|_| ZFError::SerializationError)
                }
            },
        }
    }

    /// Returns the `Timestamp` associated with the message.
    pub fn get_timestamp(&self) -> Timestamp {
        match self {
            Self::Control(ref ctrl) => match ctrl {
                ControlMessage::RecordingStart(ref rs) => rs.timestamp,
                ControlMessage::RecordingStop(ref ts) => *ts,
                // Commented because Control messages are not yet defined.
                // _ => Timestamp::new(NTP64(u64::MAX), Uuid::nil().into()),
            },
            Self::Data(data) => data.timestamp,
            Self::Watermark(ref ts) => *ts,
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

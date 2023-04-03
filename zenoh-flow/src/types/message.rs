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

use crate::traits::ZFData;
use crate::types::{FlowId, NodeId, PortId};
use crate::zfresult::ErrorKind;
use crate::Result;
use crate::{bail, zferror};

use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::{cmp::Ordering, fmt::Debug};
use uhlc::Timestamp;
use uuid::Uuid;

/// A `Payload` is Zenoh-Flow's lowest message container.
///
/// It either contains serialized data (if received from the network, or from nodes not written in
/// Rust), or `Typed` data as [`ZFData`](`ZFData`).
///
/// The `Typed` data is never serialized directly when sending over Zenoh or to an operator not
/// written in Rust.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Payload {
    /// Serialized data, coming either from Zenoh of from non-rust node.
    Bytes(Arc<Vec<u8>>),
    #[serde(skip_serializing, skip_deserializing)]
    /// Actual data as instance of 'ZFData` coming from a Rust node.
    ///
    /// This is never serialized directly.
    Typed(Arc<dyn ZFData>),
}

impl Payload {
    /// Tries to return a serialized representation of the data.
    ///
    /// It does not actually change the internal representation. The serialized representation in
    /// stored inside an `Arc` to avoid copies.
    ///
    /// # Errors
    ///
    /// If it fails to serialize an error variant will be returned.
    pub fn try_as_bytes(&self) -> Result<Arc<Vec<u8>>> {
        match &self {
            Self::Bytes(bytes) => Ok(bytes.clone()),
            Self::Typed(typed) => {
                let serialized_data = typed
                    .try_serialize()
                    .map_err(|e| zferror!(ErrorKind::SerializationError, "{:?}", e))?;
                Ok(Arc::new(serialized_data))
            }
        }
    }

    /// Tries to cast the data to the given type.
    ///
    /// If the data is represented as serialized, this will try to deserialize the bytes and change
    /// the internal representation of the data.
    ///
    /// If the data is already represented with as `Typed` then it will return an *immutable*
    /// reference to the internal data.
    ///
    /// This reference is *immutable* because one Output can send data to multiple Inputs, therefore
    /// to avoid copies the same `Arc` is sent to multiple operators, thus it is multiple-owned and
    /// the data inside cannot be modified.
    ///
    /// # Errors
    ///
    /// If fails to cast an error variant will be returned.
    pub fn try_get<Typed>(&mut self) -> Result<&Typed>
    where
        Typed: ZFData + 'static,
    {
        *self = (match &self {
            Self::Bytes(bytes) => {
                let data: Arc<dyn ZFData> = Arc::new(
                    Typed::try_deserialize(bytes.as_slice())
                        .map_err(|e| zferror!(ErrorKind::DeserializationError, "{:?}", e))?,
                );
                Ok(Self::Typed(data.clone()))
            }
            Self::Typed(typed) => Ok(Self::Typed(typed.clone())),
        } as Result<Self>)?;

        match self {
            Self::Typed(typed) => Ok(typed
                .as_any()
                .downcast_ref::<Typed>()
                .ok_or_else(|| zferror!(ErrorKind::InvalidData, "Could not downcast"))?),
            _ => Err(zferror!(ErrorKind::InvalidData, "Should be deserialized first").into()),
        }
    }
}

/// Creates a Data from an `Arc` of typed data.
/// The typed data has to be an instance of `ZFData`.
impl<UT> From<Arc<UT>> for Payload
where
    UT: ZFData + 'static,
{
    fn from(data: Arc<UT>) -> Self {
        Self::Typed(data)
    }
}

/// Creates a Data from  typed data.
/// The typed data has to be an instance of `ZFData`.
/// The data is then stored inside an `Arc` to avoid copies.
impl<UT> From<UT> for Payload
where
    UT: ZFData + 'static,
{
    fn from(data: UT) -> Self {
        Self::Typed(Arc::new(data))
    }
}

/// Creates a new `Data` from a `Arc<Vec<u8>>`.
impl From<Arc<Vec<u8>>> for Payload {
    fn from(bytes: Arc<Vec<u8>>) -> Self {
        Self::Bytes(bytes)
    }
}

/// Creates a new `Data` from a `Vec<u8>`,
/// In order to avoid copies it puts the data inside an `Arc`.
impl From<Vec<u8>> for Payload {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Bytes(Arc::new(bytes))
    }
}

/// Creates a new `Data` from a `&[u8]`.
impl From<&[u8]> for Payload {
    fn from(bytes: &[u8]) -> Self {
        Self::Bytes(Arc::new(bytes.to_vec()))
    }
}

impl<T: ZFData + 'static> From<Data<T>> for Payload {
    fn from(data: Data<T>) -> Self {
        data.payload
    }
}

impl From<DataMessage> for Payload {
    fn from(data_message: DataMessage) -> Self {
        data_message.data
    }
}

/// Zenoh-Flow data message.
///
/// It contains the actual data, the timestamp associated, the end to end deadline, the end to end
/// deadline misses and loop contexts.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataMessage {
    pub(crate) data: Payload,
    pub(crate) timestamp: Timestamp,
}

impl Deref for DataMessage {
    type Target = Payload;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl DerefMut for DataMessage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl DataMessage {
    /// Creates a new [`DataMessage`](`DataMessage`) with given `Data`,
    ///  `Timestamp` and `Vec<E2EDeadline>`.
    pub fn new(data: Payload, timestamp: Timestamp) -> Self {
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
    pub fn get_inner_data(&mut self) -> &mut Payload {
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
            data: Payload::Bytes(data),
            timestamp,
        }
    }

    /// Creates a messages from `Typed` data.
    /// This is used when the data is generated from rust nodes.
    pub fn new_deserialized(data: Arc<dyn ZFData>, timestamp: Timestamp) -> Self {
        Self {
            data: Payload::Typed(data),
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

/// The Zenoh-Flow message that is sent across `Link` and across Zenoh.
///
/// It contains either a [`DataMessage`](`DataMessage`) or a [`Timestamp`](`uhlc::Timestamp`),
/// in such case the `LinkMessage` variant is `Watermark`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LinkMessage {
    Data(DataMessage),
    Watermark(Timestamp),
}

impl LinkMessage {
    /// Creates a `LinkMessage::Data` from a [`Payload`](`Payload`).
    pub fn from_serdedata(output: Payload, timestamp: Timestamp) -> Self {
        match output {
            Payload::Typed(data) => Self::Data(DataMessage::new_deserialized(data, timestamp)),
            Payload::Bytes(data) => Self::Data(DataMessage::new_serialized(data, timestamp)),
        }
    }

    /// Serializes the `Message` using bincode.
    ///
    /// # Errors
    ///
    /// An error variant is returned in case of:
    /// - fails to serialize
    pub fn serialize_bincode(&self) -> Result<Vec<u8>> {
        match &self {
            LinkMessage::Data(data_message) => match &data_message.data {
                Payload::Bytes(_) => bincode::serialize(&self)
                    .map_err(|e| zferror!(ErrorKind::SerializationError, e).into()),
                Payload::Typed(_) => {
                    let serialized_data = data_message.data.try_as_bytes()?;
                    let serialized_message = LinkMessage::Data(DataMessage::new_serialized(
                        serialized_data,
                        data_message.timestamp,
                    ));

                    bincode::serialize(&serialized_message)
                        .map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
                }
            },
            _ => bincode::serialize(&self)
                .map_err(|e| zferror!(ErrorKind::SerializationError, e).into()),
        }
    }

    /// Serializes the `Message` using bincode into the given slice.
    ///
    /// # Errors
    ///
    /// An error variant is returned in case of:
    /// - fails to serialize
    /// - there is not enough space in the slice
    pub fn serialize_bincode_into(&self, buff: &mut [u8]) -> Result<()> {
        match &self {
            LinkMessage::Data(data_message) => match &data_message.data {
                Payload::Bytes(_) => bincode::serialize_into(buff, &self)
                    .map_err(|e| zferror!(ErrorKind::SerializationError, e).into()),
                Payload::Typed(_) => {
                    let serialized_data = data_message.data.try_as_bytes()?;
                    let serialized_message = LinkMessage::Data(DataMessage::new_serialized(
                        serialized_data,
                        data_message.timestamp,
                    ));
                    bincode::serialize_into(buff, &serialized_message)
                        .map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
                }
            },
            _ => bincode::serialize_into(buff, &self)
                .map_err(|e| zferror!(ErrorKind::SerializationError, e).into()),
        }
    }

    /// Returns the `Timestamp` associated with the message.
    pub fn get_timestamp(&self) -> Timestamp {
        match self {
            Self::Data(data) => data.timestamp,
            Self::Watermark(ref ts) => *ts,
            // Self::Control(ref ctrl) => match ctrl {
            //     ControlMessage::RecordingStart(ref rs) => rs.timestamp,
            //     ControlMessage::RecordingStop(ref ts) => *ts,
            // },
            // _ => Err(ErrorKind::Unsupported),
        }
    }
}

// Manual Ord implementation for message ordering when replay
impl Ord for LinkMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        self.get_timestamp().cmp(&other.get_timestamp())
    }
}

impl PartialOrd for LinkMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for LinkMessage {
    fn eq(&self, other: &Self) -> bool {
        self.get_timestamp() == other.get_timestamp()
    }
}

impl Eq for LinkMessage {}

/// A `Message<T>` is what is received on an `Input<T>`, typically after a call to `try_recv` or
/// `recv`.
///
/// A `Message<T>` can either contain [`Data<T>`](`Data`), or signal a _Watermark_.
pub enum Message<T: ZFData> {
    Data(Data<T>),
    Watermark,
}

/// A `Data<T>` is a convenience wrapper around `T`.
///
/// Upon reception, it transparently deserializes to `T` when the message is received serialized. It
/// downcasts it to a `&T` when the data is passed "typed" through a channel.
///
/// ## Performance
///
/// When deserializing, an allocation is performed.
#[derive(Debug)]
pub struct Data<T> {
    payload: Payload,
    bytes: Option<Vec<u8>>,
    // CAVEAT: Setting the value **typed** to `None` when it was already set to `Some` can cause a
    // panic! The visibility of the entire structure is private to force us not to make such
    // mistake.
    // In short, tread carefully!
    typed: Option<T>,
}

// CAVEAT: The implementation of `Deref` is what allows users to transparently manipulate the type
// `T`. However, for things to go well, the `typed` field MUST BE MANIPULATED WITH CAUTION.
impl<T: ZFData + 'static> Deref for Data<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        if let Some(ref typed) = self.typed {
            typed
        } else if let Payload::Typed(ref typed) = self.payload {
            typed.as_any().downcast_ref::<T>().expect(
                r#"You probably managed to find a very nasty flaw in Zenoh-Flow’s code as we
believed this situation would never happen (unless explicitely triggered — "explicitely" being an
understatement here, we feel it’s more like you really, really, wanted to see that message — in
which case, congratulations!).

Our guess as to what happened is that:
- the data in `Payload::Typed` was, at first, correct (where we internally do the
  `as_any().is::<T>()` check),
- in between this check and the call to `deref` the underlying data somehow changed.

If we did not do a mistake — fortunately the most likely scenario — then we do not know what
happened and we would be eager to investigate.

Feel free to contact us at < zenoh@zettascale.tech >.
"#,
            )
        } else {
            panic!(
                r#"You probably managed to find a very nasty flaw in Zenoh-Flow's code as we
believed this situation would never happen (unless explicitely triggered — "explicitely" being an
understatement here, we feel it's more like you really, really, wanted to see that message — in
which case, congratulations!).

Our guess as to what happened is that:
- the `data` field is a `Payload::Bytes`,
- the `typed` field is set to `None`.

If we did not do a mistake — fortunately the most likely scenario — then we do not know what
happened and we would be eager to investigate.

Feel free to contact us at < zenoh@zettascale.tech >.
"#
            )
        }
    }
}

impl<T: ZFData + 'static> Data<T> {
    /// Try to create a new [`Data<T>`](`Data`) based on a [`Payload`](`Payload`).
    ///
    /// Depending on the variant of [`Payload`](`Payload`) different steps are performed:
    /// - if `Payload::Bytes` then Zenoh-Flow tries to deserialize to an instance of `T` (performing
    ///   an allocation),
    /// - if `Payload::Typed` then Zenoh-Flow checks that the underlying type matches `T` (relying
    ///   on [`Any`](`Any`)).
    ///
    /// ## Errors
    ///
    /// An error will be returned if the Payload does not match `T`, i.e. if the deserialization or
    /// the downcast failed.
    pub(crate) fn try_new(payload: Payload) -> Result<Self> {
        let mut typed = None;

        match payload {
            Payload::Bytes(ref bytes) => typed = Some(T::try_deserialize(bytes)?),
            Payload::Typed(ref typed) => {
                if !(*typed).as_any().is::<T>() {
                    bail!(
                        ErrorKind::DeserializationError,
                        "Could not downcast payload"
                    )
                }
            }
        }

        Ok(Self {
            payload,
            bytes: None,
            typed,
        })
    }

    /// Try to obtain the bytes representation of `T`.
    ///
    /// Depending on how the [`Payload`](`Payload`) was received, this method either tries to
    /// serialize it or simply returns a slice view.
    ///
    /// ## Errors
    ///
    /// This method can return an error if the serialization failed.
    pub fn try_as_bytes(&mut self) -> Result<&[u8]> {
        match &self.payload {
            Payload::Bytes(bytes) => Ok(bytes.as_slice()),
            Payload::Typed(typed) => {
                if self.bytes.is_none() {
                    self.bytes = Some(typed.try_serialize()?);
                }
                Ok(self
                    .bytes
                    .as_ref()
                    .expect("This cannot fail as we serialized above"))
            }
        }
    }
}

// Implementing `From<T>` allows us to accept instances of `T` in the signature of `send` and
// `try_send` methods as `T` will implement `impl Into<Data<T>>`.
impl<T: ZFData + 'static> From<T> for Data<T> {
    fn from(data: T) -> Self {
        let payload = Payload::Typed(Arc::new(data));
        Self {
            payload,
            bytes: None,
            typed: None,
        }
    }
}

impl<T: ZFData + 'static> From<DataMessage> for Data<T> {
    fn from(value: DataMessage) -> Self {
        Self {
            payload: value.data,
            bytes: None,
            typed: None,
        }
    }
}

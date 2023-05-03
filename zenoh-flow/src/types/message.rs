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

use crate::bail;
use crate::prelude::ErrorKind;
use crate::traits::SendSyncAny;
use crate::types::{FlowId, NodeId, PortId};
use crate::{zferror, Result};

use async_std::sync::Arc;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::{cmp::Ordering, fmt::Debug};
use uhlc::Timestamp;
use uuid::Uuid;

/// `SerializerFn` is a type-erased version of the serializer function provided by node developer.
///
/// It is passed to downstream nodes (residing on the same process) in case they need to serialize
/// the data they receive typed.
/// Passing around the function allows us to serialize only when needed and without requiring prior
/// knowledge.
pub(crate) type SerializerFn =
    dyn Fn(&mut Vec<u8>, Arc<dyn SendSyncAny>) -> Result<()> + Send + Sync;

/// This function is what Zenoh-Flow will use to deserialize the data received on the `Input`.
///
/// It will be called for instance when data is received serialized (i.e. from an upstream node that
/// is either not implemented in Rust or on a different process) before it is given to the user's
/// code.
pub(crate) type DeserializerFn<T> = dyn Fn(&[u8]) -> anyhow::Result<T> + Send + Sync;

/// A `Payload` is Zenoh-Flow's lowest message container.
///
/// It either contains serialized data, i.e. `Bytes` (if received from the network, or from nodes
/// not written in Rust), or `Typed` data as a tuple `(`[Any](`std::any::Any`)`, SerializerFn)`.
#[derive(Clone, Serialize, Deserialize)]
pub enum Payload {
    /// Serialized data, coming either from Zenoh of from non-Rust node.
    Bytes(Arc<Vec<u8>>),
    #[serde(skip_serializing, skip_deserializing)]
    /// Data coming from another Rust node located on the same process that can either be downcasted
    /// (provided that its actual type is known) or serialized.
    Typed((Arc<dyn SendSyncAny>, Arc<SerializerFn>)),
}

impl Debug for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Payload::Bytes(_) => write!(f, "Payload::Bytes"),
            Payload::Typed(_) => write!(f, "Payload::Typed"),
        }
    }
}

impl Payload {
    pub fn from_data<T: Send + Sync + 'static>(
        data: Data<T>,
        serializer: Arc<SerializerFn>,
    ) -> Self {
        match data.inner {
            DataInner::Payload { payload, data: _ } => payload,
            DataInner::Data(data) => {
                Self::Typed((Arc::new(data) as Arc<dyn SendSyncAny>, serializer))
            }
        }
    }

    /// Populate `buffer` with the bytes representation of the [Payload].
    ///
    /// # Performance
    ///
    /// This method will serialize the [Payload] if it is `Typed`. Otherwise, the bytes
    /// representation is simply cloned.
    ///
    /// The provided `buffer` is reused and cleared between calls, so once its capacity stabilizes
    /// no more allocation is performed.
    pub(crate) fn try_as_bytes_into(&self, buffer: &mut Vec<u8>) -> Result<()> {
        buffer.clear(); // remove previous data but keep the allocated capacity

        match self {
            Payload::Bytes(bytes) => {
                (**bytes).clone_into(buffer);
                Ok(())
            }
            Payload::Typed((typed_data, serializer)) => {
                (serializer)(buffer, Arc::clone(typed_data))
            }
        }
    }

    /// Return an [Arc] containing the bytes representation of the [Payload].
    ///
    /// # Performance
    ///
    /// This method will only serialize (and thus allocate) the [Payload] if it is typed. Otherwise
    /// the [Arc] is cloned.
    //
    // NOTE: This method is used by, at least, our Python API.
    pub fn try_as_bytes(&self) -> Result<Arc<Vec<u8>>> {
        match self {
            Payload::Bytes(bytes) => Ok(bytes.clone()),
            Payload::Typed((typed_data, serializer)) => {
                let mut buffer = Vec::default();
                (serializer)(&mut buffer, Arc::clone(typed_data))?;
                Ok(Arc::new(buffer))
            }
        }
    }
}

/// Creates a new `Data` from a `Vec<u8>`.
///
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

impl DataMessage {
    /// Creates a new message from serialized data.
    ///
    /// This is used when the message is coming from Zenoh or from a non-rust node.
    pub fn new_serialized(data: Vec<u8>, timestamp: Timestamp) -> Self {
        Self {
            data: Payload::Bytes(Arc::new(data)),
            timestamp,
        }
    }

    /// Return the [Timestamp] associated with this [DataMessage].
    //
    // NOTE: This method is used by, at least, our Python API.
    pub fn get_timestamp(&self) -> &Timestamp {
        &self.timestamp
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
    pub fn from_payload(output: Payload, timestamp: Timestamp) -> Self {
        Self::Data(DataMessage {
            data: output,
            timestamp,
        })
    }

    /// Serializes the [LinkMessage] using [bincode] into the given `buffer`.
    ///
    /// The `inner_buffer` is used to serialize (if need be) the [Payload] contained inside the
    /// [LinkMessage].
    ///
    /// # Performance
    ///
    /// The provided `buffer` and `inner_buffer` are reused and cleared between calls, so once their
    /// capacity stabilizes no (re)allocation is performed.
    ///
    /// # Errors
    ///
    /// An error variant is returned in case of:
    /// - fails to serialize
    pub fn serialize_bincode_into(
        &self,
        message_buffer: &mut Vec<u8>,
        payload_buffer: &mut Vec<u8>,
    ) -> Result<()> {
        payload_buffer.clear(); // empty the buffers but keep their allocated capacity
        message_buffer.clear();

        match &self {
            LinkMessage::Data(data_message) => match &data_message.data {
                Payload::Bytes(_) => bincode::serialize_into(message_buffer, &self)
                    .map_err(|e| zferror!(ErrorKind::SerializationError, e).into()),
                Payload::Typed((data, serializer)) => {
                    (serializer)(payload_buffer, Arc::clone(data))?;
                    let serialized_message = LinkMessage::Data(DataMessage {
                        data: Payload::Bytes(Arc::new(payload_buffer.clone())),
                        timestamp: data_message.timestamp,
                    });

                    bincode::serialize_into(message_buffer, &serialized_message)
                        .map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
                }
            },
            _ => bincode::serialize_into(message_buffer, &self)
                .map_err(|e| zferror!(ErrorKind::SerializationError, e).into()),
        }
    }

    /// Serializes the [LinkMessage] using [bincode] into the given `shm_buffer` shared memory
    /// buffer.
    ///
    /// The `inner_buffer` is used to serialize (if need be) the [Payload] contained inside the
    /// [LinkMessage].
    ///
    /// # Performance
    ///
    /// The provided `inner_buffer` is reused and cleared between calls, so once its capacity
    /// stabilizes no (re)allocation is performed.
    ///
    /// # Errors
    ///
    /// An error variant is returned in case of:
    /// - fails to serialize
    /// - there is not enough space in the slice
    pub fn serialize_bincode_into_shm(
        &self,
        shm_buffer: &mut [u8],
        payload_buffer: &mut Vec<u8>,
    ) -> Result<()> {
        payload_buffer.clear(); // empty the buffer but keep the allocated capacity

        match &self {
            LinkMessage::Data(data_message) => match &data_message.data {
                Payload::Bytes(_) => bincode::serialize_into(shm_buffer, &self)
                    .map_err(|e| zferror!(ErrorKind::SerializationError, e).into()),
                Payload::Typed(_) => {
                    data_message.try_as_bytes_into(payload_buffer)?;
                    let serialized_message = LinkMessage::Data(DataMessage::new_serialized(
                        payload_buffer.clone(),
                        data_message.timestamp,
                    ));
                    bincode::serialize_into(shm_buffer, &serialized_message)
                        .map_err(|e| zferror!(ErrorKind::SerializationError, e).into())
                }
            },
            _ => bincode::serialize_into(shm_buffer, &self)
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
#[derive(Debug)]
pub enum Message<T> {
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
    inner: DataInner<T>,
}

/// The `DataInner` enum represents the two ways to send data in an [`Output<T>`](`Output`).
///
/// The `Payload` variant corresponds to a previously generated `Data<T>` being sent.
/// The `Data` variant corresponds to a new instance of `T` being sent.
pub(crate) enum DataInner<T> {
    Payload { payload: Payload, data: Option<T> },
    Data(T),
}

impl<T> Debug for DataInner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataInner::Payload { payload, data } => {
                let data = if data.is_some() { "Some" } else { "None" };
                write!(f, "DataInner::Payload: {:?} - data: {}", payload, data)
            }
            DataInner::Data(_) => write!(f, "DataInner::Data(T)"),
        }
    }
}

// Implementing `From<T>` allows us to accept instances of `T` in the signature of `send` and
// `try_send` methods as `T` will implement `impl Into<Data<T>>`.
impl<T: Send + Sync + 'static> From<T> for Data<T> {
    fn from(value: T) -> Self {
        Self {
            inner: DataInner::Data(value),
        }
    }
}

// The implementation of `Deref` is what allows users to transparently manipulate the type `T`.
//
// ## SAFETY
//
// Despite the presence of `expect` and `panic!`, we should never end up in these situations in
// normal circumstances.
//
// Let us reason here as to why this is "safe".
//
// The call to `expect` happens when the inner data is a [`Typed`](`Payload::Typed`) payload and the
// downcasts to `T` fails. This should not happen because of the way a [`Data`](`Data`) is created:
// upon creation we first perform a check that the provided typed payload can actually be downcasted
// to `T` — see the method `Data::try_from_payload`.
//
// The call to `panic!` happens when the inner data is a [`Bytes`](`Payload::Bytes`) payload and the
// `data` field is `None`. Again, this should not happen because of the way a [`Data`](`Data`) is
// created: upon creation, if the data is received as bytes, we first deserialize it and set the
// `data` field to `Some(T)` — see the method `Data::try_from_payload`.
impl<T: 'static> Deref for Data<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match &self.inner {
            DataInner::Payload { payload, data } => {
                if let Some(data) = data {
                    data
                } else if let Payload::Typed((typed, _)) = payload {
                    (**typed).as_any().downcast_ref::<T>().expect(
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
            DataInner::Data(data) => data,
        }
    }
}

impl<T: 'static> Data<T> {
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
    pub(crate) fn try_from_payload(
        payload: Payload,
        deserializer: Arc<DeserializerFn<T>>,
    ) -> Result<Self> {
        let mut typed = None;

        match payload {
            Payload::Bytes(ref bytes) => typed = Some((deserializer)(bytes.as_slice())?),
            Payload::Typed((ref typed, _)) => {
                if !(**typed).as_any().is::<T>() {
                    bail!(
                        ErrorKind::DeserializationError,
                        "Failed to downcast provided value",
                    )
                }
            }
        }

        Ok(Self {
            inner: DataInner::Payload {
                payload,
                data: typed,
            },
        })
    }
}

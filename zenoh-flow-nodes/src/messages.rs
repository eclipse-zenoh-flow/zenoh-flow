//
// Copyright © 2021 ZettaScale Technology <contact@zettascale.tech>
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

use std::{cmp::Ordering, fmt::Debug, ops::Deref, sync::Arc};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use uhlc::Timestamp;
use zenoh_flow_commons::Result;

use crate::traits::SendSyncAny;

/// `SerializerFn` is a type-erased version of the serialiser function provided by node developer.
///
/// It is passed to downstream nodes (residing on the same process) in case they need to serialise
/// the data they receive typed.
/// Passing around the function allows us to serialise only when needed and without requiring prior
/// knowledge.
pub(crate) type SerializerFn =
    dyn Fn(&mut Vec<u8>, Arc<dyn SendSyncAny>) -> Result<()> + Send + Sync;

/// This function is what Zenoh-Flow will use to deserialise the data received on the `Input`.
///
/// It will be called for instance when data is received serialised (i.e. from an upstream node that
/// is either not implemented in Rust or on a different process) before it is given to the user's
/// code.
pub(crate) type DeserializerFn<T> = dyn Fn(&[u8]) -> Result<T> + Send + Sync;

/// A `Payload` is Zenoh-Flow's lowest message container.
///
/// It either contains serialised data, i.e. `Bytes` (if received from the network, or from nodes
/// not written in Rust), or `Typed` data as a tuple `(`[Any](`std::any::Any`)`, SerializerFn)`.
#[derive(Clone, Serialize, Deserialize)]
pub enum Payload {
    /// Serialised data, coming either from Zenoh of from non-Rust node.
    Bytes(Arc<Vec<u8>>),
    #[serde(skip_serializing, skip_deserializing)]
    /// Data coming from another Rust node located on the same Zenoh-Flow runtime that can either be downcast or
    /// serialised.
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
    /// This method will serialise the [Payload] if it is `Typed`. Otherwise, the bytes
    /// representation is simply cloned.
    ///
    /// The provided `buffer` is reused and cleared between calls, so once its capacity stabilises
    /// no more allocation is performed.
    pub fn try_as_bytes_into(&self, buffer: &mut Vec<u8>) -> Result<()> {
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
    /// This method will only serialise (and thus allocate) the [Payload] if it is typed. Otherwise
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

impl From<LinkMessage> for Payload {
    fn from(data_message: LinkMessage) -> Self {
        data_message.payload
    }
}

/// A message send on a Zenoh-Flow link: a [Payload] and a [Timestamp].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LinkMessage {
    pub(crate) payload: Payload,
    pub(crate) timestamp: Timestamp,
}

impl Ord for LinkMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp().cmp(other.timestamp())
    }
}

impl PartialOrd for LinkMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for LinkMessage {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp() == other.timestamp()
    }
}

impl Eq for LinkMessage {}

impl LinkMessage {
    pub fn new(payload: Payload, timestamp: Timestamp) -> Self {
        Self { payload, timestamp }
    }

    /// Creates a new message from serialised data.
    ///
    /// This is used when the message is coming from Zenoh or from a non-rust node.
    pub fn new_serialized(data: Vec<u8>, timestamp: Timestamp) -> Self {
        Self {
            payload: Payload::Bytes(Arc::new(data)),
            timestamp,
        }
    }

    /// Return the [Payload] associated with this [LinkMessage].
    //
    // NOTE: Used by our Python API. 🐍
    pub fn payload(&self) -> &Payload {
        &self.payload
    }

    /// Return the [Timestamp] associated with this message.
    #[deprecated(
        since = "0.6.0-alpha.3",
        note = "This method will be deprecated in the next major version of Zenoh-Flow in favour of `timestamp()`."
    )]
    pub fn get_timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    /// Return the [Timestamp] associated with this message.
    //
    // NOTE: Used by our Python API. 🐍
    pub fn timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    /// Serialises the [LinkMessage] using [bincode] into the given `buffer`.
    ///
    /// The `inner_buffer` is used to serialise (if need be) the [Payload] contained inside the
    /// [LinkMessage].
    ///
    /// # Performance
    ///
    /// The provided `buffer` and `inner_buffer` are reused and cleared between calls, so once their
    /// capacity stabilises no (re)allocation is performed.
    ///
    /// # Errors
    ///
    /// An error variant is returned in case of:
    /// - fails to serialise
    pub fn serialize_bincode_into(
        &self,
        message_buffer: &mut Vec<u8>,
        payload_buffer: &mut Vec<u8>,
    ) -> Result<()> {
        payload_buffer.clear(); // empty the buffers but keep their allocated capacity
        message_buffer.clear();

        match &self.payload {
            Payload::Bytes(_) => bincode::serialize_into(message_buffer, &self)
                .context("Failed to serialise `Payload::Bytes``"),
            Payload::Typed((data, serializer)) => {
                (serializer)(payload_buffer, Arc::clone(data))?;
                let serialized_message = Self {
                    payload: Payload::Bytes(Arc::new(payload_buffer.clone())),
                    timestamp: self.timestamp,
                };

                bincode::serialize_into(message_buffer, &serialized_message)
                    .context("Failed to serialise `Payload::Typed`")
            }
        }
    }
}

/// A `Data<T>` is a wrapper around `T` given by a typed [`Input<T>`](crate::prelude::Input).
///
/// A `Data<T>` automatically dereferences to a `&T`.
///
/// # Performance
///
/// If the upstream node does not reside on the same Zenoh-Flow runtime, dereferencing to `&T` incurs some additional
/// operations: the received [Payload] will then necessarily be serialised and must first be deserialised.
///
/// To perform the deserialisation, the [deserialiser](crate::io::InputBuilder::typed()) function passed to the
/// [`Input<T>`](crate::prelude::Input) will be called.
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
// Despite the presence of `expect` and `panic!`, we should never end up in these situations in normal circumstances.
//
// Let us reason here as to why this is "safe".
//
// The call to `expect` happens when the inner data is a [Typed](Payload::Typed) payload and the downcast to `T`
// fails. This should not happen because of the way a [Data] is created: upon creation we first perform a check that the
// provided typed payload can actually be downcast to `T` — see the method `Data::try_from_payload`.
//
// The call to `panic!` happens when the inner data is a [Bytes](Payload::Bytes) payload and the `data` field is
// `None`. Again, this should not happen because of the way a [`Data`](`Data`) is created: upon creation, if the data is
// received as bytes, we first deserialise it and set the `data` field to `Some(T)` — see the method
// `Data::try_from_payload`.
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
believed this situation would never happen (unless explicitly triggered — "explicitly" being an
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
believed this situation would never happen (unless explicitly triggered — "explicitly" being an
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
    /// - if `Payload::Bytes` then Zenoh-Flow tries to deserialise to an instance of `T` (performing
    ///   an allocation),
    /// - if `Payload::Typed` then Zenoh-Flow checks that the underlying type matches `T` (relying
    ///   on [`Any`](`Any`)).
    ///
    /// ## Errors
    ///
    /// An error will be returned if the Payload does not match `T`, i.e. if the deserialisation or
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
                    bail!("Failed to downcast provided value")
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

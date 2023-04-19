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

use crate::prelude::{ErrorKind, Message, PortId};
use crate::types::{Data, DataMessage, DeserializerFn, LinkMessage};
use crate::{bail, Result};

use flume::TryRecvError;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use uhlc::Timestamp;

/// The `Inputs` structure contains all the inputs created for a [Sink](crate::prelude::Sink) or an
/// [Operator](crate::prelude::Operator).
///
/// Each input is indexed by its **port identifier**: the name that was indicated in the descriptor
/// of the node. These names are _case sensitive_ and should be an exact match to what was written
/// in the descriptor.
///
/// Zenoh-Flow provides two flavors of input: [InputRaw] and [`Input<T>`]. An [`Input<T>`]
/// conveniently exposes instances of `T` while an [InputRaw] exposes messages, allowing to
/// disregard the contained data.
///
/// The main way to interact with `Inputs` is through the `take` method.
///
/// # Example
///
/// ```ignore
/// let input_builder = inputs.take("test raw").expect("No input name 'test raw' found");
/// let input_raw = input_builder.build_raw();
///
/// let input_builder = inputs.take("test typed").expect("No input name 'test typed' found");
/// let input: Input<u64> = input_build.build_typed(
///                             |bytes| serde_json::from_slice(bytes)
///                                 .map_err(|e| anyhow::anyhow!(e))
///                         )?;
/// ```
pub struct Inputs {
    pub(crate) hmap: HashMap<PortId, Vec<flume::Receiver<LinkMessage>>>,
}

// Dereferencing on the internal `Hashmap` allows users to call all the methods implemented on it:
// `keys()` for one.
impl Deref for Inputs {
    type Target = HashMap<PortId, Vec<flume::Receiver<LinkMessage>>>;

    fn deref(&self) -> &Self::Target {
        &self.hmap
    }
}

impl Inputs {
    pub(crate) fn new() -> Self {
        Self {
            hmap: HashMap::default(),
        }
    }

    /// Insert the `flume::Receiver` in the [Inputs], creating the entry if needed in the internal
    /// `HashMap`.
    pub(crate) fn insert(&mut self, port_id: PortId, rx: flume::Receiver<LinkMessage>) {
        self.hmap
            .entry(port_id)
            .or_insert_with(Vec::default)
            .push(rx)
    }

    /// Returns an [InputBuilder] for the provided `port_id`, if an input was declared with this
    /// exact name in the descriptor of the node, otherwise returns `None`.
    ///
    /// # Usage
    ///
    /// This builder can either produce a, typed, [`Input<T>`] or an [InputRaw]. The main difference
    /// between both is the type of data they expose: an [`Input<T>`] automatically tries to downcast
    /// or deserialize the data contained in the message to expose `&T`, while an [InputRaw] simply
    /// exposes a [LinkMessage].
    ///
    /// As long as data need to be manipulated, a typed [`Input<T>`] should be favored.
    ///
    /// ## Typed
    ///
    /// To obtain an [`Input<T>`] one must call `build_typed` and provide a deserializer function. In
    /// the example below we rely on the `serde_json` crate to do the deserialization.
    ///
    /// ```ignore
    /// let input_typed: Input<u64> = inputs
    ///     .take("test")
    ///     .expect("No input named 'test' found")
    ///     .build_typed(
    ///         |bytes: &[u8]| serde_json::from_slice(bytes).map_err(|e| anyhow::anyhow!(e))
    ///     );
    /// ```
    ///
    /// ## Raw
    ///
    /// To obtain an [InputRaw] one must call `build_raw`.
    ///
    /// ```ignore
    /// let input_raw: InputRaw = inputs
    ///     .take("test")
    ///     .expect("No input named 'test' found")
    ///     .build_raw();
    /// ```
    pub fn take(&mut self, port_id: impl AsRef<str>) -> Option<InputBuilder> {
        self.hmap
            .remove(port_id.as_ref())
            .map(|receivers| InputBuilder {
                port_id: port_id.as_ref().into(),
                receivers,
            })
    }
}

/// An `InputBuilder` is the intermediate structure to obtain either an [`Input<T>`] or an
/// [InputRaw].
///
/// The main difference between both is the type of data they expose: an [`Input<T>`] automatically
/// tries to downcast or deserialize the data contained in the message to expose `&T`, while an
/// [InputRaw] simply exposes a [LinkMessage].
///
/// # Planned evolution
///
/// Zenoh-Flow will allow tweaking the behaviour of the underlying channels. For now, the
/// `receivers` channels are _unbounded_ and do not implement a dropping policy, which could lead to
/// issues.
pub struct InputBuilder {
    pub(crate) port_id: PortId,
    pub(crate) receivers: Vec<flume::Receiver<LinkMessage>>,
}

impl InputBuilder {
    /// Consume the `InputBuilder` to produce an [InputRaw].
    ///
    /// An [InputRaw] exposes the [LinkMessage] it receives, without trying to perform any
    /// conversion on the data.
    ///
    /// The [InputRaw] was designed for use cases such as load-balancing or rate-limiting. In these
    /// scenarios, the node does not need to access the underlying data.
    ///
    /// # `InputRaw` vs `Input<T>`
    ///
    /// If the node needs access to the data to perform computations, an [`Input<T>`] should be
    /// favored as it performs the conversion automatically.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let input_raw: InputRaw = inputs
    ///     .take("test")
    ///     .expect("No input named 'test' found")
    ///     .build_raw();
    /// ```
    pub fn build_raw(self) -> InputRaw {
        InputRaw {
            port_id: self.port_id,
            receivers: self.receivers,
        }
    }

    /// Consume the `InputBuilder` to produce an [`Input<T>`].
    ///
    /// An [`Input<T>`] tries to automatically convert the data contained in the [LinkMessage] in
    /// order to expose `&T`. Depending on if the data is received serialized or not, to perform
    /// this conversion either the `deserializer` is called or a downcast is attempted.
    ///
    /// # `Input<T>` vs `InputRaw`
    ///
    /// If the node does need to access the data contained in the [LinkMessage], an [InputRaw]
    /// should be favored as it does not try to perform the extra conversion steps.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let input_typed: Input<u64> = inputs
    ///     .take("test")
    ///     .expect("No input named 'test' found")
    ///     .build_typed(
    ///         |bytes: &[u8]| serde_json::from_slice(bytes).map_err(|e| anyhow::anyhow!(e))
    ///     );
    /// ```
    pub fn build_typed<T>(
        self,
        deserializer: impl Fn(&[u8]) -> anyhow::Result<T> + Send + Sync + 'static,
    ) -> Input<T> {
        Input {
            input_raw: self.build_raw(),
            deserializer: Arc::new(deserializer),
        }
    }
}

/// An [`InputRaw`](`InputRaw`) exposes the [`LinkMessage`](`LinkMessage`) it receives.
///
/// It's primary purpose is to ensure "optimal" performance. This can be useful to implement
/// behaviour where actual access to the underlying data is irrelevant.
#[derive(Clone, Debug)]
pub struct InputRaw {
    pub(crate) port_id: PortId,
    pub(crate) receivers: Vec<flume::Receiver<LinkMessage>>,
}

impl InputRaw {
    pub fn port_id(&self) -> &PortId {
        &self.port_id
    }

    /// Returns the number of channels associated with this Input.
    pub fn channels_count(&self) -> usize {
        self.receivers.len()
    }

    /// Returns the first [LinkMessage] that was received on any of the channels associated with
    /// this Input, or an `Empty` error if there were no messages.
    ///
    /// # Asynchronous alternative: `recv`
    ///
    /// This method is a synchronous fail-fast alternative to it's asynchronous counterpart: `recv`.
    /// Although synchronous, but given it is "fail-fast", this method will not block the thread on
    /// which it is executed.
    ///
    /// # Error
    ///
    /// If no message was received, an `Empty` error is returned. Note that if some channels are
    /// disconnected, for each of such channel an error is logged.
    pub fn try_recv(&self) -> Result<LinkMessage> {
        for receiver in &self.receivers {
            match receiver.try_recv() {
                Ok(message) => return Ok(message),
                Err(e) => {
                    if matches!(e, TryRecvError::Disconnected) {
                        log::error!("[Input: {}] A channel is disconnected", self.port_id);
                    }
                }
            }
        }

        // We went through all channels, no message, Empty error.
        bail!(ErrorKind::Empty, "[Input: {}] No message", self.port_id)
    }

    /// Returns the first [LinkMessage] that was received, *asynchronously*, on any of the channels
    /// associated with this Input.
    ///
    /// If several [LinkMessage] are received at the same time, one is *randomly* selected.
    ///
    /// # Error
    ///
    /// An error is returned if *all* channels are disconnected. For each disconnected channel, an
    /// error is separately logged.
    pub async fn recv(&self) -> Result<LinkMessage> {
        let mut recv_futures = self
            .receivers
            .iter()
            .map(|link| link.recv_async())
            .collect::<Vec<_>>();

        loop {
            let (res, _, remaining) = futures::future::select_all(recv_futures).await;
            match res {
                Ok(message) => return Ok(message),
                Err(_disconnected) => {
                    log::error!("[Input: {}] A channel is disconnected", self.port_id);
                    if remaining.is_empty() {
                        bail!(
                            ErrorKind::Disconnected,
                            "[Input: {}] All channels are disconnected",
                            self.port_id
                        );
                    }

                    recv_futures = remaining;
                }
            }
        }
    }
}

/// A typed `Input` that tries to automatically downcast or deserialize the data received in order
/// to expose `&T`.
///
/// # Performance
///
/// If the data is received serialized from the upstream node, an allocation is performed to host
/// the deserialized `T`.
pub struct Input<T> {
    pub(crate) input_raw: InputRaw,
    pub(crate) deserializer: Arc<DeserializerFn<T>>,
}

// Dereferencing to the [InputRaw] allows to directly call methods on it with a typed [Input].
impl<T: Send + Sync + 'static> Deref for Input<T> {
    type Target = InputRaw;

    fn deref(&self) -> &Self::Target {
        &self.input_raw
    }
}

impl<T: Send + Sync + 'static> Input<T> {
    /// Returns the first [`Message<T>`] that was received, *asynchronously*, on any of the channels
    /// associated with this Input.
    ///
    /// If several [`Message<T>`] are received at the same time, one is *randomly* selected.
    ///
    /// This method interprets the data to the type associated with this [`Input<T>`].
    ///
    /// # Performance
    ///
    /// As this method interprets the data received additional operations are performed:
    /// - data received serialized is deserialized (an allocation is performed to store an instance
    ///   of `T`),
    /// - data received "typed" are checked against the type associated to this [`Input<T>`].
    ///
    /// # Error
    ///
    /// Several errors can occur:
    /// - all the channels are disconnected,
    /// - Zenoh-Flow failed at interpreting the received data as an instance of `T`.
    pub async fn recv(&self) -> Result<(Message<T>, Timestamp)> {
        match self.input_raw.recv().await? {
            LinkMessage::Data(DataMessage { data, timestamp }) => Ok((
                Message::Data(Data::try_from_payload(data, self.deserializer.clone())?),
                timestamp,
            )),
            LinkMessage::Watermark(timestamp) => Ok((Message::Watermark, timestamp)),
        }
    }

    /// Returns the first [`Message<T>`] that was received on any of the channels associated with this
    /// Input, or `None` if all the channels are empty.
    ///
    /// # Asynchronous alternative: `recv`
    ///
    /// This method is a synchronous fail-fast alternative to it's asynchronous counterpart: `recv`.
    /// Although synchronous, this method will not block the thread on which it is executed.
    ///
    /// # Error
    ///
    /// Several errors can occur:
    /// - no message was received (i.e. Empty error),
    /// - Zenoh-Flow failed at interpreting the received data as an instance of `T`.
    ///
    /// Note that if some channels are disconnected, for each of such channel an error is logged.
    pub fn try_recv(&self) -> Result<(Message<T>, Timestamp)> {
        match self.input_raw.try_recv()? {
            LinkMessage::Data(DataMessage { data, timestamp }) => Ok((
                Message::Data(Data::try_from_payload(data, self.deserializer.clone())?),
                timestamp,
            )),
            LinkMessage::Watermark(ts) => Ok((Message::Watermark, ts)),
        }
    }
}

#[cfg(test)]
#[path = "./tests/input-tests.rs"]
mod tests;

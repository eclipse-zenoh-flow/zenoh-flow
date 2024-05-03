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

use std::{collections::HashMap, ops::Deref, sync::Arc};

use anyhow::{anyhow, bail};
use flume::TryRecvError;
use uhlc::Timestamp;
use zenoh_flow_commons::{PortId, Result};

use crate::messages::{Data, DeserializerFn, LinkMessage};

/// The `Inputs` structure contains all the inputs created for a [Sink](crate::prelude::Sink) or an
/// [Operator](crate::prelude::Operator).
///
/// Each input is indexed by its **port identifier**: the name that was indicated in the descriptor
/// of the node. These names are _case sensitive_ and should be an exact match to what was written
/// in the descriptor.
///
/// Zenoh-Flow provides two flavours of input: [InputRaw] and [`Input<T>`]. An [`Input<T>`]
/// conveniently exposes instances of `T` while an [InputRaw] exposes messages, allowing to
/// disregard the contained data.
///
/// The main way to interact with `Inputs` is through the `take` method.
///
/// # Example
///
/// ```no_run
/// # use zenoh_flow_nodes::prelude::*;
/// # let mut inputs = Inputs::default();
/// let input_raw = inputs
///     .take("test raw")
///     .expect("No input name 'test raw' found")
///     .raw();
///
/// let input: Input<u64> = inputs
///     .take("test typed")
///     .expect("No input name 'test typed' found")
///     .typed(|bytes| serde_json::from_slice(bytes).map_err(|e| anyhow!(e)));
/// ```
#[derive(Default)]
pub struct Inputs {
    pub(crate) hmap: HashMap<PortId, flume::Receiver<LinkMessage>>,
}

// Dereferencing on the internal `HashMap` allows users to call all the methods implemented on it: `keys()` for one.
impl Deref for Inputs {
    type Target = HashMap<PortId, flume::Receiver<LinkMessage>>;

    fn deref(&self) -> &Self::Target {
        &self.hmap
    }
}

impl Inputs {
    /// Insert the `flume::Receiver` in the [Inputs], creating the entry if needed in the internal `HashMap`.
    pub fn insert(&mut self, port_id: PortId, rx: flume::Receiver<LinkMessage>) {
        self.hmap.entry(port_id).or_insert(rx);
    }

    /// Returns an Input builder for the provided `port_id`, if an input was declared with this exact name in the
    /// descriptor of the node, otherwise returns `None`.
    ///
    /// # Usage
    ///
    /// This builder can either produce a, typed, [`Input<T>`](Input) or an [InputRaw]. The main difference between both
    /// is the type of data they expose: an [`Input<T>`](Input) automatically tries to downcast or deserialize the data
    /// contained in the message to expose `&T`, while an [InputRaw] simply exposes a [LinkMessage].
    ///
    /// ## Typed
    ///
    /// To obtain an [`Input<T>`](Input) one must call `typed` and provide a deserialiser function. In the example below
    /// we rely on the `serde_json` crate to do the deserialisation.
    ///
    /// ```no_run
    /// # use zenoh_flow_nodes::prelude::*;
    /// # let mut inputs = Inputs::default();
    /// let input: Input<u64> = inputs
    ///     .take("test typed")
    ///     .expect("No input name 'test typed' found")
    ///     .typed(|bytes| serde_json::from_slice(bytes).map_err(|e| anyhow!(e)));
    /// ```
    ///
    /// ## Raw
    ///
    /// To obtain an [InputRaw] one must call `raw`.
    ///
    /// ```no_run
    /// # use zenoh_flow_nodes::prelude::*;
    /// # let mut inputs = Inputs::default();
    /// let input_raw = inputs
    ///     .take("test raw")
    ///     .expect("No input name 'test raw' found")
    ///     .raw();
    /// ```
    pub fn take(&mut self, port_id: impl AsRef<str>) -> Option<InputBuilder> {
        self.hmap
            .remove(&port_id.as_ref().into())
            .map(|receiver| InputBuilder {
                port_id: port_id.as_ref().into(),
                receiver,
            })
    }
}

/// An `InputBuilder` is the intermediate structure to obtain either an [`Input<T>`](Input) or an [InputRaw].
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
    pub(crate) receiver: flume::Receiver<LinkMessage>,
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
    /// favoured as it performs the conversion automatically.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use zenoh_flow_nodes::prelude::*;
    /// # let mut inputs = Inputs::default();
    /// let input_raw = inputs
    ///     .take("test raw")
    ///     .expect("No input name 'test raw' found")
    ///     .raw();
    /// ```
    pub fn raw(self) -> InputRaw {
        InputRaw {
            port_id: self.port_id,
            receiver: self.receiver,
        }
    }

    /// Consume the `InputBuilder` to produce an [`Input<T>`].
    ///
    /// An [`Input<T>`] tries to automatically convert the data contained in the [LinkMessage] in
    /// order to expose `&T`. Depending on if the data is received serialised or not, to perform
    /// this conversion either the `deserialiser` is called or a downcast is attempted.
    ///
    /// # `Input<T>` vs `InputRaw`
    ///
    /// If the node does need to access the data contained in the [LinkMessage], an [InputRaw]
    /// should be favoured as it does not try to perform the extra conversion steps.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use zenoh_flow_nodes::prelude::*;
    /// # let mut inputs = Inputs::default();
    /// let input: Input<u64> = inputs
    ///     .take("test typed")
    ///     .expect("No input name 'test typed' found")
    ///     .typed(|bytes| serde_json::from_slice(bytes).map_err(|e| anyhow!(e)));
    /// ```
    pub fn typed<T>(
        self,
        deserializer: impl Fn(&[u8]) -> anyhow::Result<T> + Send + Sync + 'static,
    ) -> Input<T> {
        Input {
            input_raw: self.raw(),
            deserializer: Arc::new(deserializer),
        }
    }
}

/// An `InputRaw` receives "raw" [LinkMessage].
///
/// As opposed to a typed [`Input<T>`](Input), an `InputRaw` will not perform any operation on the data it receives.
/// This behaviour is useful when access to the underlying data is either irrelevant (e.g. for rate-limiting purposes)
/// or when Zenoh-Flow should not attempt to interpret the contained [Payload](crate::prelude::Payload) (e.g. for
/// bindings).
#[derive(Clone, Debug)]
pub struct InputRaw {
    pub(crate) port_id: PortId,
    pub(crate) receiver: flume::Receiver<LinkMessage>,
}

impl InputRaw {
    pub fn port_id(&self) -> &PortId {
        &self.port_id
    }

    /// Returns the number of channels associated with this Input.
    pub fn channels_count(&self) -> usize {
        self.receiver.len()
    }

    /// Returns the first queued [LinkMessage] or [None] if there is no queued message.
    ///
    /// # Asynchronous alternative: `recv`
    ///
    /// This method is a synchronous fail-fast alternative to it's asynchronous counterpart: `recv`.  Although
    /// synchronous, this method will not block the thread on which it is executed.
    ///
    /// # Errors
    ///
    /// An error is returned if the associated channel is disconnected.
    pub fn try_recv(&self) -> Result<Option<LinkMessage>> {
        match self.receiver.try_recv() {
            Ok(message) => Ok(Some(message)),
            Err(e) => match e {
                TryRecvError::Empty => Ok(None),
                TryRecvError::Disconnected => {
                    tracing::error!("Link disconnected: {}", self.port_id);
                    bail!("Disconnected");
                }
            },
        }
    }

    /// Returns the first [LinkMessage] that was received, *asynchronously*, on any of the channels associated with this
    /// Input.
    ///
    /// If several [LinkMessage] are received at the same time, one is *randomly* selected.
    ///
    /// # Errors
    ///
    /// An error is returned if a channel was disconnected.
    pub async fn recv(&self) -> Result<LinkMessage> {
        self.receiver.recv_async().await.map_err(|_| {
            tracing::error!("Link disconnected: {}", self.port_id);
            anyhow!("Disconnected")
        })
    }
}

/// A typed `Input` receiving [`Data<T>`](Data).
///
/// An `Input` will automatically try to downcast or deserialise the [Payload](crate::prelude::Payload) it receives,
/// exposing a [`Data<T>`](Data).
///
/// The type of conversion performed depends on whether the upstream node resides on the same Zenoh-Flow runtime
/// (downcast) or on another runtime (deserialisation).
///
/// # Performance
///
/// If the data is received serialised from the upstream node, an allocation is performed to host the deserialised `T`.
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
    /// Returns the first [`Data<T>`](Data) that was received, *asynchronously*, on any of the channels
    /// associated with this Input.
    ///
    /// If several [`Data<T>`](Data) are received at the same time, one is *randomly* selected.
    ///
    /// This method interprets the data to the type associated with this [`Input<T>`](Input).
    ///
    /// # Performance
    ///
    /// As this method interprets the data received, additional operations are performed:
    /// - data received serialised is deserialised (an allocation is performed to store an instance of `T`),
    /// - data received "typed" are checked against the type associated to this [`Input<T>`](Input).
    ///
    /// # Synchronous alternative: `try_recv`
    ///
    /// This method is an asynchronous alternative to it's synchronous fail-fast counterpart: `try_recv`.
    ///
    /// # Errors
    ///
    /// Several errors can occur:
    /// - a channel was disconnected,
    /// - Zenoh-Flow failed at interpreting the received data as an instance of `T`.
    pub async fn recv(&self) -> Result<(Data<T>, Timestamp)> {
        let LinkMessage { payload, timestamp } = self.input_raw.recv().await?;
        Ok((
            Data::try_from_payload(payload, self.deserializer.clone())?,
            timestamp,
        ))
    }

    /// Returns the first [`Data<T>`](Data) that was received on any of the channels associated with this Input,
    /// or [None] if all the channels are empty.
    ///
    /// # Performance
    ///
    /// As this method interprets the data received, additional operations are performed:
    /// - data received serialised is deserialised (an allocation is performed to store an instance of `T`),
    /// - data received "typed" are checked against the type associated to this [`Input<T>`](Input).
    ///
    /// # Asynchronous alternative: `recv`
    ///
    /// This method is a synchronous fail-fast alternative to it's asynchronous counterpart: `recv`.  Although
    /// synchronous, this method will not block the thread on which it is executed.
    ///
    /// # Errors
    ///
    /// Several errors can occur:
    /// - a channel was disconnected,
    /// - Zenoh-Flow failed at interpreting the received data as an instance of `T`.
    ///
    /// Note that if some channels are disconnected, for each of such channel an error is logged.
    pub fn try_recv(&self) -> Result<Option<(Data<T>, Timestamp)>> {
        if let Some(LinkMessage { payload, timestamp }) = self.input_raw.try_recv()? {
            return Ok(Some((
                Data::try_from_payload(payload, self.deserializer.clone())?,
                timestamp,
            )));
        }

        Ok(None)
    }
}

#[cfg(test)]
#[path = "./tests/input-tests.rs"]
mod tests;

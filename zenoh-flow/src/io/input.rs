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

use crate::prelude::{ErrorKind, Message, PortId, ZFData};
use crate::types::{Data, DataMessage, LinkMessage};
use crate::{bail, Result};
use flume::TryRecvError;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Deref;
use uhlc::Timestamp;

/// The [`Inputs`](`Inputs`) structure contains all the receiving channels we created for a
/// [`Sink`](`crate::prelude::Sink`) or an [`Operator`](`crate::prelude::Operator`).
///
/// To access these underlying channels, two methods are available:
/// - `take`: this will return an `Input<T>` where `T` implements [`ZFData`](`ZFData`),
/// - `take_raw`: this will return an [`InputRaw`](`InputRaw`) — a type agnostic receiver.
///
/// Choosing between `take` and `take_raw` is a trade-off between convenience and performance: an
/// `Input<T>` conveniently receives instances of `T` and is thus less performant as Zenoh-Flow has
/// to manipulate the data; an `InputRaw` is more performant but all the manipulation must be
/// performed in the code of the Node. An `InputRaw` is currently leveraged by the bindings in other
/// programming languages — as they cannot understand the types coming from Rust — but can also be
/// leveraged, for instance, for load-balancing or rate-limiting.
pub struct Inputs {
    pub(crate) hmap: HashMap<PortId, Vec<flume::Receiver<LinkMessage>>>,
}

// Dereferencing on the internal [`HashMap`](`std::collections::Hashmap`) allows users to call all the methods
// implemented on it: `keys()` for one.
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

    /// Insert the `flume::Receiver` in the [`Inputs`](`Inputs`), creating the entry if needed in
    /// the internal `HashMap`.
    pub(crate) fn insert(&mut self, port_id: PortId, rx: flume::Receiver<LinkMessage>) {
        self.hmap
            .entry(port_id)
            .or_insert_with(Vec::default)
            .push(rx)
    }

    /// Returns the typed [`Input<T>`](`Input`) associated to the provided `port_id`, if one is
    /// associated, otherwise `None` is returned.
    ///
    /// ## Performance
    ///
    /// With a typed [`Input<T>`](`Input`), Zenoh-Flow will perform operations on the underlying
    /// data: if the data are received serialized then Zenoh-Flow will deserialize them, if they are
    /// received "typed" then Zenoh-Flow will check that the type matches what is expected.
    ///
    /// If the underlying data is not relevant then these additional operations can be avoided by
    /// calling `take_raw` and using an [`InputRaw`](`InputRaw`) instead.
    pub fn take<T: ZFData>(&mut self, port_id: impl AsRef<str>) -> Option<Input<T>> {
        self.hmap.remove(port_id.as_ref()).map(|receivers| Input {
            _phantom: PhantomData,
            input_raw: InputRaw {
                port_id: port_id.as_ref().into(),
                receivers,
            },
        })
    }

    /// Returns the [`InputRaw`](`InputRaw`) associated to the provided `port_id`, if one is
    /// associated, otherwise `None` is returned.
    ///
    /// ## Convenience
    ///
    /// With an [`InputRaw`](`InputRaw`), Zenoh-Flow will not manipulate the underlying data, for
    /// instance trying to deserialize them to a certain `T`.
    ///
    /// It is thus up to the user to call `try_get::<T>()` on the [`Payload`](`crate::prelude::Payload`) and handle
    /// the error that could surface.
    ///
    /// If all the data must be "converted" to the same type `T` then calling `take::<T>()` and
    /// using a typed [`Input`](`Input`) will be more convenient and as efficient.
    pub fn take_raw(&mut self, port_id: impl AsRef<str>) -> Option<InputRaw> {
        self.hmap
            .remove(port_id.as_ref())
            .map(|receivers| InputRaw {
                port_id: port_id.as_ref().into(),
                receivers,
            })
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

    /// Returns the first [`LinkMessage`](`LinkMessage`) that was received on any of the channels
    /// associated with this Input, or an `Empty` error if there were no messages.
    ///
    /// ## Asynchronous alternative: `recv`
    ///
    /// This method is a synchronous fail-fast alternative to it's asynchronous counterpart: `recv`.
    /// Although synchronous, but given it is "fail-fast", this method will not block the thread on
    /// which it is executed.
    ///
    /// ## Error
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

    /// Returns the first [`LinkMessage`](`LinkMessage`) that was received, *asynchronously*, on any
    /// of the channels associated with this Input.
    ///
    /// If several [`LinkMessage`](`LinkMessage`) are received at the same time, one is *randomly*
    /// selected.
    ///
    /// ## Error
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

#[derive(Clone, Debug)]
pub struct Input<T: ZFData + 'static> {
    _phantom: PhantomData<T>,
    pub(crate) input_raw: InputRaw,
}

// Dereferencing to the [`InputRaw`](`InputRaw`) allows to directly call methods on it with a typed
// [`Input`](`Input`).
impl<T: ZFData + 'static> Deref for Input<T> {
    type Target = InputRaw;

    fn deref(&self) -> &Self::Target {
        &self.input_raw
    }
}

impl<T: ZFData + 'static> Input<T> {
    /// Returns the first [`Message<T>`](`Message`) that was received, *asynchronously*, on any of
    /// the channels associated with this Input.
    ///
    /// If several [`Message<T>`](`Message`) are received at the same time, one is *randomly*
    /// selected.
    ///
    /// This method interprets the data to the type associated with this [`Input<T>`](`Input`).
    ///
    /// ## Performance
    ///
    /// As this method interprets the data received additional operations are performed:
    /// - data received serialized is deserialized (an allocation is performed to store an instance
    ///   of `T`),
    /// - data received "typed" are checked against the type associated to this
    ///   [`Input<T>`](`Input`).
    ///
    /// ## Error
    ///
    /// Several errors can occur:
    /// - all the channels are disconnected,
    /// - Zenoh-Flow failed at interpreting the received data as an instance of `T`.
    pub async fn recv(&self) -> Result<(Message<T>, Timestamp)> {
        match self.input_raw.recv().await? {
            LinkMessage::Data(DataMessage { data, timestamp }) => {
                Ok((Message::Data(Data::try_new(data)?), timestamp))
            }
            LinkMessage::Watermark(timestamp) => Ok((Message::Watermark, timestamp)),
        }
    }

    /// Returns the first [`Message<T>`](`Message`) that was received on any of the channels
    /// associated with this Input, or `None` if all the channels are empty.
    ///
    /// ## Asynchronous alternative: `recv`
    ///
    /// This method is a synchronous fail-fast alternative to it's asynchronous counterpart: `recv`.
    /// Although synchronous, this method will not block the thread on which it is executed.
    ///
    /// ## Error
    ///
    /// Several errors can occur:
    /// - no message was received (i.e. Empty error),
    /// - Zenoh-Flow failed at interpreting the received data as an instance of `T`.
    ///
    /// Note that if some channels are disconnected, for each of such channel an error is logged.
    pub fn try_recv(&self) -> Result<(Message<T>, Timestamp)> {
        match self.input_raw.try_recv()? {
            LinkMessage::Data(DataMessage { data, timestamp }) => {
                Ok((Message::Data(Data::try_new(data)?), timestamp))
            }
            LinkMessage::Watermark(ts) => Ok((Message::Watermark, ts)),
        }
    }
}

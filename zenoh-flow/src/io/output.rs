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

use crate::prelude::{Data, ErrorKind, Payload, PortId, ZFData};
use crate::types::LinkMessage;
use crate::{zferror, Result};
use flume::Sender;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use uhlc::{Timestamp, HLC};

/// The [`Outputs`](`Outputs`) structure contains all the sender channels we created for a
/// [`Source`](`Source`) or an [`Operator`](`Operator`).
///
/// To access these underlying channels, two methods are available:
/// - `take`: this will return an `Output<T>` where `T` implements [`ZFData`](`ZFData`),
/// - `take_raw`: this will return an [`OutputRaw`](`OutputRaw`) — a type agnostic sender.
///
/// Choosing between `take` and `take_raw` is a trade-off between convenience and performance: an
/// `Output<T>` conveniently accepts instances of `T` and is thus less performant as Zenoh-Flow has
/// to manipulate the data (transforming it into a [`LinkMessage`](`LinkMessage`)); an `OutputRaw`
/// is more performant but the transformation must be performed in the code of the Node.
pub struct Outputs {
    pub(crate) hmap: HashMap<PortId, Vec<flume::Sender<LinkMessage>>>,
    pub(crate) hlc: Arc<HLC>,
}

// Dereferencing on the internal [`HashMap`](`Hashmap`) allows users to call all the methods
// implemented on it: `keys()` for one.
impl Deref for Outputs {
    type Target = HashMap<PortId, Vec<flume::Sender<LinkMessage>>>;

    fn deref(&self) -> &Self::Target {
        &self.hmap
    }
}

impl Outputs {
    pub(crate) fn new(hlc: Arc<HLC>) -> Self {
        Self {
            hmap: HashMap::default(),
            hlc,
        }
    }

    /// Insert the `flume::Sender` in the [`Inputs`](`Inputs`), creating the entry if needed in the
    /// internal `HashMap`.
    pub(crate) fn insert(&mut self, port_id: PortId, tx: Sender<LinkMessage>) {
        self.hmap.entry(port_id).or_insert_with(Vec::new).push(tx)
    }

    /// Returns the typed [`Output<T>`](`Output`) associated to the provided `port_id`, if one is
    /// associated, otherwise `None` is returned.
    ///
    /// ## Performance
    ///
    /// With a typed [`Output<T>`](`Output`), only `impl Into<Data<T>>` can be passed to the `send`
    /// methods. This provides type guarantees and leaves the encapsulation to Zenoh-Flow.
    ///
    /// If the underlying data is not relevant (i.e. there is no need to access the `T`) then
    /// calling `take_raw` and using an [`OutputRaw`](`OutputRaw`) will be more efficient. An
    /// [`OutputRaw`](`OutputRaw`) has a dedicated `forward` method that performs no additional
    /// operation and, simply, forwards a [`LinkMessage`](`LinkMessage`).
    pub fn take<T: ZFData>(&mut self, port_id: impl AsRef<str>) -> Option<Output<T>> {
        self.hmap.remove(port_id.as_ref()).map(|senders| Output {
            _phantom: PhantomData,
            output_raw: OutputRaw {
                port_id: port_id.as_ref().into(),
                senders,
                hlc: Arc::clone(&self.hlc),
                last_watermark: Arc::new(AtomicU64::new(
                    self.hlc.new_timestamp().get_time().as_u64(),
                )),
            },
        })
    }

    /// Returns the [`OutputRaw`](`OutputRaw`) associated to the provided `port_id`, if one is
    /// associated, otherwise `None` is returned.
    ///
    /// ## Convenience
    ///
    /// With an [`OutputRaw`](`OutputRaw`), Zenoh-Flow expects a [`LinkMessage`](`LinkMessage`). It
    /// is up to the user to encapsulate an instance of `T` in a [`LinkMessage`](`LinkMessage`).
    ///
    /// If all the data must be encapsulated then calling `take::<T>()` and using a typed
    /// [`Output<T>`](`Output`) will be more convenient and *as efficient*.
    pub fn take_raw(&mut self, port_id: impl AsRef<str>) -> Option<OutputRaw> {
        self.hmap.remove(port_id.as_ref()).map(|senders| OutputRaw {
            port_id: port_id.as_ref().into(),
            senders,
            hlc: Arc::clone(&self.hlc),
            last_watermark: Arc::new(AtomicU64::new(self.hlc.new_timestamp().get_time().as_u64())),
        })
    }
}

/// An [`OutputRaw`](`OutputRaw`) sends [`LinkMessage`](`LinkMessage`) to downstream Nodes.
///
/// It's primary purpose is to ensure "optimal" performance by performing no operation on the
/// received [`LinkMessage`](`LinkMessage`). This can be useful to implement behaviour where actual
/// access to the underlying data is irrelevant.
#[derive(Clone)]
pub struct OutputRaw {
    pub(crate) port_id: PortId,
    pub(crate) senders: Vec<flume::Sender<LinkMessage>>,
    pub(crate) hlc: Arc<HLC>,
    pub(crate) last_watermark: Arc<AtomicU64>,
}

impl OutputRaw {
    /// Returns the port id associated with this Output.
    pub fn port_id(&self) -> &PortId {
        &self.port_id
    }

    /// Returns the number of channels associated with this Output.
    pub fn channels_count(&self) -> usize {
        self.senders.len()
    }

    /// If a timestamp is provided, check that it is not inferior to the latest watermark.
    ///
    /// If no timestamp is provided, a new one is generated from the HLC.
    pub(crate) fn check_timestamp(&self, timestamp: Option<u64>) -> Result<Timestamp> {
        let ts = match timestamp {
            Some(ts_u64) => Timestamp::new(uhlc::NTP64(ts_u64), *self.hlc.get_id()),
            None => self.hlc.new_timestamp(),
        };

        if ts.get_time().0 < self.last_watermark.load(Ordering::Relaxed) {
            return Err(zferror!(ErrorKind::BelowWatermarkTimestamp(ts)).into());
        }

        Ok(ts)
    }

    /// Attempt to forward, *synchronously*, the message to the downstream Nodes.
    ///
    /// ## Asynchronous alternative: `forward`
    ///
    /// This method is a synchronous fail-fast alternative to it's asynchronous counterpart:
    /// `forward`. Hence, although synchronous, this method will not block the thread on which it is
    /// executed.
    ///
    /// ## Errors
    ///
    /// If an error occurs while sending the message on a channel, Zenoh-Flow still tries to send it
    /// on the remaining channels. For each failing channel, an error is logged.
    pub(crate) fn try_forward(&self, message: LinkMessage) -> Result<()> {
        let mut err_count = 0;
        self.senders.iter().for_each(|sender| {
            if let Err(e) = sender.try_send(message.clone()) {
                err_count += 1;
                match e {
                    flume::TrySendError::Full(_) => {
                        log::error!("[Output: {}] A channel is full", self.port_id)
                    }
                    flume::TrySendError::Disconnected(_) => {
                        log::error!("[Output: {}] A channel is disconnected", self.port_id)
                    }
                }
            }
        });

        if err_count > 0 {
            return Err(zferror!(
                ErrorKind::SendError,
                "[Output: {}] Encountered {} errors while sending (async) data",
                self.port_id,
                err_count
            )
            .into());
        }

        Ok(())
    }

    /// Attempt to send, synchronously, the `data` on all channels to the downstream Nodes.
    ///
    /// If no `timestamp` is provided, the current timestamp — as per the [`HLC`](`HLC`) used by the
    /// Zenoh-Flow daemon running this Node — is taken.
    ///
    /// ## Asynchronous alternative: `send`
    ///
    /// This method is a synchronous fail-fast alternative to its asynchronous counterpart: `send`.
    /// Hence, although synchronous, this method will not block the thread on which it is executed.
    ///
    /// ## Errors
    ///
    /// If an error occurs while sending the watermark on a channel, Zenoh-Flow still tries to send
    /// it on the remaining channels. For each failing channel, an error is logged and counted for.
    pub fn try_send(&self, data: impl Into<Payload>, timestamp: Option<u64>) -> Result<()> {
        let ts = self.check_timestamp(timestamp)?;
        let message = LinkMessage::from_serdedata(data.into(), ts);

        self.try_forward(message)
    }

    /// Attempt to send, *synchronously*, the watermark on all channels to the downstream Nodes.
    ///
    /// If no `timestamp` is provided, the current timestamp — as per the [`HLC`](`HLC`) used by the
    /// Zenoh-Flow daemon running this Node — is taken.
    ///
    /// ## Asynchronous alternative: `send_watermark`
    ///
    /// This method is a synchronous fail-fast alternative to it's asynchronous counterpart: `send`.
    /// Although synchronous, this method will not block the thread on which it is executed.
    ///
    /// ## Errors
    ///
    /// If an error occurs while sending the watermark on a channel, Zenoh-Flow still tries to send
    /// it on the remaining channels. For each failing channel, an error is logged and counted for.
    pub fn try_send_watermark(&self, timestamp: Option<u64>) -> Result<()> {
        let ts = self.check_timestamp(timestamp)?;
        self.last_watermark
            .store(ts.get_time().0, Ordering::Relaxed);
        let message = LinkMessage::Watermark(ts);
        self.try_forward(message)
    }

    /// Forward, *asynchronously*, the [`LinkMessage`](`LinkMessage`) on all channels to the
    /// downstream Nodes.
    ///
    /// ## Errors
    ///
    /// If an error occurs while sending the message on a channel, Zenoh-Flow still tries to send
    /// it on the remaining channels. For each failing channel, an error is logged and counted for.
    pub async fn forward(&self, message: LinkMessage) -> Result<()> {
        // FIXME Feels like a cheap hack counting the number of errors. To improve.
        let mut err = 0;
        let fut_senders = self
            .senders
            .iter()
            .map(|sender| sender.send_async(message.clone()));
        // [`join_all`](`futures::future::join_all`) executes all futures in parallel.
        let res = futures::future::join_all(fut_senders).await;

        res.iter().for_each(|res| {
            if let Err(e) = res {
                log::error!(
                    "[Output: {}] Error occured while sending to downstream node(s): {:?}",
                    self.port_id(),
                    e
                );
                err += 1;
            }
        });

        if err > 0 {
            return Err(zferror!(
                ErrorKind::SendError,
                "[Output: {}] Encountered {} errors while sending (async) data",
                self.port_id,
                err
            )
            .into());
        }

        Ok(())
    }

    /// Send, *asynchronously*, the `data` on all channels to the downstream Nodes.
    ///
    /// If no `timestamp` is provided, the current timestamp — as per the [`HLC`](`HLC`) used by the
    /// Zenoh-Flow daemon running this Node — is taken.
    ///
    /// ## Errors
    ///
    /// If an error occurs while sending the watermark on a channel, Zenoh-Flow still tries to send
    /// it on the remaining channels. For each failing channel, an error is logged and counted for.
    pub async fn send(&self, data: impl Into<Payload>, timestamp: Option<u64>) -> Result<()> {
        let ts = self.check_timestamp(timestamp)?;
        let message = LinkMessage::from_serdedata(data.into(), ts);

        self.forward(message).await
    }

    /// Send, *asynchronously*, a [`Watermark`](`LinkMessage::Watermark`) on all channels.
    ///
    /// If no `timestamp` is provided, the current timestamp — as per the [`HLC`](`HLC`) used by the
    /// Zenoh-Flow daemon running this Node — is taken.
    ///
    /// ## Watermarks
    ///
    /// A [`Watermark`](`LinkMessage::Watermark`) is a special kind of message whose purpose is to
    /// signal and guarantee the fact that no message with a lower [`Timestamp`](`Timestamp`) will
    /// be send afterwards.
    ///
    /// ## Errors
    ///
    /// If an error occurs while sending the watermark on a channel, Zenoh-Flow still tries to send
    /// it on the remaining channels. For each failing channel, an error is logged and counted for.
    pub async fn send_watermark(&self, timestamp: Option<u64>) -> Result<()> {
        let ts = self.check_timestamp(timestamp)?;
        self.last_watermark
            .store(ts.get_time().0, Ordering::Relaxed);
        let message = LinkMessage::Watermark(ts);
        self.forward(message).await
    }
}

/// An [`Output<T>`](`Output`) sends instances of `T` to downstream Nodes.
///
/// It's primary purpose is to ensure type guarantees: only types that implement `Into<T>` can be
/// sent to downstream Nodes.
#[derive(Clone)]
pub struct Output<T> {
    _phantom: PhantomData<T>,
    pub(crate) output_raw: OutputRaw,
}

// Dereferencing to the [`OutputRaw`](`OutputRaw`) allows to directly call methods on it with a
// typed [`Output`](`Output`).
impl<T> Deref for Output<T> {
    type Target = OutputRaw;

    fn deref(&self) -> &Self::Target {
        &self.output_raw
    }
}

impl<T: ZFData + 'static> Output<T> {
    /// Send, *asynchronously*, the provided `data` to all downstream Nodes.
    ///
    /// If no `timestamp` is provided, the current timestamp — as per the [`HLC`](`HLC`) used by the
    /// Zenoh-Flow daemon running this Node — is taken.
    ///
    /// ## Constraint `Into<Data<T>>`
    ///
    /// Both `T` and `Data<T>` implement this constraint. Hence, in practice, any type that
    /// implements `Into<T>` can be sent (provided that `Into::<T>::into(u)` is called first).
    ///
    /// ## Errors
    ///
    /// If an error occurs while sending the message on a channel, Zenoh-Flow still tries to send it
    /// on the remaining channels. For each failing channel, an error is logged and counted for. The
    /// total number of encountered errors is returned.
    pub async fn send(&self, data: impl Into<Data<T>>, timestamp: Option<u64>) -> Result<()> {
        let ts = self.check_timestamp(timestamp)?;
        let message = LinkMessage::from_serdedata(Into::<Payload>::into(data.into()), ts);
        self.output_raw.forward(message).await
    }

    /// Tries to send the provided `data` to all downstream Nodes.
    ///
    /// If no `timestamp` is provided, the current timestamp — as per the [`HLC`](`HLC`) used by the
    /// Zenoh-Flow daemon running this Node — is taken.
    ///
    /// ## Constraint `Into<Data<T>>`
    ///
    /// Both `T` and `Data<T>` implement this constraint. Hence, in practice, any type that
    /// implements `Into<T>` can be sent (provided that `Into::<T>::into(u)` is called first).
    ///
    /// ## Errors
    ///
    /// If an error occurs while sending the message on a channel, Zenoh-Flow still tries to send it
    /// on the remaining channels. For each failing channel, an error is logged and counted for. The
    /// total number of encountered errors is returned.
    pub fn try_send(&self, data: impl Into<Data<T>>, timestamp: Option<u64>) -> Result<()> {
        let ts = self.check_timestamp(timestamp)?;
        let message = LinkMessage::from_serdedata(Into::<Payload>::into(data.into()), ts);
        self.output_raw.try_forward(message)
    }
}

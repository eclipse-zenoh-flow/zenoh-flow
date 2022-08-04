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

use crate::{Data, Message, PortId, ZFError, ZFResult};
use async_std::sync::Arc;
use flume::TryRecvError;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
};
use uhlc::{Timestamp, HLC};

pub type Inputs = HashMap<PortId, Input>;
pub type Outputs = HashMap<PortId, Output>;

pub trait Streams {
    type Item;

    fn take(&mut self, port_id: impl AsRef<str>) -> Option<Self::Item>;
}

impl Streams for Inputs {
    type Item = Input;

    fn take(&mut self, port_id: impl AsRef<str>) -> Option<Self::Item> {
        self.remove(port_id.as_ref())
    }
}

impl Streams for Outputs {
    type Item = Output;

    fn take(&mut self, port_id: impl AsRef<str>) -> Option<Self::Item> {
        self.remove(port_id.as_ref())
    }
}

#[derive(Clone, Debug)]
pub struct Input {
    pub(crate) id: PortId,
    pub(crate) receivers: Vec<flume::Receiver<Message>>,
}

impl Input {
    pub fn id(&self) -> &PortId {
        &self.id
    }

    /// Returns the number of channels associated with this Input.
    pub fn channels_count(&self) -> usize {
        self.receivers.len()
    }

    /// Returns the first `Message` that was received, *asynchronously*, on any of the channels
    /// associated with this Input.
    ///
    /// If several `Message` are received at the same time, one is randomly selected.
    ///
    /// ## Error
    ///
    /// If an error occurs on one of the channel, this error is returned.
    pub async fn recv_async(&self) -> ZFResult<Message> {
        let iter = self.receivers.iter().map(|link| link.recv_async());

        // FIXME The remaining futures are not cancelled. Wouldn’t a `race` be better in that
        // situation? Or maybe we can store the other futures in the struct and poll them once
        // `recv` is called again?
        let (res, _, _) = futures::future::select_all(iter).await;

        res.map_err(|e| ZFError::RecvError(format!("{e:?}")))
    }

    /// Returns the first `Message` that was received on any of the channels associated with this
    /// Input.
    ///
    /// The order in which the channels are processed match matches the order in which they were
    /// declared in the description file.
    ///
    /// ## Error
    ///
    /// If an error occurs on one of the channel, this error is returned.
    pub fn recv(&self) -> ZFResult<Message> {
        let mut msg: Option<ZFResult<Message>> = None;

        while msg.is_none() {
            for receiver in &self.receivers {
                match receiver.try_recv() {
                    Ok(message) => {
                        msg.replace(Ok(message));
                        break;
                    }
                    Err(e) => match e {
                        TryRecvError::Empty => (),
                        TryRecvError::Disconnected => {
                            msg.replace(Err(ZFError::Disconnected));
                        }
                    },
                }
            }
        }

        msg.ok_or(ZFError::Empty)?
            .map_err(|e| ZFError::RecvError(format!("{e:?}")))
    }

    pub(crate) fn new(id: PortId) -> Self {
        Self {
            id,
            receivers: vec![],
        }
    }

    pub(crate) fn add(&mut self, receiver: flume::Receiver<Message>) {
        self.receivers.push(receiver);
    }
}

#[derive(Clone)]
pub struct Output {
    pub(crate) port_id: PortId,
    pub(crate) senders: Vec<flume::Sender<Message>>,
    pub(crate) hlc: Arc<HLC>,
    pub(crate) last_watermark: Arc<AtomicU64>,
}

impl Output {
    /// Returns the port id associated with this Output.
    ///
    /// Port ids are unique per type (i.e. Input / Output) and per node.
    pub fn port_id(&self) -> &PortId {
        &self.port_id
    }

    /// Creates a new Output, providing its id and a reference to the HLC.
    ///
    /// The reference to the HLC is used (and required) to generate timestamps and watermarks.
    pub(crate) fn new(id: PortId, hlc: Arc<HLC>) -> Self {
        let now = hlc.new_timestamp();
        Self {
            port_id: id,
            senders: vec![],
            hlc,
            last_watermark: Arc::new(AtomicU64::new(now.get_time().as_u64())),
        }
    }

    /// Add a Sender to this Output.
    pub(crate) fn add(&mut self, tx: flume::Sender<Message>) {
        self.senders.push(tx);
    }

    /// Returns the number of channels associated with this Output.
    pub fn channels_count(&self) -> usize {
        self.senders.len()
    }

    /// If a timestamp is provided, check that it is not inferior to the latest watermark.
    ///
    /// If no timestamp is provided, a new one is generated from the HLC.
    fn check_timestamp(&self, timestamp: Option<u64>) -> ZFResult<Timestamp> {
        let ts = match timestamp {
            Some(ts_u64) => Timestamp::new(uhlc::NTP64(ts_u64), *self.hlc.get_id()),
            None => self.hlc.new_timestamp(),
        };

        if ts.get_time().0 < self.last_watermark.load(Ordering::Relaxed) {
            return Err(ZFError::BelowWatermarkTimestamp(ts));
        }

        Ok(ts)
    }

    fn send_to_all(&self, message: Message) -> ZFResult<()> {
        // FIXME Feels like a cheap hack counting the number of errors. To improve.
        let mut err = 0usize;
        for sender in &self.senders {
            if let Err(e) = sender.send(message.clone()) {
                log::error!("[Output: {}] {:?}", self.port_id, e);
                err += 1;
            }
        }

        if err > 0 {
            return Err(ZFError::SendError(format!(
                "[Output: {}] Encountered {} errors while sending (or trying to)",
                self.port_id, err
            )));
        }

        Ok(())
    }

    pub(crate) async fn send_to_all_async(&self, message: Message) -> ZFResult<()> {
        // FIXME Feels like a cheap hack counting the number of errors. To improve.
        let mut err = 0usize;
        for sender in &self.senders {
            if let Err(e) = sender.send_async(message.clone()).await {
                log::error!("[Output: {}] {:?}", self.port_id, e);
                err += 1;
            }
        }

        if err > 0 {
            return Err(ZFError::SendError(format!(
                "[Output: {}] Encountered {} errors while async sending (or trying to)",
                self.port_id, err
            )));
        }

        Ok(())
    }

    /// Send, *synchronously*, the message on all channels.
    ///
    /// If no timestamp is provided, the current timestamp — as per the HLC — is taken.
    ///
    /// If an error occurs while sending the message on a channel, we still try to send it on the
    /// remaining channels. For each failing channel, an error is logged and counted for. The total
    /// number of encountered errors is returned.
    pub fn send(&self, data: Data, timestamp: Option<u64>) -> ZFResult<()> {
        let ts = self.check_timestamp(timestamp)?;
        let message = Message::from_serdedata(data, ts);
        self.send_to_all(message)
    }

    /// Send, *asynchronously*, the message on all channels.
    ///
    /// If no timestamp is provided, the current timestamp — as per the HLC — is taken.
    ///
    /// If an error occurs while sending the message on a channel, we still try to send it on the
    /// remaining channels. For each failing channel, an error is logged and counted for. The total
    /// number of encountered errors is returned.
    pub async fn send_async(&self, data: Data, timestamp: Option<u64>) -> ZFResult<()> {
        let ts = self.check_timestamp(timestamp)?;
        let message = Message::from_serdedata(data, ts);
        self.send_to_all_async(message).await
    }

    /// Send, *synchronously*, a watermark on all channels.
    ///
    /// If no timestamp is provided, the current timestamp — as per the HLC — is taken.
    ///
    /// If an error occurs while sending the watermark on a channel, we still try to send it on the
    /// remaining channels. For each failing channel, an error is logged and counted for. The total
    /// number of encountered errors is returned.
    pub fn send_watermark(&self, timestamp: Option<u64>) -> ZFResult<()> {
        let ts = self.check_timestamp(timestamp)?;
        self.last_watermark
            .store(ts.get_time().0, Ordering::Relaxed);
        let message = Message::Watermark(ts);
        self.send_to_all(message)
    }

    /// Send, *asynchronously*, a watermark on all channels.
    ///
    /// If no timestamp is provided, the current timestamp — as per the HLC — is taken.
    ///
    /// If an error occurs while sending the watermark on a channel, we still try to send it on the
    /// remaining channels. For each failing channel, an error is logged and counted for. The total
    /// number of encountered errors is returned.
    pub async fn send_watermark_async(&self, timestamp: Option<u64>) -> ZFResult<()> {
        let ts = self.check_timestamp(timestamp)?;
        self.last_watermark
            .store(ts.get_time().0, Ordering::Relaxed);
        let message = Message::Watermark(ts);
        self.send_to_all_async(message).await
    }
}

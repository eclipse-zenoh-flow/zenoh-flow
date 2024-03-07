//
// Copyright (c) 2021 - 2023 ZettaScale Technology
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

use crate::messages::{Data, LinkMessage, Payload, SerializerFn};
use anyhow::bail;
use flume::Sender;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use uhlc::{Timestamp, HLC};
use zenoh_flow_commons::{PortId, Result};

/// The [Outputs] structure contains all the outputs created for a [Source](crate::prelude::Source)
/// or an [Operator](crate::prelude::Operator).
///
/// Each output is indexed by its **port identifier**: the name that was indicated in the descriptor
/// of the node. These names are _case sensitive_ and should be an exact match to what was written
/// in the descriptor.
///
/// Zenoh-Flow provides two flavors of output: [OutputRaw] and [`Output<T>`]. An [`Output<T>`]
/// conveniently accepts instances of `T` while an [OutputRaw] operates at the message level,
/// potentially disregarding the data it contains.
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
    pub fn new(hlc: Arc<HLC>) -> Self {
        Self {
            hmap: HashMap::default(),
            hlc,
        }
    }

    /// Insert the `flume::Sender` in the [Outputs], creating the entry if needed in the internal
    /// `HashMap`.
    pub fn insert(&mut self, port_id: PortId, tx: Sender<LinkMessage>) {
        self.hmap.entry(port_id).or_insert_with(Vec::new).push(tx)
    }

    /// Returns an [OutputBuilder] for the provided `port_id`, if an output was declared with this
    /// exact name in the descriptor of the node, otherwise returns `None`.
    ///
    /// # Usage
    ///
    /// This builder can either produce a, typed, [`Output<T>`] or an [OutputRaw]. The main difference
    /// between both is the type of data they accept: an [`Output<T>`] accepts anything that is
    /// `Into<T>` while an [OutputRaw] accepts a [LinkMessage] or anything that is
    /// `Into<`[Payload]`>`.
    ///
    /// As long as data are produced or manipulated, a typed [`Output<T>`] should be favored.
    ///
    /// ## Typed
    ///
    /// To obtain an [`Output<T>`] one must call `typed` and provide a serializer function. In
    /// the example below we rely on the `serde_json` crate to do the serialization.
    ///
    /// ```ignore
    /// let output_typed: Output<u64> = outputs
    ///     .take("test")
    ///     .expect("No key named 'test' found")
    ///     .typed(|data: &u64| serde_json::to_vec(data).map_err(|e| anyhow::anyhow!(e)));
    /// ```
    ///
    /// ## Raw
    ///
    /// To obtain an [OutputRaw] one must call `raw`.
    ///
    /// ```ignore
    /// let output_raw = outputs
    ///     .take("test")
    ///     .expect("No key named 'test' found")
    ///     .raw();
    /// ```
    pub fn take(&mut self, port_id: impl AsRef<str>) -> Option<OutputBuilder> {
        self.hmap
            .remove(&port_id.as_ref().into())
            .map(|senders| OutputBuilder {
                port_id: port_id.as_ref().into(),
                senders,
                hlc: Arc::clone(&self.hlc),
            })
    }
}

/// An [OutputBuilder] is the intermediate structure to obtain either an [`Output<T>`] or an
/// [OutputRaw].
///
/// The main difference between both is the type of data they accept: an [`Output<T>`] accepts
/// anything that is `Into<T>` while an [OutputRaw] accepts a [LinkMessage] or anything that is
/// `Into<`[Payload]`>`.
///
/// # Planned evolution
///
/// Zenoh-Flow will allow tweaking the behaviour of the underlying channels. For now, the `senders`
/// channels are _unbounded_ and do not implement a dropping policy, which could lead to issues.
pub struct OutputBuilder {
    pub(crate) port_id: PortId,
    pub(crate) senders: Vec<flume::Sender<LinkMessage>>,
    pub(crate) hlc: Arc<HLC>,
}

impl OutputBuilder {
    /// Consume this `OutputBuilder` to produce an [OutputRaw].
    ///
    /// An [OutputRaw] sends [LinkMessage]s (through `forward`) or anything that is
    /// `Into<`[Payload]`>` (through `send` and `try_send`) to downstream nodes.
    ///
    /// The [OutputRaw] was designed for use cases such as load-balancing or rate-limiting. In this
    /// scenarios, the node does not need to access the underlying data and the message can simply
    /// be forwarded downstream.
    ///
    /// # `OutputRaw` vs `Output<T>`
    ///
    /// If the node produces instances of `T` as a result of computations, an [`Output<T>`] should be
    /// favored as it sends anything that is `Into<T>`. Thus, contrary to an [OutputRaw], there is
    /// no need to encapsulate `T` inside a [Payload].
    ///
    /// # Example
    ///
    /// ```ignore
    /// let output_raw = outputs
    ///     .take("test")
    ///     .expect("No key named 'test' found")
    ///     .raw();
    /// ```
    pub fn raw(self) -> OutputRaw {
        OutputRaw {
            port_id: self.port_id,
            senders: self.senders,
            hlc: self.hlc,
        }
    }

    /// Consume this `OutputBuilder` to produce an [`Output<T>`].
    ///
    /// An [`Output<T>`] sends anything that is `Into<T>` (through `send` and `try_send`) to
    /// downstream nodes.
    ///
    /// An [`Output<T>`] requires knowing how to serialize `T`. Data is only serialized when it is (a)
    /// transmitted to a node located on another process or (b) transmitted to a node written in a
    /// programming language other than Rust.
    ///
    /// The serialization will automatically be performed by Zenoh-Flow and only when needed.
    ///
    /// # `Output<T>` vs `OutputRaw`
    ///
    /// If the node does not process any data and only has access to a [LinkMessage], an [OutputRaw]
    /// would be better suited as it does not require to downcast it into an object that
    /// implements `Into<T>`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let output_typed: Output<u64> = outputs
    ///     .take("test")
    ///     .expect("No key named 'test' found")
    ///     .typed(|data: &u64| serde_json::to_vec(data).map_err(|e| anyhow::anyhow!(e)));
    /// ```
    pub fn typed<T: Send + Sync + 'static>(
        self,
        serializer: impl Fn(&mut Vec<u8>, &T) -> anyhow::Result<()> + Send + Sync + 'static,
    ) -> Output<T> {
        Output {
            _phantom: PhantomData,
            output_raw: self.raw(),
            serializer: Arc::new(move |buffer, data| {
                if let Some(typed) = (*data).as_any().downcast_ref::<T>() {
                    match (serializer)(buffer, typed) {
                        Ok(serialized_data) => Ok(serialized_data),
                        Err(e) => bail!(e),
                    }
                } else {
                    bail!("Failed to downcast provided value")
                }
            }),
        }
    }
}

/// An [OutputRaw] sends [LinkMessage] or `Into<`[Payload]`>` to downstream Nodes.
///
/// Its primary purpose is to ensure optimal performance: any message received on an input can
/// transparently be sent downstream, without requiring (a potentially expensive) access to the data
/// it contained.
#[derive(Clone)]
pub struct OutputRaw {
    pub(crate) port_id: PortId,
    pub(crate) senders: Vec<flume::Sender<LinkMessage>>,
    pub(crate) hlc: Arc<HLC>,
}

impl OutputRaw {
    fn make_timestamp(&self, timestamp: Option<u64>) -> Timestamp {
        timestamp
            .map(|ts| Timestamp::new(uhlc::NTP64(ts), *self.hlc.get_id()))
            .unwrap_or_else(|| self.hlc.new_timestamp())
    }

    /// Returns the port id associated with this Output.
    pub fn port_id(&self) -> &PortId {
        &self.port_id
    }

    /// Returns the number of channels associated with this Output.
    pub fn channels_count(&self) -> usize {
        self.senders.len()
    }

    /// Attempt to forward, *synchronously*, the message to the downstream Nodes.
    ///
    /// # Asynchronous alternative: `forward`
    ///
    /// This method is a synchronous fail-fast alternative to it's asynchronous counterpart:
    /// `forward`. Hence, although synchronous, this method will not block the thread on which it is
    /// executed.
    ///
    /// # Errors
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
                        tracing::error!("[Output: {}] Channel is full", self.port_id)
                    }
                    flume::TrySendError::Disconnected(_) => {
                        tracing::error!("[Output: {}] Channel disconnected", self.port_id)
                    }
                }
            }
        });

        if err_count > 0 {
            bail!(
                "[Output: {}] Encountered {} errors while sending (async) data",
                self.port_id,
                err_count
            )
        }

        Ok(())
    }

    /// Attempt to send, *synchronously*, the `data` on all channels to the downstream Nodes.
    ///
    /// If no `timestamp` is provided, the current timestamp (as per the [HLC](uhlc::HLC) used by
    /// the Zenoh-Flow daemon running this Node) is taken.
    ///
    /// # Asynchronous alternative: `send`
    ///
    /// This method is a synchronous fail-fast alternative to its asynchronous counterpart: `send`.
    /// Hence, although synchronous, this method will not block the thread on which it is executed.
    ///
    /// # Errors
    ///
    /// If an error occurs while sending the watermark on a channel, Zenoh-Flow still tries to send
    /// it on the remaining channels. For each failing channel, an error is logged and counted for.
    pub fn try_send(&self, payload: impl Into<Payload>, timestamp: Option<u64>) -> Result<()> {
        let message = LinkMessage {
            payload: payload.into(),
            timestamp: self.make_timestamp(timestamp),
        };

        self.try_forward(message)
    }

    /// Forward, *asynchronously*, the [LinkMessage] on all channels to the downstream Nodes.
    ///
    /// # Errors
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
                tracing::error!(
                    "[Output: {}] Error occured while sending to downstream node(s): {:?}",
                    self.port_id(),
                    e
                );
                err += 1;
            }
        });

        if err > 0 {
            bail!(
                "[Output: {}] Encountered {} errors while sending (async) data",
                self.port_id,
                err
            )
        }

        Ok(())
    }

    /// Send, *asynchronously*, the `data` on all channels to the downstream Nodes.
    ///
    /// If no `timestamp` is provided, the current timestamp — as per the [HLC](uhlc::HLC) used by
    /// the Zenoh-Flow daemon running this Node — is taken.
    ///
    /// # Errors
    ///
    /// If an error occurs while sending the watermark on a channel, Zenoh-Flow still tries to send
    /// it on the remaining channels. For each failing channel, an error is logged and counted for.
    pub async fn send(&self, payload: impl Into<Payload>, timestamp: Option<u64>) -> Result<()> {
        let message = LinkMessage {
            payload: payload.into(),
            timestamp: self.make_timestamp(timestamp),
        };

        self.forward(message).await
    }
}

/// An [`Output<T>`] sends instances of `T` to downstream Nodes.
///
/// It's primary purpose is to ensure type guarantees: only types that implement `Into<T>` can be
/// sent to downstream Nodes.
#[derive(Clone)]
pub struct Output<T> {
    _phantom: PhantomData<T>,
    pub(crate) output_raw: OutputRaw,
    pub(crate) serializer: Arc<SerializerFn>,
}

// Dereferencing to the [`OutputRaw`](`OutputRaw`) allows to directly call methods on it with a
// typed [`Output`](`Output`).
impl<T> Deref for Output<T> {
    type Target = OutputRaw;

    fn deref(&self) -> &Self::Target {
        &self.output_raw
    }
}

impl<T: Send + Sync + 'static> Output<T> {
    // Construct the `LinkMessage` to send.
    fn construct_message(
        &self,
        data: impl Into<Data<T>>,
        timestamp: Option<u64>,
    ) -> Result<LinkMessage> {
        let payload = Payload::from_data(data.into(), Arc::clone(&self.serializer));
        Ok(LinkMessage {
            payload,
            timestamp: self.make_timestamp(timestamp),
        })
    }

    /// Send, *asynchronously*, the provided `data` to all downstream Nodes.
    ///
    /// If no `timestamp` is provided, the current timestamp (as per the [HLC](uhlc::HLC) used by
    /// the Zenoh-Flow daemon running this Node) is taken.
    ///
    /// # Constraint `Into<Data<T>>`
    ///
    /// Both `T` and `Data<T>` implement this constraint. Hence, in practice, any type that
    /// implements `Into<T>` can be sent (provided that `Into::<T>::into(u)` is called first).
    ///
    /// # Errors
    ///
    /// If an error occurs while sending the message on a channel, Zenoh-Flow still tries to send it
    /// on the remaining channels. For each failing channel, an error is logged and counted for. The
    /// total number of encountered errors is returned.
    pub async fn send(&self, data: impl Into<Data<T>>, timestamp: Option<u64>) -> Result<()> {
        self.output_raw
            .forward(self.construct_message(data, timestamp)?)
            .await
    }

    /// Tries to send the provided `data` to all downstream Nodes.
    ///
    /// If no `timestamp` is provided, the current timestamp (as per the [HLC](uhlc::HLC) used by
    /// the Zenoh-Flow daemon running this Node) is taken.
    ///
    /// # Constraint `Into<Data<T>>`
    ///
    /// Both `T` and `Data<T>` implement this constraint. Hence, in practice, any type that
    /// implements `Into<T>` can be sent (provided that `Into::<T>::into(u)` is called first).
    ///
    /// # Errors
    ///
    /// If an error occurs while sending the message on a channel, Zenoh-Flow still tries to send it
    /// on the remaining channels. For each failing channel, an error is logged and counted for. The
    /// total number of encountered errors is returned.
    pub fn try_send(&self, data: impl Into<Data<T>>, timestamp: Option<u64>) -> Result<()> {
        self.output_raw
            .try_forward(self.construct_message(data, timestamp)?)
    }
}

#[cfg(test)]
#[path = "./tests/output-tests.rs"]
mod tests;

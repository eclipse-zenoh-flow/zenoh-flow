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

use crate::{Data, Message, PortId, ZFResult};
use async_std::sync::Arc;
use futures::Future;
use std::pin::Pin;

/// The Zenoh Flow link sender.
/// A wrapper over a flume Sender, that sends `Arc<Message>` and is associated
/// with a `PortId`
#[derive(Clone, Debug)]
pub struct LinkSender {
    pub id: PortId,
    pub sender: flume::Sender<Arc<Message>>,
}

/// The Zenoh Flow link receiver.
/// A wrapper over a flume Receiver, that receives `Arc<Message>` and the associated
/// `PortId`
#[derive(Clone, Debug)]
pub struct LinkReceiver {
    pub id: PortId,
    pub receiver: flume::Receiver<Arc<Message>>,
}

/// The output of the [`LinkReceiver`](`LinkReceiver`), a tuple
/// containing the `PortId` and `Arc<Message>`.
///
/// In Zenoh Flow `T = Data`.
///
///
pub type ZFLinkOutput = ZFResult<(PortId, Arc<Message>)>;

impl LinkReceiver {
    /// Wrapper over flume::Receiver::recv_async(),
    /// it returns [`ZFLinkOutput`](`ZFLinkOutput`)
    ///
    /// # Errors
    /// If fails if the link is disconnected
    pub fn recv(
        &self,
    ) -> ::core::pin::Pin<Box<dyn std::future::Future<Output = ZFLinkOutput> + '_ + Send + Sync>>
    {
        async fn __recv(_self: &LinkReceiver) -> ZFResult<(PortId, Arc<Message>)> {
            Ok((_self.id.clone(), _self.receiver.recv_async().await?))
        }

        Box::pin(__recv(self))
    }

    /// Discards the message
    ///
    /// *Note:* Not implemented.
    ///
    /// # Errors
    /// It fails if the link is disconnected
    pub async fn discard(&self) -> ZFResult<()> {
        Ok(())
    }

    /// Returns the `PortId` associated with the receiver.
    pub fn id(&self) -> PortId {
        self.id.clone()
    }

    /// Checks if the receiver is disconnected.
    pub fn is_disconnected(&self) -> bool {
        self.receiver.is_disconnected()
    }
}

impl LinkSender {
    /// Wrapper over flume::Sender::send_async(),
    /// it sends `Arc<Message>`.
    ///
    /// # Errors
    /// It fails if the link is disconnected
    pub async fn send(&self, data: Arc<Message>) -> ZFResult<()> {
        Ok(self.sender.send_async(data).await?)
    }

    /// Returns the sender occupation.
    pub fn len(&self) -> usize {
        self.sender.len()
    }

    /// Checks if the sender is empty.
    pub fn is_empty(&self) -> bool {
        self.sender.is_empty()
    }

    /// Returns the sender capacity if any.
    pub fn capacity(&self) -> Option<usize> {
        self.sender.capacity()
    }

    /// Returns the sender `PortId`.
    pub fn id(&self) -> PortId {
        self.id.clone()
    }

    /// Checks is the sender is disconnected.
    pub fn is_disconnected(&self) -> bool {
        self.sender.is_disconnected()
    }
}

/// Creates the `Link` with the given capacity and `PortId`s.
pub fn link(
    capacity: Option<usize>,
    send_id: PortId,
    recv_id: PortId,
) -> (LinkSender, LinkReceiver) {
    let (sender, receiver) = match capacity {
        None => flume::unbounded(),
        Some(cap) => flume::bounded(cap),
    };

    (
        LinkSender {
            id: send_id,
            sender,
        },
        LinkReceiver {
            id: recv_id,
            receiver,
        },
    )
}

// Async closures

/// Trait wrapping an async closures for receiver callback, it requires rust-nightly because of
/// https://github.com/rust-lang/rust/issues/62290
///
/// * Note: * not intended to be directly used by users.
pub trait AsyncCallbackRx: Send + Sync {
    fn call(
        &self,
        arg: Arc<Message>,
    ) -> Pin<Box<dyn Future<Output = ZFResult<()>> + Send + Sync + 'static>>;
}

/// Implementation of AsyncCallbackRx for any async closure that takes
/// `Arc<Message>` as parameter and returns `ZFResult<()>`.
/// This "converts" any `async move |msg| { ... Ok() }` to `AsyncCallbackRx`
///
/// *Note:* It takes an `FnOnce` because of the `move` keyword. The closure
/// has to be `Clone` as we are going to call the closure more than once.
impl<Fut, Fun> AsyncCallbackRx for Fun
where
    Fun: FnOnce(Arc<Message>) -> Fut + Sync + Send + Clone,
    Fut: Future<Output = ZFResult<()>> + 'static + Send + Sync,
{
    fn call(
        &self,
        arg: Arc<Message>,
    ) -> Pin<Box<dyn Future<Output = ZFResult<()>> + Send + Sync + 'static>> {
        Box::pin(self.clone()(arg))
    }
}

/// The `AsyncCallbackReceiver` wraps the `LinkReceiver` and the
/// `AsyncCallbackRx`.
///
/// It is used to trigger the user callback when a new message is available.
#[derive(Clone)]
pub struct AsyncCallbackReceiver {
    _id: String,
    rx: LinkReceiver,
    cb: Arc<dyn AsyncCallbackRx>,
}

impl AsyncCallbackReceiver {
    pub fn new(_id: String, rx: LinkReceiver, cb: Arc<dyn AsyncCallbackRx>) -> Self {
        Self { _id, rx, cb }
    }

    pub async fn run(&self) -> ZFResult<()> {
        let (_id, msg) = self.rx.recv().await?;
        self.cb.call(msg).await
    }
}

/// Trait wrapping an async closures for sender callback, it requires rust-nightly because of
/// https://github.com/rust-lang/rust/issues/62290
///
/// * Note: * not intended to be directly used by users.
pub trait AsyncCallbackTx: Send + Sync {
    fn call(&self) -> Pin<Box<dyn Future<Output = ZFResult<Data>> + Send + Sync + 'static>>;
}

/// Implementation of AsyncCallbackTx for any async closure that returns
/// `ZFResult<()>`.
/// This "converts" any `async move { ... }` to `AsyncCallbackTx`
///
/// *Note:* It takes an `FnOnce` because of the `move` keyword. The closure
/// has to be `Clone` as we are going to call the closure more than once.
impl<Fut, Fun> AsyncCallbackTx for Fun
where
    Fun: FnOnce() -> Fut + Sync + Send + Clone,
    Fut: Future<Output = ZFResult<Data>> + Send + Sync + 'static,
{
    fn call(&self) -> Pin<Box<dyn Future<Output = ZFResult<Data>> + Send + Sync + 'static>> {
        Box::pin(self.clone()())
    }
}

/// The `AsyncCallbackSender` wraps the `LinkSender` and the
/// `AsyncCallbackTx`.
///
/// It is used to trigger the user callback with the given schedule.
pub struct AsyncCallbackSender {
    _id: String,
    tx: LinkSender,
    cb: Arc<dyn AsyncCallbackTx>,
}

impl AsyncCallbackSender {
    pub fn new(_id: String, tx: LinkSender, cb: Arc<dyn AsyncCallbackTx>) -> Self {
        Self { _id, tx, cb }
    }

    pub async fn trigger(&self) -> ZFResult<()> {
        let data = self.cb.call().await?;
        // FIXME
        let ts = uhlc::Timestamp::new(uhlc::NTP64(0u64), uhlc::ID::new(1, [0u8; 16]));

        // FIXME
        let msg = Message::from_node_output(crate::NodeOutput::Data(data), ts);
        self.tx.send(Arc::new(msg)).await
    }
}

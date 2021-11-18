//
// Copyright (c) 2017, 2021 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//

use crate::{PortId, ZFResult};
use async_std::sync::Arc;

#[derive(Clone, Debug)]
pub struct LinkSender<T> {
    pub id: PortId,
    pub sender: flume::Sender<Arc<T>>,
}

#[derive(Clone, Debug)]
pub struct LinkReceiver<T> {
    pub id: PortId,
    pub receiver: flume::Receiver<Arc<T>>,
}

pub type ZFLinkOutput<T> = ZFResult<(PortId, Arc<T>)>;

impl<T: std::marker::Send + std::marker::Sync> LinkReceiver<T> {
    pub fn recv(
        &self,
    ) -> ::core::pin::Pin<Box<dyn std::future::Future<Output = ZFLinkOutput<T>> + '_ + Send + Sync>>
    {
        async fn __recv<T>(_self: &LinkReceiver<T>) -> ZFResult<(PortId, Arc<T>)> {
            Ok((_self.id.clone(), _self.receiver.recv_async().await?))
        }

        Box::pin(__recv(self))
    }

    pub async fn discard(&self) -> ZFResult<()> {
        Ok(())
    }

    pub fn id(&self) -> PortId {
        self.id.clone()
    }

    pub fn is_disconnected(&self) -> bool {
        self.receiver.is_disconnected()
    }
}

impl<T> LinkSender<T> {
    pub async fn send(&self, data: Arc<T>) -> ZFResult<()> {
        Ok(self.sender.send_async(data).await?)
    }

    pub fn len(&self) -> usize {
        self.sender.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sender.is_empty()
    }

    pub fn capacity(&self) -> Option<usize> {
        self.sender.capacity()
    }

    pub fn id(&self) -> PortId {
        self.id.clone()
    }

    pub fn is_disconnected(&self) -> bool {
        self.sender.is_disconnected()
    }
}

pub fn link<T>(
    capacity: Option<usize>,
    send_id: PortId,
    recv_id: PortId,
) -> (LinkSender<T>, LinkReceiver<T>) {
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

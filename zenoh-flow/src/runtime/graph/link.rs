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
use async_std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct LinkSender<T> {
    pub id: PortId,
    pub sender: flume::Sender<Arc<T>>,
}

#[derive(Clone, Debug)]
pub struct ZFLinkReceiver<T> {
    pub id: PortId,
    pub receiver: flume::Receiver<Arc<T>>,
    pub last_message: Arc<Mutex<Option<Arc<T>>>>,
}

pub type ZFLinkOutput<T> = ZFResult<(PortId, Arc<T>)>;

impl<T: std::marker::Send + std::marker::Sync> ZFLinkReceiver<T> {
    pub fn peek(
        &self,
    ) -> ::core::pin::Pin<Box<dyn std::future::Future<Output = ZFLinkOutput<T>> + '_>> {
        async fn __peek<T>(_self: &ZFLinkReceiver<T>) -> ZFResult<(PortId, Arc<T>)> {
            let mut guard = _self.last_message.lock().await;

            match &*guard {
                Some(message) => Ok((_self.id.clone(), message.clone())),
                None => {
                    let last_message = _self.receiver.recv_async().await?;
                    *guard = Some(last_message.clone());

                    Ok((_self.id.clone(), last_message))
                }
            }
        }
        Box::pin(__peek(self))
    }

    pub fn recv(
        &self,
    ) -> ::core::pin::Pin<Box<dyn std::future::Future<Output = ZFLinkOutput<T>> + '_ + Send + Sync>>
    {
        async fn __recv<T>(_self: &ZFLinkReceiver<T>) -> ZFResult<(PortId, Arc<T>)> {
            let mut guard = _self.last_message.lock().await;
            match &*guard {
                Some(message) => {
                    let msg = message.clone();
                    *guard = None;

                    Ok((_self.id.clone(), msg))
                }
                None => Ok((_self.id.clone(), _self.receiver.recv_async().await?)),
            }
        }

        Box::pin(__recv(self))
    }

    pub async fn discard(&self) -> ZFResult<()> {
        let mut guard = self.last_message.lock().await;
        *guard = None;
        Ok(())
    }

    pub fn id(&self) -> PortId {
        self.id.clone()
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
}

pub fn link<T>(
    capacity: Option<usize>,
    send_id: String,
    recv_id: String,
) -> (LinkSender<T>, ZFLinkReceiver<T>) {
    let (sender, receiver) = match capacity {
        None => flume::unbounded(),
        Some(cap) => flume::bounded(cap),
    };

    (
        LinkSender {
            id: send_id.into(),
            sender,
        },
        ZFLinkReceiver {
            id: recv_id.into(),
            receiver,
            last_message: Arc::new(Mutex::new(None)),
        },
    )
}

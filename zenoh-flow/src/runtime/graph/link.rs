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

use async_std::sync::Arc;

use crate::ZFResult;

#[derive(Clone, Debug)]
pub struct ZFLinkSender<T> {
    pub id: String,
    pub sender: flume::Sender<Arc<T>>,
}

#[derive(Clone, Debug)]
pub struct ZFLinkReceiver<T> {
    pub id: String,
    pub receiver: flume::Receiver<Arc<T>>,
    pub last_message: Option<Arc<T>>,
}

pub type ZFLinkOutput<T> = ZFResult<(String, Arc<T>)>;

impl<T: std::marker::Send + std::marker::Sync> ZFLinkReceiver<T> {
    // pub async fn peek(&mut self) -> ZFResult<(String, Arc<T>)> {
    //     match &self.last_message {
    //         Some(message) => Ok((self.id, message.clone())),
    //         None => {
    //             let last_message = self.receiver.recv_async().await?;
    //             self.last_message = Some(last_message.clone());

    //             Ok((self.id, last_message))
    //         }
    //     }
    // }

    pub fn peek(
        &mut self,
    ) -> ::core::pin::Pin<Box<dyn std::future::Future<Output = ZFLinkOutput<T>> + '_>> {
        async fn __peek<T>(_self: &mut ZFLinkReceiver<T>) -> ZFResult<(String, Arc<T>)> {
            match &_self.last_message {
                Some(message) => Ok((_self.id.clone(), message.clone())),
                None => {
                    let last_message = _self.receiver.recv_async().await?;
                    _self.last_message = Some(last_message.clone());

                    Ok((_self.id.clone(), last_message))
                }
            }
        }
        Box::pin(__peek(self))
    }

    // pub async fn recv(&mut self) -> ZFResult<Arc<T>> {
    //     match &self.last_message {
    //         Some(message) => {
    //             let msg = message.clone();
    //             self.last_message = None;

    //             Ok(msg)
    //         }
    //         None => Ok(self.receiver.recv_async().await?),
    //     }
    // }

    pub fn try_recv(&mut self) -> ZFResult<(String, Arc<T>)> {
        match &self.last_message {
            Some(message) => {
                let msg = message.clone();
                self.last_message = None;

                Ok((self.id.clone(), msg))
            }
            None => Ok((self.id.clone(), self.receiver.try_recv()?)),
        }
    }

    pub fn recv(
        &mut self,
    ) -> ::core::pin::Pin<Box<dyn std::future::Future<Output = ZFLinkOutput<T>> + '_ + Send + Sync>>
    {
        async fn __recv<T>(_self: &mut ZFLinkReceiver<T>) -> ZFResult<(String, Arc<T>)> {
            match &_self.last_message {
                Some(message) => {
                    let msg = message.clone();
                    _self.last_message = None;

                    Ok((_self.id.clone(), msg))
                }
                None => Ok((_self.id.clone(), _self.receiver.recv_async().await?)),
            }
        }

        Box::pin(__recv(self))
    }

    pub fn drop(&mut self) -> ZFResult<()> {
        self.last_message = None;
        Ok(())
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }
}

impl<T> ZFLinkSender<T> {
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

    pub fn id(&self) -> String {
        self.id.clone()
    }
}

pub fn link<T>(
    capacity: Option<usize>,
    send_id: String,
    recv_id: String,
) -> (ZFLinkSender<T>, ZFLinkReceiver<T>) {
    let (sender, receiver) = match capacity {
        None => flume::unbounded(),
        Some(cap) => flume::bounded(cap),
    };

    (
        ZFLinkSender {
            id: send_id,
            sender,
        },
        ZFLinkReceiver {
            id: recv_id,
            receiver,
            last_message: None,
        },
    )
}

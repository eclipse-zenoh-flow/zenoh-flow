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

pub struct ZFLinkSender<T> {
    pub sender: flume::Sender<Arc<T>>,
}

pub struct ZFLinkReceiver<T> {
    pub receiver: flume::Receiver<Arc<T>>,
    pub last_message: Option<Arc<T>>,
}

impl<T> ZFLinkReceiver<T> {
    pub async fn peek(&mut self) -> ZFResult<Arc<T>> {
        match &self.last_message {
            Some(message) => Ok(message.clone()),
            None => {
                let last_message = self.receiver.recv_async().await?;
                self.last_message = Some(last_message.clone());

                Ok(last_message)
            }
        }
    }

    pub async fn recv(&mut self) -> ZFResult<Arc<T>> {
        match &self.last_message {
            Some(message) => {
                let msg = message.clone();
                self.last_message = None;

                Ok(msg)
            }
            None => Ok(self.receiver.recv_async().await?),
        }
    }

    pub fn drop(&mut self) -> ZFResult<()> {
        self.last_message = None;
        Ok(())
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
}

pub fn link<T>(capacity: usize) -> (ZFLinkSender<T>, ZFLinkReceiver<T>) {
    let (sender, receiver) = flume::bounded(capacity);
    (
        ZFLinkSender { sender },
        ZFLinkReceiver {
            receiver,
            last_message: None,
        },
    )
}

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
use crate::stream::{ZFFIFOError, ZFLinkReceiverTrait, ZFLinkSenderTrait};

use async_std::sync::{Arc, Mutex};
use async_trait::async_trait;
use event_listener::Event;
use std::collections::VecDeque;

pub fn new_fifo<T>(capacity: usize, id: usize) -> (ZFFifoSender<T>, ZFFifoReceiver<T>) {
    let buf = Arc::new(Mutex::new(VecDeque::<T>::with_capacity(capacity)));
    let not_full = Arc::new(Event::new());
    let not_empty = Arc::new(Event::new());
    let sender = ZFFifoSender::<T>::new(
        capacity,
        id,
        buf.clone(),
        not_full.clone(),
        not_empty.clone(),
    );
    let receiver = ZFFifoReceiver::<T>::new(
        capacity,
        id,
        buf.clone(),
        not_full.clone(),
        not_empty.clone(),
    );
    (sender, receiver)
}

pub struct ZFFifoSender<T> {
    queue: Arc<Mutex<VecDeque<T>>>,
    id: usize,
    size: usize,
    not_full: Arc<Event>,
    not_empty: Arc<Event>, // other fields
}

impl<T> ZFFifoSender<T> {
    pub fn new(
        size: usize,
        id: usize,
        queue: Arc<Mutex<VecDeque<T>>>,
        not_full: Arc<Event>,
        not_empty: Arc<Event>,
    ) -> Self {
        Self {
            queue,
            id,
            size,
            not_empty,
            not_full,
        }
    }
}

#[async_trait]
impl<T> ZFLinkSenderTrait<T> for ZFFifoSender<T>
where
    T: std::marker::Send,
{
    fn try_send(&self, msg: T) -> Result<(), ZFFIFOError> {
        if let Some(mut guard) = self.queue.try_lock() {
            if guard.len() < self.size {
                guard.push_back(msg);
                self.not_empty.notify(1);
                return Ok(());
            } else {
                return Err(ZFFIFOError::Full);
            }
        }
        Err(ZFFIFOError::Locked)
    }

    async fn send(&self, msg: T) -> Result<(), ZFFIFOError> {
        loop {
            let mut guard = self.queue.lock().await;
            if guard.len() < self.size {
                guard.push_back(msg);
                self.not_empty.notify(1);
                return Ok(());
            }
            drop(guard);
            self.not_full.listen().await
        }
    }

    // fn is_closed(&self) -> bool {
    //     true
    // }

    // fn is_full(&self) -> bool {
    //     false
    // }

    // fn is_empty(&self) -> bool {
    //     false
    // }

    // fn len(&self) -> usize {
    //     self.queue.lock().await.len()
    // }

    fn capacity(&self) -> usize {
        self.size
    }

    fn id(&self) -> usize {
        self.id
    }
}

pub struct ZFFifoReceiver<T> {
    queue: Arc<Mutex<VecDeque<T>>>,
    id: usize,
    size: usize,
    not_full: Arc<Event>,
    not_empty: Arc<Event>, // other fields
}

impl<T> ZFFifoReceiver<T> {
    pub fn new(
        size: usize,
        id: usize,
        queue: Arc<Mutex<VecDeque<T>>>,
        not_full: Arc<Event>,
        not_empty: Arc<Event>,
    ) -> Self {
        Self {
            queue,
            id,
            size,
            not_empty,
            not_full,
        }
    }
}

#[async_trait]
impl<T> ZFLinkReceiverTrait<T> for ZFFifoReceiver<T>
where
    T: std::marker::Send,
{
    fn try_recv(&self) -> Result<(T, usize), ZFFIFOError> {
        if let Some(mut guard) = self.queue.try_lock() {
            if guard.len() > 0 {
                if let Some(res) = guard.pop_front() {
                    drop(guard);
                    self.not_full.notify(1);
                    return Ok((res, self.id));
                } else {
                    return Err(ZFFIFOError::Empty);
                }
            } else {
                return Err(ZFFIFOError::Empty);
            }
        }
        Err(ZFFIFOError::Locked)
    }

    async fn recv(&self) -> Result<(T, usize), ZFFIFOError> {
        loop {
            let mut guard = self.queue.lock().await;
            if guard.len() > 0 {
                if let Some(res) = guard.pop_front() {
                    drop(guard);
                    self.not_full.notify(1);
                    return Ok((res, self.id));
                }
            }
            drop(guard);
            self.not_empty.listen().await;
        }
    }

    /// Consumes the last message in the queue
    async fn take(&self) -> Result<(T, usize), ZFFIFOError> {
        self.recv().await
    }

    /// Same as take but not async, if no messages are present returns
    /// ZFFIFOError::Empty
    fn try_take(&self) -> Result<(T, usize), ZFFIFOError> {
        self.try_recv()
    }

    /// Reads the last message in the queue without consuming it.
    /// The message will still be in the queue.
    async fn peek(&self) -> Result<(&T, usize), ZFFIFOError> {
        // This does not compile becase of res referencing the mutex guard
        loop {
            let guard = self.queue.lock().await;
            if guard.len() > 0 {
                if let Some(res) = guard.get(0) {
                    //return Ok((res, self.id));
                    return Err(ZFFIFOError::Empty);
                }
            }
            drop(guard);
            self.not_empty.listen().await;
        }
    }

    /// Same as peek but not async, if no messages are present returns
    /// ZFFIFOError::Empty
    fn try_peek(&self) -> Result<(&T, usize), ZFFIFOError> {
        // This does not compile because of res referencing the mutex guard
        // if let Some(guard) = self.queue.try_lock() {
        //     if guard.len() > 0 {
        //         if let Some(res) = guard.get(0) {
        //             return Ok((res, self.id));
        //         } else {
        //             return Err(ZFFIFOError::Empty);
        //         }
        //     } else {
        //         return Err(ZFFIFOError::Empty);
        //     }
        // }
        // Err(ZFFIFOError::Locked)
        unimplemented!()
    }

    // fn is_closed(&self) -> bool {
    //     true
    // }

    // fn is_full(&self) -> bool {
    //     false
    // }

    // fn is_empty(&self) -> bool {
    //     false
    // }

    // fn len(&self) -> usize {
    //     0
    // }

    fn capacity(&self) -> usize {
        self.size
    }

    fn id(&self) -> usize {
        self.id
    }
}

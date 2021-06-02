//
// Copyright (c) 2017, 2020 ADLINK Technology Inc.
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
use async_trait::async_trait;

#[derive(Debug, PartialEq, Eq)]
pub enum ZFFIFOError {
    Full,
    Closed,
    Empty,
    Locked,
}

#[async_trait]
pub trait ZFLinkSenderTrait<T> {
    fn try_send(&self, msg: T) -> Result<(), ZFFIFOError>;

    async fn send(&self, msg: T) -> Result<(), ZFFIFOError>;

    // fn is_closed(&self) -> bool;

    // fn is_full(&self) -> bool;

    // fn is_empty(&self) -> bool;

    // fn async len(&self) -> usize;

    fn capacity(&self) -> usize;

    fn id(&self) -> usize;
}

#[async_trait]
pub trait ZFLinkReceiverTrait<T> {
    fn try_recv(&self) -> Result<(T, usize), ZFFIFOError>;

    async fn recv(&self) -> Result<(T, usize), ZFFIFOError>;

    /// Consumes the last message in the queue
    async fn take(&self) -> Result<(T, usize), ZFFIFOError>;

    /// Same as take but not async, if no messages are present returns
    /// ZFFIFOError::Empty
    fn try_take(&self) -> Result<(T, usize), ZFFIFOError>;

    /// Reads the last message in the queue without consuming it.
    /// The message will still be in the queue.
    async fn peek(&self) -> Result<(&T, usize), ZFFIFOError>;

    /// Same as peek but not async, if no messages are present returns
    /// ZFFIFOError::Empty
    fn try_peek(&self) -> Result<(&T, usize), ZFFIFOError>;

    // fn is_closed(&self) -> bool;

    // fn is_full(&self) -> bool;

    // fn is_empty(&self) -> bool;

    // fn len(&self) -> usize;

    fn capacity(&self) -> usize;

    fn id(&self) -> usize;
}

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

use crate::{DataMessage, Message, ZFError, ZFResult};
use async_std::sync::Arc;
use uhlc::Timestamp;

#[derive(Debug, Clone)]
pub enum TokenAction {
    Consume, // Default, data is passed to the run and consumed from the "thing"
    Drop,    // Data is dropped
    KeepRun, // Data is passed to the run and kept in the "thing"
    Keep,    // Data is kept in the "thing"
    Wait,    // Waits the Data, this is applicable only to NotReadyToken
}

#[derive(Debug, Clone)]
pub struct ReadyToken {
    pub data: DataMessage,
    pub action: TokenAction,
}

#[derive(Debug, Clone)]
pub enum Token {
    Pending,
    Ready(ReadyToken),
}

impl Token {
    pub fn new_ready(data: DataMessage) -> Self {
        Self::Ready(ReadyToken {
            data,
            action: TokenAction::Consume,
        })
    }

    pub fn get_timestamp(&self) -> Option<Timestamp> {
        match self {
            Self::Pending => None,
            Self::Ready(token) => Some(token.data.timestamp),
        }
    }

    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready(_))
    }

    pub fn is_not_ready(&self) -> bool {
        matches!(self, Self::Pending)
    }

    pub fn consume(&mut self) -> ZFResult<()> {
        match self {
            Self::Ready(ref mut ready) => {
                ready.action = TokenAction::Consume;
                Ok(())
            }
            _ => Err(ZFError::GenericError),
        }
    }

    pub fn drop(&mut self) -> ZFResult<()> {
        match self {
            Self::Ready(ref mut ready) => {
                ready.action = TokenAction::Drop;
                Ok(())
            }
            _ => Err(ZFError::GenericError),
        }
    }

    pub fn keep_run(&mut self) -> ZFResult<()> {
        match self {
            Self::Ready(ref mut ready) => {
                ready.action = TokenAction::KeepRun;
                Ok(())
            }
            _ => Err(ZFError::GenericError),
        }
    }

    pub fn keep(&mut self) -> ZFResult<()> {
        match self {
            Self::Ready(ref mut ready) => {
                ready.action = TokenAction::Keep;
                Ok(())
            }
            _ => Err(ZFError::GenericError),
        }
    }

    pub fn data(&self) -> ZFResult<DataMessage> {
        match self {
            Self::Ready(ready) => Ok(ready.data.clone()),
            _ => Err(ZFError::GenericError),
        }
    }

    pub fn action(&self) -> &TokenAction {
        match self {
            Self::Ready(ready) => &ready.action,
            Self::Pending => &TokenAction::Wait,
        }
    }

    pub fn split(self) -> (Option<DataMessage>, TokenAction) {
        match self {
            Self::Ready(ready) => (Some(ready.data), ready.action),
            Self::Pending => (None, TokenAction::Wait),
        }
    }
}

impl From<Arc<Message>> for Token {
    fn from(message: Arc<Message>) -> Self {
        match message.as_ref() {
            Message::Control(_) => Token::Pending,
            Message::Data(data_message) => Token::new_ready(data_message.clone()),
        }
    }
}

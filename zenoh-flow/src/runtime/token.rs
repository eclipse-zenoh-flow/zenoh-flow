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

use crate::runtime::deadline::E2EDeadlineMiss;
use crate::{Data, DataMessage, PortId};
use std::collections::HashMap;
use uhlc::Timestamp;

#[derive(Clone)]
pub struct Tokens {
    pub(crate) map: HashMap<PortId, Token>,
}

impl Tokens {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
        }
    }

    pub fn get_mut(&mut self, port_id: &PortId) -> Option<&mut Token> {
        self.map.get_mut(port_id)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenAction {
    Consume,
    Drop,
    Keep,
}

impl std::fmt::Display for TokenAction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Consume => write!(f, "Consume"),
            Self::Keep => write!(f, "Keep"),
            Self::Drop => write!(f, "Drop"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReadyToken {
    pub(crate) data: DataMessage,
    pub(crate) action: TokenAction,
}

impl ReadyToken {
    /// Tell Zenoh Flow to immediately discard the data.
    ///
    /// The data associated to the `Token` will be immediately discarded, regardless of the result
    /// of the `Input Rule`. Hence, even if the `Input Rule` return `True` (indicating the `Run`
    /// method will be called), the data **will not** be transmitted.
    pub fn set_action_drop(&mut self) {
        self.action = TokenAction::Drop
    }

    /// Tell Zenoh Flow to use the data in the "next" run and keep it.
    ///
    /// The data associated to the `Token` will be transmitted to the `Run` method of the `Operator`
    /// the next time `Run` is called, i.e. the next time the `Input Rule` returns `True`.
    /// Once `Run` is over, the data is kept, _preventing data from being received on that link_.
    pub fn set_action_keep(&mut self) {
        self.action = TokenAction::Keep
    }

    /// (default) Tell Zenoh Flow to use the data in the "next" run and drop it after.
    ///
    /// The data associated to the `Token` will be transmitted to the `Run` method of the `Operator`
    /// the next time `Run` is called, i.e. the next time the `Input Rule` returns `True`.
    /// Once `Run` is over, the data is dropped, allowing for data to be received on that link.
    pub fn set_action_consume(&mut self) {
        self.action = TokenAction::Consume
    }

    pub fn get_action(&self) -> &TokenAction {
        &self.action
    }

    pub fn get_data_mut(&mut self) -> &mut Data {
        &mut self.data.data
    }

    pub fn get_timestamp(&self) -> &Timestamp {
        &self.data.timestamp
    }

    pub fn get_missed_end_to_end_deadlines(&self) -> &[E2EDeadlineMiss] {
        &self.data.missed_end_to_end_deadlines
    }
}

#[derive(Debug, Clone)]
pub enum Token {
    Pending,
    Ready(ReadyToken),
}

impl Token {
    pub(crate) fn should_drop(&self) -> bool {
        if let Token::Ready(token_ready) = self {
            if let TokenAction::Drop = token_ready.action {
                return true;
            }
        }

        false
    }
}

impl From<DataMessage> for Token {
    fn from(data_message: DataMessage) -> Self {
        Self::Ready(ReadyToken {
            data: data_message,
            action: TokenAction::Consume,
        })
    }
}

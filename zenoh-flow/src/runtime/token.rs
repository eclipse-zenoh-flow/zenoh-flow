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
use crate::{Data, DataMessage, LoopContext, PortId};
use std::collections::HashMap;
use uhlc::Timestamp;

#[derive(Clone)]
pub struct InputTokens {
    pub(crate) map: HashMap<PortId, InputToken>,
}

impl InputTokens {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
        }
    }

    pub fn get_mut(&mut self, port_id: &PortId) -> Option<&mut InputToken> {
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
pub struct DataToken {
    pub(crate) data: DataMessage,
    pub(crate) action: TokenAction,
}

impl DataToken {
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

    pub fn get_loop_contexts(&self) -> &[LoopContext] {
        self.data.get_loop_contexts()
    }
}

#[derive(Debug, Clone)]
pub enum InputToken {
    Pending,
    Ready(DataToken),
}

impl InputToken {
    pub(crate) fn should_drop(&self) -> bool {
        if let InputToken::Ready(data_token) = self {
            if let TokenAction::Drop = data_token.action {
                return true;
            }
        }

        false
    }

    /// Tell Zenoh Flow to immediately discard the data.
    ///
    /// The data associated to the `InputToken` will be immediately discarded, regardless of the
    /// result of the `Input Rule`. Hence, even if the `Input Rule` return `True` (indicating the
    /// `Run` method will be called), the data **will not** be transmitted.
    ///
    /// This method will do nothing if the `InputToken` is a `InputToken::Pending`.
    pub fn set_action_drop(&mut self) {
        if let InputToken::Ready(data_token) = self {
            data_token.action = TokenAction::Drop;
        }
    }

    /// Tell Zenoh Flow to use the data in the "next" run and keep it.
    ///
    /// The data associated to the `InputToken` will be transmitted to the `Run` method of the
    /// `Operator` the next time `Run` is called, i.e. the next time the `Input Rule` returns
    /// `True`. Once `Run` is over, the data is kept, _preventing data from being received on that
    /// link_.
    ///
    /// This method will do nothing if the `InputToken` is a `InputToken::Pending`.
    pub fn set_action_keep(&mut self) {
        if let InputToken::Ready(data_token) = self {
            data_token.action = TokenAction::Keep;
        }
    }

    /// (default) Tell Zenoh Flow to use the data in the "next" run and drop it after.
    ///
    /// The data associated to the `InputToken` will be transmitted to the `Run` method of the
    /// `Operator` the next time `Run` is called, i.e. the next time the `Input Rule` returns
    /// `True`. Once `Run` is over, the data is dropped, allowing for data to be received on that
    /// link.
    ///
    /// This method will do nothing if the `InputToken` is a `InputToken::Pending`.
    pub fn set_action_consume(&mut self) {
        if let InputToken::Ready(data_token) = self {
            data_token.action = TokenAction::Consume;
        }
    }
}

impl From<DataMessage> for InputToken {
    fn from(data_message: DataMessage) -> Self {
        Self::Ready(DataToken {
            data: data_message,
            action: TokenAction::Consume,
        })
    }
}

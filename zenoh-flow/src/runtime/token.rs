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

// use crate::{Data, DataMessage, PortId};
// use std::collections::HashMap;
// use uhlc::Timestamp;

// /// The Tokens provided to input rules of an operator.
// /// Tokens are indexed by [`PortId`](`PortId`)
// /// *NOTE:* Not yet used.
// /// It will be used instead of the `HashMap<PortId,InputToken>` in
// /// `Operator::input_rule` function.
// #[derive(Clone)]
// pub struct InputTokens {
//     pub(crate) map: HashMap<PortId, InputToken>,
// }

// impl InputTokens {
//     /// Creates an empty set of token with the given `capacity`.
//     pub fn with_capacity(capacity: usize) -> Self {
//         Self {
//             map: HashMap::with_capacity(capacity),
//         }
//     }

//     /// Gets a mutable reference to the [`InputToken`](`InputToken`) associated
//     /// to the given `port_id`.
//     pub fn get_mut(&mut self, port_id: &PortId) -> Option<&mut InputToken> {
//         self.map.get_mut(port_id)
//     }
// }

// /// The action that can be executed on a token.
// /// Once the Token is created with some data inside,
// /// different actions could be executed by the input rules.
// ///
// /// - Consume (default) the data will be consumed when run is triggered
// /// - Drop the data will be dropped
// /// - Keep the data will be kept for the current and the next
// /// time the run is triggered, it has to be explicitly set back to `Consume`
// #[derive(Debug, Clone, PartialEq)]
// pub enum TokenAction {
//     Consume,
//     Drop,
//     Keep,
// }

// impl std::fmt::Display for TokenAction {
//     fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
//         match self {
//             Self::Consume => write!(f, "Consume"),
//             Self::Keep => write!(f, "Keep"),
//             Self::Drop => write!(f, "Drop"),
//         }
//     }
// }

// /// The Token containing the data.
// #[derive(Debug, Clone)]
// pub struct DataToken {
//     pub(crate) data: DataMessage,
//     pub(crate) action: TokenAction,
// }

// impl DataToken {
//     /// Creates a new `DataToken` with the given `TokenAction` and `DataMessage`
//     pub fn new(action: TokenAction, data: DataMessage) -> Self {
//         Self { action, data }
//     }

//     /// Gets the current action for this token.
//     pub fn get_action(&self) -> &TokenAction {
//         &self.action
//     }

//     /// Gets a mutable reference to the `Data`.
//     pub fn get_data_mut(&mut self) -> &mut Data {
//         &mut self.data.data
//     }

//     /// Gets the token `Timestamp`.
//     pub fn get_timestamp(&self) -> &Timestamp {
//         &self.data.timestamp
//     }
// }

// /// The token representing the input.
// /// It can be either containing the data or the information the data is
// /// still pending.
// #[derive(Debug, Clone)]
// pub enum InputToken {
//     Pending,
//     Ready(DataToken),
// }

// impl InputToken {
//     /// Checks if the data should be dropped.
//     pub(crate) fn should_drop(&self) -> bool {
//         if let InputToken::Ready(data_token) = self {
//             if let TokenAction::Drop = data_token.action {
//                 return true;
//             }
//         }

//         false
//     }

//     /// Tell Zenoh Flow to immediately discard the data.
//     ///
//     /// The data associated to the `InputToken` will be immediately discarded, regardless of the
//     /// result of the `Input Rule`. Hence, even if the `Input Rule` return `True` (indicating the
//     /// `Run` method will be called), the data **will not** be transmitted.
//     ///
//     /// This method will do nothing if the `InputToken` is a `InputToken::Pending`.
//     pub fn set_action_drop(&mut self) {
//         if let InputToken::Ready(data_token) = self {
//             data_token.action = TokenAction::Drop;
//         }
//     }

//     /// Tell Zenoh Flow to use the data in the "next" run and keep it.
//     ///
//     /// The data associated to the `InputToken` will be transmitted to the `Run` method of the
//     /// `Operator` the next time `Run` is called, i.e. the next time the `Input Rule` returns
//     /// `True`. Once `Run` is over, the data is kept, _preventing data from being received on that
//     /// link_.
//     ///
//     /// This method will do nothing if the `InputToken` is a `InputToken::Pending`.
//     pub fn set_action_keep(&mut self) {
//         if let InputToken::Ready(data_token) = self {
//             data_token.action = TokenAction::Keep;
//         }
//     }

//     /// (default) Tell Zenoh Flow to use the data in the "next" run and drop it after.
//     ///
//     /// The data associated to the `InputToken` will be transmitted to the `Run` method of the
//     /// `Operator` the next time `Run` is called, i.e. the next time the `Input Rule` returns
//     /// `True`. Once `Run` is over, the data is dropped, allowing for data to be received on that
//     /// link.
//     ///
//     /// This method will do nothing if the `InputToken` is a `InputToken::Pending`.
//     pub fn set_action_consume(&mut self) {
//         if let InputToken::Ready(data_token) = self {
//             data_token.action = TokenAction::Consume;
//         }
//     }
// }

// impl From<DataMessage> for InputToken {
//     fn from(data_message: DataMessage) -> Self {
//         Self::Ready(DataToken::new(TokenAction::Consume, data_message))
//     }
// }

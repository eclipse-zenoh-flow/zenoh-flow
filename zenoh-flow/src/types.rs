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

use crate::async_std::sync::Arc;
use crate::runtime::message::{ZFControlMessage, ZFDataMessage, ZFMessage};
use crate::serde::{Deserialize, Serialize};
use crate::{ZFDataTrait, ZFStateTrait};

use std::collections::HashMap;
use std::convert::From;
use uhlc::Timestamp;

// Placeholder types
pub type OperatorId = String;
pub type ZFZenohResource = String;
pub type ZFOperatorName = String;
pub type ZFTimestamp = usize; //TODO: improve it, usize is just a placeholder

pub type ZFPortID = String;
pub type ZFRuntimeID = String;

pub type ZFResult<T> = Result<T, ZFError>;

pub use crate::ZFError;

/// ZFContext is a structure provided by Zenoh Flow to access the execution context directly from
/// the components.
pub struct Context {
    pub mode: usize,
}

impl Default for Context {
    fn default() -> Self {
        Self { mode: 0 }
    }
}

#[derive(Debug, Clone)]
pub enum ZFComponentOutput {
    Data(Arc<dyn ZFDataTrait>),
    // TODO Users should not have access to all control messages. When implementing the control
    // messages change this to an enum with a "limited scope".
    Control(ZFControlMessage),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TokenAction {
    Consume, // Default, data is passed to the run and consumed from the "thing"
    Drop,    // Data is dropped
    KeepRun, // Data is passed to the run and kept in the "thing"
    Keep,    // Data is kept in the "thing"
    Wait,    //Waits the Data, this is applicable only to NotReadyToken
}

#[derive(Debug, Clone, Default)]
pub struct NotReadyToken;

#[derive(Debug, Clone)]
pub struct ReadyToken {
    pub data: ZFDataMessage,
    pub action: TokenAction,
}

#[derive(Debug, Clone)]
pub enum Token {
    NotReady,
    Ready(ReadyToken),
}

impl Token {
    pub fn new_ready(data: ZFDataMessage) -> Self {
        Self::Ready(ReadyToken {
            data,
            action: TokenAction::Consume,
        })
    }

    pub fn get_timestamp(&self) -> Option<Timestamp> {
        match self {
            Self::NotReady => None,
            Self::Ready(token) => Some(token.data.timestamp),
        }
    }

    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready(_))
    }

    pub fn is_not_ready(&self) -> bool {
        matches!(self, Self::NotReady)
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

    pub fn data(&self) -> ZFResult<ZFDataMessage> {
        match self {
            Self::Ready(ready) => Ok(ready.data.clone()),
            _ => Err(ZFError::GenericError),
        }
    }

    pub fn action(&self) -> &TokenAction {
        match self {
            Self::Ready(ready) => &ready.action,
            Self::NotReady => &TokenAction::Wait,
        }
    }

    pub fn split(self) -> (Option<ZFDataMessage>, TokenAction) {
        match self {
            Self::Ready(ready) => (Some(ready.data), ready.action),
            Self::NotReady => (None, TokenAction::Wait),
        }
    }
}

impl From<Arc<ZFMessage>> for Token {
    fn from(message: Arc<ZFMessage>) -> Self {
        match message.as_ref() {
            ZFMessage::Control(_) => Token::NotReady,
            ZFMessage::Data(data_message) => Token::new_ready(data_message.clone()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ZFInput(HashMap<String, ZFDataMessage>);

impl Default for ZFInput {
    fn default() -> Self {
        Self::new()
    }
}

impl ZFInput {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn insert(&mut self, id: String, data: ZFDataMessage) -> Option<ZFDataMessage> {
        self.0.insert(id, data)
    }

    pub fn get(&self, id: &str) -> Option<&ZFDataMessage> {
        self.0.get(id)
    }

    pub fn get_mut(&mut self, id: &str) -> Option<&mut ZFDataMessage> {
        self.0.get_mut(id)
    }
}

impl<'a> IntoIterator for &'a ZFInput {
    type Item = (&'a String, &'a ZFDataMessage);
    type IntoIter = std::collections::hash_map::Iter<'a, String, ZFDataMessage>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[derive(Debug, Clone)]
pub struct EmptyState;

impl ZFStateTrait for EmptyState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub fn default_output_rule(
    _state: &mut Box<dyn ZFStateTrait>,
    outputs: &HashMap<String, Arc<dyn ZFDataTrait>>,
) -> ZFResult<HashMap<ZFPortID, ZFComponentOutput>> {
    let mut results = HashMap::with_capacity(outputs.len());
    for (k, v) in outputs {
        results.insert(k.clone(), ZFComponentOutput::Data(v.clone()));
    }

    Ok(results)
}

pub fn default_input_rule(
    _state: &mut Box<dyn ZFStateTrait>,
    tokens: &mut HashMap<String, Token>,
) -> ZFResult<bool> {
    for token in tokens.values() {
        match token {
            Token::Ready(_) => continue,
            Token::NotReady => return Ok(false),
        }
    }

    Ok(true)
}

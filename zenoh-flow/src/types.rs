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
use crate::runtime::message::{ControlMessage, DataMessage, Message};
use crate::serde::{Deserialize, Serialize};
use crate::{Data, State};

use std::collections::HashMap;
use std::convert::From;
use uhlc::Timestamp;

pub type OperatorId = Arc<str>;
pub type PortId = Arc<str>;
pub type RuntimeId = Arc<str>;

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
pub enum ComponentOutput {
    Data(Arc<dyn Data>),
    // TODO Users should not have access to all control messages. When implementing the control
    // messages change this to an enum with a "limited scope".
    Control(ControlMessage),
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
    pub data: DataMessage,
    pub action: TokenAction,
}

#[derive(Debug, Clone)]
pub enum Token {
    NotReady,
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

    pub fn data(&self) -> ZFResult<DataMessage> {
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

    pub fn split(self) -> (Option<DataMessage>, TokenAction) {
        match self {
            Self::Ready(ready) => (Some(ready.data), ready.action),
            Self::NotReady => (None, TokenAction::Wait),
        }
    }
}

impl From<Arc<Message>> for Token {
    fn from(message: Arc<Message>) -> Self {
        match message.as_ref() {
            Message::Control(_) => Token::NotReady,
            Message::Data(data_message) => Token::new_ready(data_message.clone()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ZFInput(HashMap<String, DataMessage>);

impl Default for ZFInput {
    fn default() -> Self {
        Self::new()
    }
}

impl ZFInput {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn insert(&mut self, id: String, data: DataMessage) -> Option<DataMessage> {
        self.0.insert(id, data)
    }

    pub fn get(&self, id: &str) -> Option<&DataMessage> {
        self.0.get(id)
    }

    pub fn get_mut(&mut self, id: &str) -> Option<&mut DataMessage> {
        self.0.get_mut(id)
    }
}

impl<'a> IntoIterator for &'a ZFInput {
    type Item = (&'a String, &'a DataMessage);
    type IntoIter = std::collections::hash_map::Iter<'a, String, DataMessage>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[derive(Debug, Clone)]
pub struct EmptyState;

impl State for EmptyState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

pub fn default_output_rule(
    _state: &mut Box<dyn State>,
    outputs: HashMap<PortId, Arc<dyn Data>>,
) -> ZFResult<HashMap<PortId, ComponentOutput>> {
    let mut results = HashMap::with_capacity(outputs.len());
    for (k, v) in outputs {
        results.insert(k, ComponentOutput::Data(v));
    }

    Ok(results)
}

pub fn default_input_rule(
    _state: &mut Box<dyn State>,
    tokens: &mut HashMap<PortId, Token>,
) -> ZFResult<bool> {
    for token in tokens.values() {
        match token {
            Token::Ready(_) => continue,
            Token::NotReady => return Ok(false),
        }
    }

    Ok(true)
}

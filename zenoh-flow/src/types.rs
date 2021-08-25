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
pub type ZFOperatorId = String;
pub type ZFZenohResource = String;
pub type ZFOperatorName = String;
pub type ZFTimestamp = usize; //TODO: improve it, usize is just a placeholder

pub type ZFPortID = String;
pub type ZFRuntimeID = String;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum ZFError {
    GenericError,
    SerializationError,
    DeseralizationError,
    MissingState,
    InvalidState,
    Unimplemented,
    Empty,
    MissingConfiguration,
    VersionMismatch,
    Disconnected,
    Uncompleted(String),
    PortTypeNotMatching((String, String)),
    OperatorNotFound(ZFOperatorId),
    PortNotFound((ZFOperatorId, String)),
    RecvError(String),
    SendError(String),
    MissingInput(String),
    MissingOutput(String),
    InvalidData(String),
    IOError(String),
    ZenohError(String),
    LoadingError(String),
    ParsingError(String),
    RunnerStopError(crate::async_std::channel::RecvError),
    RunnerStopSendError(crate::async_std::channel::SendError<()>),
}

impl From<flume::RecvError> for ZFError {
    fn from(err: flume::RecvError) -> Self {
        Self::RecvError(format!("{:?}", err))
    }
}

impl From<crate::async_std::channel::RecvError> for ZFError {
    fn from(err: async_std::channel::RecvError) -> Self {
        Self::RunnerStopError(err)
    }
}

impl From<crate::async_std::channel::SendError<()>> for ZFError {
    fn from(err: async_std::channel::SendError<()>) -> Self {
        Self::RunnerStopSendError(err)
    }
}

impl From<flume::TryRecvError> for ZFError {
    fn from(err: flume::TryRecvError) -> Self {
        match err {
            flume::TryRecvError::Disconnected => Self::Disconnected,
            flume::TryRecvError::Empty => Self::Empty,
        }
    }
}

impl<T> From<flume::SendError<T>> for ZFError {
    fn from(err: flume::SendError<T>) -> Self {
        Self::SendError(format!("{:?}", err))
    }
}

impl From<std::io::Error> for ZFError {
    fn from(err: std::io::Error) -> Self {
        Self::IOError(format!("{}", err))
    }
}

impl From<zenoh_util::core::ZError> for ZFError {
    fn from(err: zenoh_util::core::ZError) -> Self {
        Self::ZenohError(format!("{}", err))
    }
}

impl From<libloading::Error> for ZFError {
    fn from(err: libloading::Error) -> Self {
        Self::LoadingError(format!("Error when loading the library: {}", err))
    }
}

#[cfg(feature = "data_json")]
impl From<serde_json::Error> for ZFError {
    fn from(_err: serde_json::Error) -> Self {
        Self::SerializationError
    }
}

#[cfg(feature = "data_json")]
impl From<std::str::Utf8Error> for ZFError {
    fn from(_err: std::str::Utf8Error) -> Self {
        Self::SerializationError
    }
}

pub struct ZFInnerCtx {
    pub state: Box<dyn StateTrait>,
    pub mode: usize, //can be arc<atomic> and inside ZFContext
}

#[derive(Clone)]
pub struct ZFContext(Arc<Mutex<ZFInnerCtx>>); //TODO: have only state inside Mutex

impl ZFContext {
    pub fn new(state: Box<dyn StateTrait>, mode: usize) -> Self {
        let inner = Arc::new(Mutex::new(ZFInnerCtx { state, mode }));
        Self(inner)
    }

    pub async fn async_lock(&'_ self) -> MutexGuard<'_, ZFInnerCtx> {
        self.0.lock().await
    }

    pub fn lock(&self) -> MutexGuard<ZFInnerCtx> {
        crate::zf_spin_lock!(self.0)
    }
}

pub type ZFResult<T> = Result<T, ZFError>;

/// ZFContext is a structure provided by Zenoh Flow to access the execution context directly from
/// the components.
pub struct ZFContext {
    pub mode: usize,
}

impl Default for ZFContext {
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

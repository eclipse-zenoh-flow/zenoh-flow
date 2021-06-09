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

use serde::{Deserialize, Serialize};

use crate::message::{Message, ZFMessage};
use crate::operator::{DataTrait, StateTrait};

use async_std::sync::Arc;
use std::any::Any;
use std::collections::HashMap;

// Placeholder types
pub type ZFOperatorId = String;
pub type ZFTimestamp = u128;
pub type ZFLinkId = u128;

#[derive(Debug)]
pub enum ZFError {
    GenericError,
    SerializationError,
    DeseralizationError,
    RecvError(flume::RecvError),
    SendError(String),
    MissingInput(ZFLinkId),
    InvalidData(ZFLinkId),
}

impl From<flume::RecvError> for ZFError {
    fn from(err: flume::RecvError) -> Self {
        Self::RecvError(err)
    }
}

impl<T> From<flume::SendError<T>> for ZFError {
    fn from(err: flume::SendError<T>) -> Self {
        Self::SendError(format!("{:?}", err))
    }
}

pub struct ZFContext {
    // pub state: Arc<Mutex<dyn StateTrait>>,
    pub state: Option<Box<dyn StateTrait>>,
    pub mode: u128,
}

impl ZFContext {
    pub fn set_state(&mut self, state: Box<dyn StateTrait>) {
        self.state = Some(state);
    }
    // get returns not mutable
    pub fn get_state(&mut self) -> Box<dyn StateTrait> {
        self.state.take().unwrap()
    }

    //take returns mutable
}

pub type ZFResult<T> = Result<T, ZFError>;

// Maybe TokenActions should be always sent back to the OperatorRunner,
// to allow it the management of the data in the links.
pub enum OperatorResult {
    InResult(Result<(bool, HashMap<ZFLinkId, TokenAction>), ZFError>),
    RunResult(ZFError), // This may be just ZFError
    OutResult(Result<HashMap<ZFLinkId, ZFMessage>, ZFError>),
}

pub type OperatorRun = dyn Fn(&mut ZFContext, &HashMap<ZFLinkId, Option<Arc<ZFMessage>>>) -> OperatorResult
    + Send
    + Sync
    + 'static;

pub type ZFSourceResult = Result<Vec<Message>, ZFError>;
pub type ZFSourceRun = dyn Fn(&mut ZFContext) -> ZFSourceResult + Send + Sync + 'static; // This should be a future, Sources can do I/O

pub type ZFSinkResult = Result<(), ZFError>;
pub type ZFSinkRun =
    dyn Fn(&mut ZFContext, Vec<&ZFMessage>) -> ZFSinkResult + Send + Sync + 'static; // This should be a future, Sinks can do I/O

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "UPPERCASE")]
pub enum ZFOperatorKind {
    Source,
    Sink,
    Compute,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFSinkDescription {
    pub id: ZFOperatorId,
    pub name: String,
    pub inputs: Vec<ZFLinkId>,
    pub lib: String,
}

impl std::fmt::Display for ZFSinkDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Sink", self.id)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFSourceDescription {
    pub id: ZFOperatorId,
    pub name: String,
    pub outputs: Vec<ZFLinkId>,
    pub lib: String,
}

impl std::fmt::Display for ZFSourceDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Source", self.id)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFOperatorDescription {
    pub id: ZFOperatorId,
    pub name: String,
    pub inputs: Vec<ZFLinkId>,
    pub outputs: Vec<ZFLinkId>,
    pub lib: String,
}

impl std::fmt::Display for ZFOperatorDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Operator", self.id)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFConnection {
    pub from: (ZFOperatorId, ZFLinkId),
    pub to: (ZFOperatorId, ZFLinkId),
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
pub struct NotReadyToken {
    pub ts: ZFTimestamp,
}

impl NotReadyToken {
    /// Creates a `NotReadyToken` with its timestamp set to 0.
    pub fn new() -> Self {
        NotReadyToken { ts: 0 }
    }
}

#[derive(Debug, Clone)]
pub struct ReadyToken {
    pub ts: ZFTimestamp,
    pub data: Arc<dyn DataTrait>,
    pub action: TokenAction,
}

#[derive(Debug, Clone)]
pub enum Token {
    NotReady(NotReadyToken),
    Ready(ReadyToken),
}

impl Token {
    pub fn new_ready(ts: ZFTimestamp, data: Arc<dyn DataTrait>) -> Self {
        Self::Ready(ReadyToken {
            ts,
            data,
            action: TokenAction::Consume,
        })
    }

    pub fn new_not_ready(ts: ZFTimestamp) -> Self {
        Self::NotReady(NotReadyToken { ts })
    }

    pub fn is_ready(&self) -> bool {
        match self {
            Self::Ready(_) => true,
            _ => false,
        }
    }

    pub fn is_not_ready(&self) -> bool {
        match self {
            Self::NotReady(_) => true,
            _ => false,
        }
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

    pub fn data(&self) -> ZFResult<Arc<dyn DataTrait>> {
        match self {
            Self::Ready(ready) => Ok(ready.data.clone()),
            _ => Err(ZFError::GenericError),
        }
    }

    pub fn action<'a>(&'a self) -> &'a TokenAction {
        match self {
            Self::Ready(ready) => &ready.action,
            Self::NotReady(_) => &TokenAction::Wait,
        }
    }

    pub fn split(self) -> (Option<Arc<dyn DataTrait>>, TokenAction) {
        match self {
            Self::Ready(ready) => (Some(ready.data), ready.action),
            Self::NotReady(_) => (None, TokenAction::Wait),
        }
    }
}
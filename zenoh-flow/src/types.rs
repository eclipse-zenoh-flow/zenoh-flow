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

use crate::async_std::sync::{Arc, Mutex, MutexGuard};
use crate::operator::{DataTrait, StateTrait};
use crate::runtime::message::{ZFDataMessage, ZFMessage, ZFMsg};
use crate::serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::From;

// Placeholder types
pub type ZFOperatorId = String;
pub type ZFZenohResource = String;
pub type ZFOperatorName = String;
pub type ZFTimestamp = usize; //TODO: improve it, usize is just a placeholder
pub type ZFLinkId = String; // TODO: improve it, String is just a placeholder
pub type ZFRuntimeID = String;

#[derive(Debug)]
pub enum ZFError {
    GenericError,
    SerializationError,
    DeseralizationError,
    MissingState,
    InvalidState,
    Unimplemented,
    PortIdNotMatching((ZFLinkId, ZFLinkId)),
    OperatorNotFound(ZFOperatorName),
    PortNotFound((ZFOperatorName, ZFLinkId)),
    RecvError(flume::RecvError),
    SendError(String),
    MissingInput(ZFLinkId),
    InvalidData(ZFLinkId),
    Disconnected,
    Empty,
    IOError(std::io::Error),
    MissingConfiguration,
    VersionMismatch,
    ZenohError(zenoh_util::core::ZError),
}

impl From<flume::RecvError> for ZFError {
    fn from(err: flume::RecvError) -> Self {
        Self::RecvError(err)
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
        Self::IOError(err)
    }
}

impl From<zenoh_util::core::ZError> for ZFError {
    fn from(err: zenoh_util::core::ZError) -> Self {
        Self::ZenohError(err)
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
        let inner = Arc::new(Mutex::new(ZFInnerCtx {
            mode: mode,
            state: state,
        }));
        Self(inner)
    }

    pub async fn async_lock<'a>(&'a self) -> MutexGuard<'a, ZFInnerCtx> {
        self.0.lock().await
    }

    pub fn lock<'a>(&'a self) -> MutexGuard<'a, ZFInnerCtx> {
        crate::zf_spin_lock!(self.0) // should not use this, should have an async lock and a sync "lock"
    }

    // pub fn set_state(&mut self, state: Box<dyn StateTrait>) {
    //     self.state = Some(state);
    // }

    // pub fn take_state(&mut self) -> Option<Box<dyn StateTrait>> {
    //     self.state.take()
    // }

    // pub fn get_state(&self) -> Option<&Box<dyn StateTrait>> {
    //     self.state.as_ref()
    // }

    // pub fn get_state(&self) -> & {
    //     self.state.as_ref().unwrap() //getting state
    //     // crate::downcast!(T, self.state.as_ref().unwrap()).unwrap() //downcasting to right type

    // }
}

pub type ZFResult<T> = Result<T, ZFError>;

// Maybe TokenActions should be always sent back to the OperatorRunner,
// to allow it the management of the data in the links.

// TODO: remove
pub enum OperatorResult {
    InResult(Result<(bool, HashMap<ZFLinkId, TokenAction>), ZFError>),
    RunResult(ZFError), // This may be just ZFError
    OutResult(Result<HashMap<ZFLinkId, ZFMessage>, ZFError>),
}

// TODO: remove
pub type OperatorRun = dyn Fn(&mut ZFContext, &HashMap<ZFLinkId, Option<Arc<ZFMessage>>>) -> OperatorResult
    + Send
    + Sync
    + 'static;

// TODO: move to operator.rs
pub type ZFSourceResult = Result<Vec<ZFDataMessage>, ZFError>;
//TODO: move to operator.rs
pub type ZFSourceRun = dyn Fn(&mut ZFContext) -> ZFSourceResult + Send + Sync + 'static; // This should be a future, Sources can do I/O

// TODO: move to operator.rs
pub type ZFSinkResult = Result<(), ZFError>;

// TODO: move to operator.rs
pub type ZFSinkRun =
    dyn Fn(&mut ZFContext, Vec<&ZFMessage>) -> ZFSinkResult + Send + Sync + 'static; // This should be a future, Sinks can do I/O

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFZenohSenderDescription {
    pub id: ZFOperatorId,
    pub name: ZFOperatorName,
    pub input: ZFLinkId,
    pub resource: ZFZenohResource,
    pub uri: String,
    pub runtime: Option<String>,
}

impl std::fmt::Display for ZFZenohSenderDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Sender", self.name)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFZenohReceiverDescription {
    pub id: ZFOperatorId,
    pub name: ZFOperatorName,
    pub output: ZFLinkId,
    pub resource: ZFZenohResource,
    pub uri: String,
    pub runtime: Option<String>,
}

impl std::fmt::Display for ZFZenohReceiverDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - Kind: Receiver", self.name)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ZFZenohConnectorDescription {
    Sender(ZFZenohSenderDescription),
    Receiver(ZFZenohReceiverDescription),
}

impl std::fmt::Display for ZFZenohConnectorDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ZFZenohConnectorDescription::Receiver(rx) => write!(f, "{}", rx),
            ZFZenohConnectorDescription::Sender(tx) => write!(f, "{}", tx),
        }
    }
}

impl ZFZenohConnectorDescription {
    pub fn get_id(&self) -> ZFOperatorId {
        match self {
            ZFZenohConnectorDescription::Receiver(rx) => rx.id.clone(),
            ZFZenohConnectorDescription::Sender(tx) => tx.id.clone(),
        }
    }

    pub fn get_name(&self) -> ZFOperatorName {
        match self {
            ZFZenohConnectorDescription::Receiver(rx) => rx.name.clone(),
            ZFZenohConnectorDescription::Sender(tx) => tx.name.clone(),
        }
    }

    pub fn get_runtime(&self) -> Option<String> {
        match self {
            ZFZenohConnectorDescription::Sender(tx) => tx.runtime.clone(),
            ZFZenohConnectorDescription::Receiver(rx) => rx.runtime.clone(),
        }
    }
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
pub enum TokenData {
    Serialized(Arc<Vec<u8>>),
    Deserialized(Arc<dyn DataTrait>),
}

impl TokenData {
    pub fn get_data(self) -> ZFResult<Arc<dyn DataTrait>> {
        match self {
            Self::Serialized(_) => Err(ZFError::GenericError),
            Self::Deserialized(de) => Ok(de),
        }
    }

    pub fn get_raw(self) -> ZFResult<Arc<Vec<u8>>> {
        match self {
            Self::Serialized(ser) => Ok(ser),
            Self::Deserialized(_) => Err(ZFError::GenericError),
        }
    }
}

//TODO: improve
#[derive(Debug, Clone)]
pub struct ReadyToken {
    pub ts: ZFTimestamp,
    pub data: TokenData,
    pub action: TokenAction,
}

#[derive(Debug, Clone)]
pub enum Token {
    NotReady(NotReadyToken),
    Ready(ReadyToken),
}

impl Token {
    pub fn new_ready(ts: ZFTimestamp, data: TokenData) -> Self {
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

    pub fn data(&self) -> ZFResult<TokenData> {
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

    pub fn split(self) -> (Option<TokenData>, TokenAction) {
        match self {
            Self::Ready(ready) => (Some(ready.data), ready.action),
            Self::NotReady(_) => (None, TokenAction::Wait),
        }
    }
}

impl From<Arc<ZFMessage>> for Token {
    fn from(msg: Arc<ZFMessage>) -> Self {
        match &msg.msg {
            ZFMsg::Ctrl(_) => Token::NotReady(NotReadyToken { ts: msg.ts }),
            ZFMsg::Data(data_msg) => match data_msg {
                ZFDataMessage::Serialized(ser) => Token::Ready(ReadyToken {
                    ts: msg.ts,
                    action: TokenAction::Consume,
                    data: TokenData::Serialized(ser.clone()), //Use ZBuf
                }),
                ZFDataMessage::Deserialized(de) => Token::Ready(ReadyToken {
                    ts: msg.ts,
                    action: TokenAction::Consume,
                    data: TokenData::Deserialized(de.clone()),
                }),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub enum ZFData {
    Serialized(Arc<Vec<u8>>),
    Deserialized(Arc<dyn DataTrait>),
}

impl ZFData {
    pub fn get_serialized(&self) -> &Arc<Vec<u8>> {
        match self {
            Self::Serialized(ser) => ser,
            Self::Deserialized(_) => panic!(),
        }
    }

    pub fn get_deserialized(&self) -> &Arc<dyn DataTrait> {
        match self {
            Self::Deserialized(de) => de,
            Self::Serialized(_) => panic!(),
        }
    }
}

impl From<TokenData> for ZFData {
    fn from(d: TokenData) -> Self {
        match d {
            TokenData::Serialized(ser) => ZFData::Serialized(ser),
            TokenData::Deserialized(de) => ZFData::Deserialized(de),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ZFInput(HashMap<ZFLinkId, ZFData>);

impl ZFInput {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn insert(&mut self, id: ZFLinkId, data: ZFData) -> Option<ZFData> {
        self.0.insert(id, data)
    }

    pub fn get(&self, id: &ZFLinkId) -> Option<&ZFData> {
        self.0.get(id)
    }

    pub fn get_mut(&mut self, id: &ZFLinkId) -> Option<&mut ZFData> {
        self.0.get_mut(id)
    }
}

impl<'a> IntoIterator for &'a ZFInput {
    type Item = (&'a ZFLinkId, &'a ZFData);
    type IntoIter = std::collections::hash_map::Iter<'a, ZFLinkId, ZFData>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EmptyState;

#[typetag::serde]
impl crate::operator::StateTrait for EmptyState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

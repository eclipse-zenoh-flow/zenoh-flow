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

use crate::message::{Message, ZFCtrlMessage};

pub type ZFOperatorId = String;
pub type ZFTimestamp = u128;

pub enum InputRuleResult {
    Consume,
    Drop,
    Ignore, //To be removed
    Wait,
}

pub enum ZFError {
    GenericError,
    SerializationError,
    DeseralizationError,
}

pub struct ZFContext {
    pub state: Vec<u8>,
    pub mode: u8,
}

pub enum OperatorResult {
    InResult(Result<(bool, Vec<InputRuleResult>), ZFError>),
    RunResult(Result<(), ZFError>),
    OutResult(Result<(Vec<Message>, Vec<Option<ZFCtrlMessage>>), ZFError>), // Data, Control
}

pub type OperatorRun =
    dyn Fn(&mut ZFContext, Vec<Option<&Message>>) -> OperatorResult + Send + Sync + 'static;

pub type ZFSourceResult = Result<Vec<Message>, ZFError>;
pub type ZFSourceRun = dyn Fn(&mut ZFContext) -> ZFSourceResult + Send + Sync + 'static; // This should be a future, Sources can do I/O

pub type ZFSinkResult = Result<(), ZFError>;
pub type ZFSinkRun =
    dyn Fn(&mut ZFContext, Vec<Option<&Message>>) -> ZFSinkResult + Send + Sync + 'static; // This should be a future, Sinks can do I/O

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ZFOperatorKind {
    Source,
    Sink,
    Compute,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFOperatorDescription {
    pub id: ZFOperatorId,
    pub name: String,
    pub inputs: u8,
    pub outputs: u8,
    pub lib: String,
    pub kind: ZFOperatorKind,
}

impl std::fmt::Display for ZFOperatorDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.id)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ZFOperatorConnection {
    pub from: ZFOperatorId,
    pub to: ZFOperatorId,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataFlowDescription {
    pub operators: Vec<ZFOperatorDescription>,
    pub connections: Vec<ZFOperatorConnection>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TokenAction {
    Consume, //Default, data is passed to the run and consumed from the "thing"
    Drop, //Data is dropped
    KeepRun, //Data is passed to the run and kept in the "thing"
    Keep, //Data is kept in the "thing"
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NotReadyToken {
    pub ts : ZFTimestamp
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReadyToken<T> {
    pub ts : ZFTimestamp,
    pub data : T,
    pub action : TokenAction,
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Token<T> {
    NotReady(NotReadyToken),
    Ready(ReadyToken<T>),
}


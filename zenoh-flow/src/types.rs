use serde::{Deserialize, Serialize};

use crate::message::{Message, ZFCtrlMessage};

pub type ZFOperatorId = String;

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

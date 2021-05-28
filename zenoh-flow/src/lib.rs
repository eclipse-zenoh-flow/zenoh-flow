pub use ::bincode;
pub use ::paste;
pub use ::serde;

pub mod message;
pub mod stream;

use message::{Message, ZFCtrlMessage};

pub enum InputRuleResult {
    Process,
    Drop,
    Ignore,
    Wait,
}

pub enum ZFError {
    GenericError,
    SerializationError,
    DeseralizationError,
}

pub struct ZFContext {
    pub state: Vec<u8>,
    pub mode: String,
}

pub enum OperatorResult {
    InResult(Result<(bool, Vec<InputRuleResult>), ZFError>),
    RunResult(Result<(), ZFError>),
    OutResult(Result<(Vec<Message>, Vec<ZFCtrlMessage>), ZFError>), // Data, Control
}

pub type OperatorRun =
    dyn Fn(&mut ZFContext, Vec<Option<&Message>>) -> OperatorResult + Send + Sync + 'static; // maybe this can be async?

/// This trait will be implemented by the zfoperator proc_macro
pub trait ZFOperator {
    fn make_run(&self, ctx: &mut ZFContext) -> Box<OperatorRun>;
}

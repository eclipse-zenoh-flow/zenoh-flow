pub use ::bincode;
pub use ::paste;
pub use ::serde;

pub mod loader;
pub mod message;
pub mod stream;

use message::{Message, ZFCtrlMessage};

pub type ZFOperatorId = String;

pub enum InputRuleResult {
    Consume,
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
    pub mode: u8,
}

pub enum OperatorResult {
    InResult(Result<(bool, Vec<InputRuleResult>), ZFError>),
    RunResult(Result<(), ZFError>),
    OutResult(Result<(Vec<Message>, Vec<Option<ZFCtrlMessage>>), ZFError>), // Data, Control
}

pub type OperatorRun =
    dyn Fn(&mut ZFContext, Vec<Option<&Message>>) -> OperatorResult + Send + Sync + 'static; // maybe this can be async?

/// This trait will be implemented by the zfoperator proc_macro
pub trait ZFOperator {
    fn make_run(&self, ctx: &mut ZFContext) -> Box<OperatorRun>;
}

pub type ZFSourceResult = Result<Vec<Message>, ZFError>;
pub type ZFSourceRun = dyn Fn(&mut ZFContext) -> ZFSourceResult + Send + Sync + 'static;

pub trait ZFSource {
    fn make_source(&self, ctx: &mut ZFContext) -> Box<ZFSourceRun>;
}

pub type ZFSinkResult = Result<(), ZFError>;
pub type ZFSinkRun =
    dyn Fn(&mut ZFContext, Vec<Option<&Message>>) -> ZFSinkResult + Send + Sync + 'static;

pub trait ZFSink {
    fn make_sink(&self, ctx: &mut ZFContext) -> Box<ZFSinkRun>;
}

#[macro_export]
macro_rules! export_operator {
    ($register:expr) => {
        #[doc(hidden)]
        #[no_mangle]
        pub static zfoperator_declaration: $crate::loader::ZFOperatorDeclaration =
            $crate::loader::ZFOperatorDeclaration {
                rustc_version: $crate::loader::RUSTC_VERSION,
                core_version: $crate::loader::CORE_VERSION,
                register: $register,
            };
    };
}

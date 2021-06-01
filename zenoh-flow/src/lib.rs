pub use ::bincode;
pub use ::paste;
pub use ::serde;

pub mod graph;
pub mod loader;
pub mod message;
pub mod stream;

pub mod types;
pub use types::*;

/// This trait will be implemented by the zfoperator proc_macro
pub trait ZFOperator {
    fn make_run(&self, ctx: &mut ZFContext) -> Box<OperatorRun>;
}

pub trait ZFSource {
    fn make_source(&self, ctx: &mut ZFContext) -> Box<ZFSourceRun>;
}

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

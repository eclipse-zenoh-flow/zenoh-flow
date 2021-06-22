use crate::async_std::sync::Arc;
use crate::message::ZFMessage;
use crate::types::{Token, ZFContext, ZFLinkId, ZFResult};
use futures::Future;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::pin::Pin;

//Create a Derive macro for this
#[typetag::serde(tag = "zf_data_type", content = "value")]
pub trait DataTrait: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
}

//Create a Derive macro for this
#[typetag::serde(tag = "zf_state_type", content = "value")]
pub trait StateTrait: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
}

//Create a Derive macro for this??
pub trait OperatorMode: Into<u128> + From<u128> {}

pub type InputRuleResult = ZFResult<bool>;

// CAUTION, USER CAN DO NASTY THINGS, eg. remove a link we have passed to him.
pub type FnInputRule =
    dyn Fn(ZFContext, &mut HashMap<ZFLinkId, Token>) -> InputRuleResult + Send + Sync + 'static;

pub type OutputRuleResult = ZFResult<HashMap<ZFLinkId, Arc<ZFMessage>>>;

pub type FnOutputRule = dyn Fn(ZFContext, HashMap<ZFLinkId, Arc<Box<dyn DataTrait>>>) -> OutputRuleResult
    + Send
    + Sync
    + 'static;

pub type RunResult = ZFResult<HashMap<ZFLinkId, Arc<Box<dyn DataTrait>>>>;

pub type FnRun = dyn Fn(ZFContext, HashMap<ZFLinkId, Arc<Box<dyn DataTrait>>>) -> RunResult
    + Send
    + Sync
    + 'static;

pub trait OperatorTrait {
    fn get_input_rule(&self, ctx: ZFContext) -> Box<FnInputRule>;

    fn get_output_rule(&self, ctx: ZFContext) -> Box<FnOutputRule>;

    fn get_run(&self, ctx: ZFContext) -> Box<FnRun>;

    fn get_state(&self) -> Box<dyn StateTrait>;
}

pub type FutRunResult = Pin<Box<dyn Future<Output = RunResult> + Send + Sync>>;

pub type FnSourceRun = Box<dyn Fn(ZFContext) -> FutRunResult + Send + Sync>;

pub trait SourceTrait {
    fn get_run(&self, ctx: ZFContext) -> FnSourceRun;

    // a get output_rule

    fn get_state(&self) -> Box<dyn StateTrait>;
}

pub type FutSinkResult = Pin<Box<dyn Future<Output = ZFResult<()>> + Send + Sync>>;

pub type FnSinkRun = Box<
    dyn Fn(ZFContext, HashMap<ZFLinkId, Arc<Box<dyn DataTrait>>>) -> FutSinkResult + Send + Sync,
>;

pub trait SinkTrait {
    fn get_input_rule(&self, ctx: ZFContext) -> Box<FnInputRule>;

    fn get_run(&self, ctx: ZFContext) -> FnSinkRun;

    fn get_state(&self) -> Box<dyn StateTrait>;
}

#[macro_export]
macro_rules! downcast {
    ($ident : ident, $val : expr) => {
        $val.as_any().downcast_ref::<$ident>()
    };
}

#[macro_export]
macro_rules! downcast_mut {
    ($ident : ident, $val : expr) => {
        $val.as_mut_any().downcast_mut::<$ident>()
    };
}

#[macro_export]
macro_rules! take_state {
    ($ident : ident, $ctx : expr) => {
        match $ctx.take_state() {
            Some(mut state) => match zenoh_flow::downcast_mut!($ident, state) {
                Some(mut data) => Ok((state, data)),
                None => Err(zenoh_flow::types::ZFError::InvalidState),
            },
            None => Err(zenoh_flow::types::ZFError::MissingState),
        }
    };
}

#[macro_export]
macro_rules! get_state {
    ($ident : ident, $ctx : expr) => {
        match $ctx.get_state() {
            Some(state) => match zenoh_flow::downcast!($ident, state) {
                Some(data) => Ok((state, data)),
                None => Err(zenoh_flow::types::ZFError::InvalidState),
            },
            None => Err(zenoh_flow::types::ZFError::MissingState),
        }
    };
}

#[macro_export]
macro_rules! get_input {
    ($ident : ident, $index : expr, $map : expr) => {
        match $map.get(&$index) {
            Some(d) => match zenoh_flow::downcast!($ident, d) {
                Some(data) => Ok(data),
                None => Err(zenoh_flow::types::ZFError::InvalidData($index)),
            },
            None => Err(zenoh_flow::types::ZFError::MissingInput($index)),
        }
    };
}

#[macro_export]
macro_rules! zf_empty_state {
    () => {
        Box::new(zenoh_flow::EmptyState {})
    };
}

#[macro_export]
macro_rules! zf_source_result {
    ($result : expr) => {
        Box::pin($result)
    };
}

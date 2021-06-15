use crate::message::ZFMessage;
use crate::types::{Token, ZFContext, ZFLinkId, ZFResult};
use async_std::sync::Arc;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;

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
pub type FnInputRule = dyn Fn(&mut ZFContext, &mut HashMap<ZFLinkId, Token>) -> InputRuleResult
    + Send
    + Sync
    + 'static;

pub type OutputRuleResult = ZFResult<HashMap<ZFLinkId, Arc<ZFMessage>>>;

pub type FnOutputRule = dyn Fn(&mut ZFContext, HashMap<ZFLinkId, Arc<Box<dyn DataTrait>>>) -> OutputRuleResult
    + Send
    + Sync
    + 'static;

pub type RunResult = ZFResult<HashMap<ZFLinkId, Arc<Box<dyn DataTrait>>>>;

pub type FnRun = dyn Fn(&mut ZFContext, HashMap<ZFLinkId, Arc<Box<dyn DataTrait>>>) -> RunResult
    + Send
    + Sync
    + 'static;

pub trait OperatorTrait {
    fn get_input_rule(&self, ctx: &ZFContext) -> Box<FnInputRule>;

    fn get_output_rule(&self, ctx: &ZFContext) -> Box<FnOutputRule>;

    fn get_run(&self, ctx: &ZFContext) -> Box<FnRun>;

    fn get_state(&self) -> Option<Box<dyn StateTrait>>;
}

pub type FnSourceRun = dyn Fn(&mut ZFContext) -> RunResult + Send + Sync + 'static;

pub trait SourceTrait {
    fn get_run(&self, ctx: &ZFContext) -> Box<FnSourceRun>;

    fn get_state(&self) -> Option<Box<dyn StateTrait>>;
}

pub type FnSinkRun =
    dyn Fn(&mut ZFContext, HashMap<ZFLinkId, Arc<Box<dyn DataTrait>>>) + Send + Sync + 'static;

pub trait SinkTrait {
    fn get_input_rule(&self, ctx: &ZFContext) -> Box<FnInputRule>;

    fn get_run(&self, ctx: &ZFContext) -> Box<FnSinkRun>;

    fn get_state(&self) -> Option<Box<dyn StateTrait>>;
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

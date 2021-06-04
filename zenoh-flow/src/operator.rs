use crate::message::ZFMessage;
use crate::types::{Token, ZFContext, ZFLinkId, ZFResult};
use async_std::sync::Arc;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;

//Create a Derive macro for this
pub trait DataTrait: Debug {
    fn as_any(&self) -> &dyn Any;
}

//Create a Derive macro for this
pub trait StateTrait: Debug {
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
}

//Create a Derive macro for this??
pub trait OperatorMode: Into<u128> + From<u128> {}

pub type InputRuleResult = ZFResult<bool>;

// CAUTION, USER CAN DO NASTY THINGS, eg. remove a link we have passed to him.
pub type FnInputRule = dyn Fn(&mut ZFContext, &mut HashMap<ZFLinkId, Token<dyn DataTrait>>) -> InputRuleResult
    + Send
    + Sync
    + 'static;

pub type OutputRuleResult = ZFResult<HashMap<ZFLinkId, Arc<ZFMessage>>>;

pub type FnOutputRule = dyn Fn(&mut ZFContext, HashMap<ZFLinkId, Arc<dyn DataTrait>>) -> OutputRuleResult
    + Send
    + Sync
    + 'static;

pub type RunResult = ZFResult<HashMap<ZFLinkId, Arc<dyn DataTrait>>>;

pub type FnRun = dyn Fn(&mut ZFContext, HashMap<ZFLinkId, Arc<dyn DataTrait>>) -> RunResult
    + Send
    + Sync
    + 'static;

pub trait OperatorTrait {
    fn get_input_rule(&self, ctx: &ZFContext) -> Box<FnInputRule>;

    fn get_output_rule(&self, ctx: &ZFContext) -> Box<FnOutputRule>;

    fn get_run(&self, ctx: &ZFContext) -> Box<FnRun>;

    fn get_state(&self) -> Arc<dyn StateTrait>;

    fn serialize_state(&self) -> Vec<u8>;

    fn deserialize_state() -> Arc<dyn StateTrait>;
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

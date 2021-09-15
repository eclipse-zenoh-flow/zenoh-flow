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

use crate::runtime::message::ZFDataMessage;
use crate::{Context, PortId, Token, ZFComponentOutput, ZFResult};
use async_std::sync::Arc;
use async_trait::async_trait;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;

// NOTE: This trait is separate from `ZFDataTrait` so that we can provide a `#derive` macro to
// automatically implement it for the users.
pub trait ZFDowncastAny {
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
}

pub trait DataTrait: ZFDowncastAny + Debug + Send + Sync {
    fn try_serialize(&self) -> ZFResult<Vec<u8>>;
}

pub trait Deserializable {
    fn try_deserialize(bytes: &[u8]) -> ZFResult<Self>
    where
        Self: Sized;
}

pub trait ZFStateTrait: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
}

pub trait ZFComponent {
    fn initialize(&self, configuration: &Option<HashMap<String, String>>) -> Box<dyn ZFStateTrait>;

    fn clean(&self, state: &mut Box<dyn ZFStateTrait>) -> ZFResult<()>;
}

pub trait ZFComponentInputRule {
    fn input_rule(
        &self,
        context: &mut Context,
        state: &mut Box<dyn ZFStateTrait>,
        tokens: &mut HashMap<PortId, Token>,
    ) -> ZFResult<bool>;
}

pub trait ZFComponentOutputRule {
    fn output_rule(
        &self,
        context: &mut Context,
        state: &mut Box<dyn ZFStateTrait>,
        outputs: &HashMap<PortId, Arc<dyn DataTrait>>,
    ) -> ZFResult<HashMap<PortId, ZFComponentOutput>>;
}

pub trait ZFOperatorTrait:
    ZFComponent + ZFComponentInputRule + ZFComponentOutputRule + Send + Sync
{
    fn run(
        &self,
        context: &mut Context,
        state: &mut Box<dyn ZFStateTrait>,
        inputs: &mut HashMap<PortId, ZFDataMessage>,
    ) -> ZFResult<HashMap<PortId, Arc<dyn DataTrait>>>;
}

#[async_trait]
pub trait ZFSourceTrait: ZFComponent + ZFComponentOutputRule + Send + Sync {
    async fn run(
        &self,
        context: &mut Context,
        state: &mut Box<dyn ZFStateTrait>,
    ) -> ZFResult<HashMap<PortId, Arc<dyn DataTrait>>>;
}

#[async_trait]
pub trait ZFSinkTrait: ZFComponent + ZFComponentInputRule + Send + Sync {
    async fn run(
        &self,
        context: &mut Context,
        state: &mut Box<dyn ZFStateTrait>,
        inputs: &mut HashMap<PortId, ZFDataMessage>,
    ) -> ZFResult<()>;
}

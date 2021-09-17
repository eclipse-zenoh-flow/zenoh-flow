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

use crate::runtime::message::DataMessage;
use crate::{ComponentOutput, Context, PortId, Token, ZFResult};
use async_std::sync::Arc;
use async_trait::async_trait;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;

// NOTE: This trait is separate from `ZFDataTrait` so that we can provide a `#derive` macro to
// automatically implement it for the users.
pub trait DowncastAny {
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
}

pub trait Data: DowncastAny + Debug + Send + Sync {
    fn try_serialize(&self) -> ZFResult<Vec<u8>>;
}

pub trait Deserializable {
    fn try_deserialize(bytes: &[u8]) -> ZFResult<Self>
    where
        Self: Sized;
}

pub trait State: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
}

pub trait Component {
    fn initialize(&self, configuration: &Option<HashMap<String, String>>) -> Box<dyn State>;

    fn clean(&self, state: &mut Box<dyn State>) -> ZFResult<()>;
}

pub trait InputRule {
    fn input_rule(
        &self,
        context: &mut Context,
        state: &mut Box<dyn State>,
        tokens: &mut HashMap<PortId, Token>,
    ) -> ZFResult<bool>;
}

pub trait OutputRule {
    fn output_rule(
        &self,
        context: &mut Context,
        state: &mut Box<dyn State>,
        outputs: HashMap<PortId, Arc<dyn Data>>,
    ) -> ZFResult<HashMap<PortId, ComponentOutput>>;
}

pub trait Operator: Component + InputRule + OutputRule + Send + Sync {
    fn run(
        &self,
        context: &mut Context,
        state: &mut Box<dyn State>,
        inputs: &mut HashMap<PortId, DataMessage>,
    ) -> ZFResult<HashMap<PortId, Arc<dyn Data>>>;
}

#[async_trait]
pub trait Source: Component + OutputRule + Send + Sync {
    async fn run(
        &self,
        context: &mut Context,
        state: &mut Box<dyn State>,
    ) -> ZFResult<HashMap<PortId, Arc<dyn Data>>>;
}

#[async_trait]
pub trait Sink: Component + InputRule + Send + Sync {
    async fn run(
        &self,
        context: &mut Context,
        state: &mut Box<dyn State>,
        inputs: &mut HashMap<PortId, DataMessage>,
    ) -> ZFResult<()>;
}

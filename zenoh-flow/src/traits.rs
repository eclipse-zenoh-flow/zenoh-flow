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
use crate::{
    Configuration, Context, Data, DeadlineMiss, NodeOutput, PortId, State, Token, ZFResult,
};
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

pub trait ZFData: DowncastAny + Debug + Send + Sync {
    fn try_serialize(&self) -> ZFResult<Vec<u8>>;
}

pub trait Deserializable {
    fn try_deserialize(bytes: &[u8]) -> ZFResult<Self>
    where
        Self: Sized;
}

pub trait ZFState: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
}

pub trait Node {
    fn initialize(&self, configuration: &Option<Configuration>) -> ZFResult<State>;

    fn finalize(&self, state: &mut State) -> ZFResult<()>;
}

pub trait Operator: Node + Send + Sync {
    fn input_rule(
        &self,
        context: &mut Context,
        state: &mut State,
        tokens: &mut HashMap<PortId, Token>,
    ) -> ZFResult<bool>;

    fn run(
        &self,
        context: &mut Context,
        state: &mut State,
        inputs: &mut HashMap<PortId, DataMessage>,
    ) -> ZFResult<HashMap<PortId, Data>>;

    fn output_rule(
        &self,
        context: &mut Context,
        state: &mut State,
        outputs: HashMap<PortId, Data>,
        deadline_miss: Option<DeadlineMiss>,
    ) -> ZFResult<HashMap<PortId, NodeOutput>>;
}

#[async_trait]
pub trait Source: Node + Send + Sync {
    async fn run(&self, context: &mut Context, state: &mut State) -> ZFResult<Data>;
}

#[async_trait]
pub trait Sink: Node + Send + Sync {
    async fn run(
        &self,
        context: &mut Context,
        state: &mut State,
        input: DataMessage,
    ) -> ZFResult<()>;
}

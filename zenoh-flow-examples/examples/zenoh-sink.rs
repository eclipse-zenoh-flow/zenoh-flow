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

use async_std::sync::Arc;
use std::collections::HashMap;
use zenoh_flow::{
    downcast, get_input,
    operator::{DataTrait, FnInputRule, FnSinkRun, InputRuleResult, SinkTrait, StateTrait},
    serde::{Deserialize, Serialize},
    types::{Token, ZFContext, ZFError, ZFLinkId},
    zenoh_flow_macros::ZFState,
};

use zenoh_flow_examples::ZFBytes;

use zenoh::net::config;
use zenoh::net::{open, Session};
use zenoh::ZFuture;

static INPUT: &str = "Frame";

#[derive(Debug)]
struct ExampleGenericZenohSink {
    state: ZSinkState,
}

#[derive(ZFState, Clone, Debug)]
struct ZSinkState {
    session: Arc<Session>,
}

impl ExampleGenericZenohSink {
    pub fn new() -> Self {
        Self {
            state: ZSinkState {
                session: Arc::new(open(config::peer()).wait().unwrap()),
            },
        }
    }

    pub fn ir_1(_ctx: &mut ZFContext, inputs: &mut HashMap<ZFLinkId, Token>) -> InputRuleResult {
        if let Some(token) = inputs.get(INPUT) {
            match token {
                Token::Ready(_) => Ok(true),
                Token::NotReady(_) => Ok(false),
            }
        } else {
            Err(ZFError::MissingInput(String::from(INPUT)))
        }
    }

    pub fn run_1(ctx: &mut ZFContext, inputs: HashMap<ZFLinkId, Arc<dyn DataTrait>>) {
        let state = ctx.get_state().unwrap(); //getting state,
        let _state = downcast!(ZSinkState, state).unwrap(); //downcasting to right type

        let path = format!("/zf/probe/{}", String::from(INPUT));
        let data = get_input!(ZFBytes, String::from(INPUT), inputs).unwrap();

        _state
            .session
            .write(&path.into(), data.bytes.clone().into())
            .wait()
            .unwrap();
    }
}

impl SinkTrait for ExampleGenericZenohSink {
    fn get_input_rule(&self, ctx: &ZFContext) -> Box<FnInputRule> {
        match ctx.mode {
            0 => Box::new(Self::ir_1),
            _ => panic!("No way"),
        }
    }

    fn get_run(&self, ctx: &ZFContext) -> Box<FnSinkRun> {
        match ctx.mode {
            0 => Box::new(Self::run_1),
            _ => panic!("No way"),
        }
    }

    fn get_state(&self) -> Option<Box<dyn StateTrait>> {
        Some(Box::new(self.state.clone()))
    }
}

zenoh_flow::export_sink!(register);

extern "C" fn register(registrar: &mut dyn zenoh_flow::loader::ZFSinkRegistrarTrait) {
    registrar.register_zfsink(
        "zsink",
        Box::new(ExampleGenericZenohSink::new()) as Box<dyn zenoh_flow::operator::SinkTrait + Send>,
    );
}

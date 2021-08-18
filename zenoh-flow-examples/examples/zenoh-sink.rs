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
    serde::{Deserialize, Serialize},
    types::{
        DataTrait, FnInputRule, FnSinkRun, FutSinkOutput, InputRuleOutput, SinkTrait, StateTrait,
        Token, ZFContext, ZFError, ZFInput, ZFResult,
    },
    zenoh_flow_derive::ZFState,
};

use zenoh_flow_examples::ZFBytes;

use zenoh::net::config;
use zenoh::net::{open, Session};
use zenoh::ZFuture;

static INPUT: &str = "Data";

#[derive(Debug)]
struct ExampleGenericZenohSink {
    state: ZSinkState,
}

#[derive(Debug, Clone)]
struct ZSinkInner {
    session: Arc<Session>,
}

#[derive(Serialize, Deserialize, Clone, Debug, ZFState)]
struct ZSinkState {
    #[serde(skip_serializing, skip_deserializing)]
    inner: Option<ZSinkInner>,
}

impl ExampleGenericZenohSink {
    pub fn new() -> Self {
        Self {
            state: ZSinkState {
                inner: Some(ZSinkInner {
                    session: Arc::new(open(config::peer()).wait().unwrap()),
                }),
            },
        }
    }

    pub fn ir_1(_ctx: ZFContext, inputs: &mut HashMap<String, Token>) -> InputRuleOutput {
        if let Some(token) = inputs.get(INPUT) {
            match token {
                Token::Ready(_) => Ok(true),
                Token::NotReady => Ok(false),
            }
        } else {
            Err(ZFError::MissingInput(String::from(INPUT)))
        }
    }

    pub async fn run_1(ctx: ZFContext, mut inputs: ZFInput) -> ZFResult<()> {
        let guard = ctx.async_lock().await; //getting state,
        let _state = downcast!(ZSinkState, guard.state).unwrap(); //downcasting to right type

        let inner = _state.inner.as_ref().unwrap();

        let path = format!("/zf/probe/{}", String::from(INPUT));
        let (_, data) = get_input!(ZFBytes, String::from(INPUT), inputs).unwrap();

        inner
            .session
            .write(&path.into(), data.bytes.into())
            .wait()
            .unwrap();
        Ok(())
    }
}

impl SinkTrait for ExampleGenericZenohSink {
    fn get_input_rule(&self, _ctx: ZFContext) -> Box<FnInputRule> {
        Box::new(Self::ir_1)
    }

    fn get_run(&self, _ctx: ZFContext) -> FnSinkRun {
        Box::new(|ctx: ZFContext, inputs: ZFInput| -> FutSinkOutput {
            Box::pin(Self::run_1(ctx, inputs))
        })
    }

    fn get_state(&self) -> Box<dyn StateTrait> {
        Box::new(self.state.clone())
    }
}

zenoh_flow::export_sink!(register);

extern "C" fn register(
    _configuration: Option<HashMap<String, String>>,
) -> ZFResult<Box<dyn zenoh_flow::SinkTrait + Send>> {
    Ok(Box::new(ExampleGenericZenohSink::new()) as Box<dyn zenoh_flow::SinkTrait + Send>)
}

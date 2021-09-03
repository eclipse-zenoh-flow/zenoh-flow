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
use async_trait::async_trait;
use std::collections::HashMap;
use zenoh_flow::{
    default_input_rule, downcast, get_input,
    types::{Token, ZFResult},
    zenoh_flow_derive::ZFState,
    ZFComponent, ZFComponentInputRule, ZFSinkTrait,
};

use zenoh::net::config;
use zenoh::net::{open, Session};
use zenoh::ZFuture;
use zenoh_flow_examples::ZFBytes;

static INPUT: &str = "Data";

#[derive(Debug)]
struct ExampleGenericZenohSink;

#[derive(Clone, Debug, ZFState)]
struct ZSinkState {
    session: Arc<Session>,
}

impl ZSinkState {
    pub fn new() -> Self {
        Self {
            session: Arc::new(open(config::peer()).wait().unwrap()),
        }
    }
}

impl ZFComponent for ExampleGenericZenohSink {
    fn initial_state(
        &self,
        _configuration: &Option<HashMap<String, String>>,
    ) -> Box<dyn zenoh_flow::ZFStateTrait> {
        Box::new(ZSinkState::new())
    }
}

impl ZFComponentInputRule for ExampleGenericZenohSink {
    fn input_rule(
        &self,
        state: &mut Box<dyn zenoh_flow::ZFStateTrait>,
        tokens: &mut HashMap<String, Token>,
    ) -> ZFResult<bool> {
        default_input_rule(state, tokens)
    }
}

#[async_trait]
impl ZFSinkTrait for ExampleGenericZenohSink {
    async fn run(
        &self,
        dyn_state: &mut Box<dyn zenoh_flow::ZFStateTrait>,
        inputs: &mut HashMap<String, zenoh_flow::runtime::message::ZFDataMessage>,
    ) -> ZFResult<()> {
        let state = downcast!(ZSinkState, dyn_state).unwrap();

        let path = format!("/zf/probe/{}", String::from(INPUT));
        let (_, data) = get_input!(ZFBytes, String::from(INPUT), inputs).unwrap();

        state
            .session
            .write(&path.into(), data.0.into())
            .wait()
            .unwrap();
        Ok(())
    }
}

zenoh_flow::export_sink!(register);

fn register() -> ZFResult<Box<dyn ZFSinkTrait + Send>> {
    Ok(Box::new(ExampleGenericZenohSink))
}

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

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use zenoh::net::Session;
use zenoh_flow::{
    downcast,
    operator::{
        DataTrait, FnInputRule, FnSinkRun, FutSinkResult, InputRuleResult, SinkTrait, StateTrait,
    },
    zenoh_flow_macros::ZFState,
    zf_empty_state, Token, ZFContext, ZFLinkId, ZFResult,
};

struct ZenohSender {
    pub state: ZenohSenderState,
}

// #[derive(Serialize, Deserialize, Debug, Clone, ZFState)]
#[derive(Serialize, Deserialize, Debug, Clone, ZFState)]
struct ZenohSenderState {
    pub resource: String,
    pub input: String,
    #[serde(skip_serializing, skip_deserializing)]
    pub session: Option<Arc<Session>>,
}

impl ZenohSender {
    pub fn new(session: Arc<Session>, configuration: Option<HashMap<String, String>>) -> Self {
        if let Some(config) = configuration {
            return Self {
                state: ZenohSenderState {
                    resource: config.get("resource").cloned().unwrap(),
                    input: config.get("input").cloned().unwrap(),
                    session: Some(session),
                },
            };
        }

        panic!("Missing configuration for ZenohSender")
    }

    pub fn input_rule(_ctx: ZFContext, _inputs: &mut HashMap<ZFLinkId, Token>) -> InputRuleResult {
        Ok(true)
    }

    pub async fn run(
        context: ZFContext,
        inputs: HashMap<ZFLinkId, Arc<Box<dyn DataTrait>>>,
    ) -> ZFResult<()> {
        let guard_context = context.lock();
        let state = downcast!(ZenohSenderState, guard_context.state).unwrap();
        let session = state.session.as_ref().unwrap();

        for data in inputs.values() {
            let raw = bincode::serialize(&(**data)).unwrap();

            session
                .write(&state.resource.clone().into(), raw.into())
                .await
                .unwrap();
        }

        Ok(())
    }
}

impl SinkTrait for ZenohSender {
    fn get_state(&self) -> Box<dyn StateTrait> {
        Box::new(self.state.clone())
    }

    fn get_input_rule(&self, _ctx: ZFContext) -> Box<FnInputRule> {
        Box::new(Self::input_rule)
    }

    fn get_run(&self, _ctx: ZFContext) -> FnSinkRun {
        Box::new(
            |ctx: ZFContext, inputs: HashMap<ZFLinkId, Arc<Box<dyn DataTrait>>>| -> FutSinkResult {
                Box::pin(Self::run(ctx, inputs))
            },
        )
    }
}

zenoh_flow::export_zenoh_sender!(register);

extern "C" fn register(
    registrar: &mut dyn zenoh_flow::loader::ZFSinkRegistrarTrait,
    session: Arc<Session>,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<()> {
    registrar.register_zfsink(
        "ZenohSender",
        Box::new(ZenohSender::new(session, configuration))
            as Box<dyn zenoh_flow::operator::SinkTrait + Send>,
    );
    Ok(())
}

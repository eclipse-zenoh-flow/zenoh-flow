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

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use zenoh::net::{Session, SubInfo};
use zenoh_flow::{
    downcast,
    operator::{
        default_output_rule, DataTrait, FnOutputRule, FnSourceRun, FutRunResult, RunResult,
        SourceTrait, StateTrait,
    },
    zenoh_flow_macros::ZFState,
    ZFContext, ZFLinkId, ZFResult,
};
// NOTE: This is a dirty dirty workaround, bincode needs the type in order to deserialize…
use zenoh_flow_examples::{ZFString, ZFUsize};

struct ZenohReceiver {
    pub state: ZenohReceiverState,
}

// #[derive(Serialize, Deserialize, Debug, Clone, ZFState)]
#[derive(Serialize, Deserialize, Debug, Clone, ZFState)]
struct ZenohReceiverState {
    pub resource: String,
    pub output: String,
    #[serde(skip_serializing, skip_deserializing)]
    pub session: Option<Arc<Session>>,
}

impl ZenohReceiver {
    pub fn new(session: Arc<Session>, configuration: Option<HashMap<String, String>>) -> Self {
        if let Some(config) = configuration {
            return Self {
                state: ZenohReceiverState {
                    resource: config.get("resource").cloned().unwrap(),
                    output: config.get("output").cloned().unwrap(),
                    session: Some(session),
                },
            };
        }

        panic!("Missing configuration for ZenohReceiver")
    }

    pub async fn run(context: ZFContext) -> RunResult {
        let guard_context = context.lock();
        let state = downcast!(ZenohReceiverState, guard_context.state).unwrap();
        let session = state.session.as_ref().unwrap();

        let sub_info = SubInfo {
            reliability: zenoh::net::Reliability::Reliable,
            mode: zenoh::net::SubMode::Push,
            period: None,
        };
        let mut results: HashMap<ZFLinkId, Arc<Box<dyn DataTrait>>> = HashMap::with_capacity(1);
        let mut subscriber = session
            .declare_subscriber(&state.resource.clone().into(), &sub_info)
            .await
            .unwrap();

        let sample = subscriber.receiver().next().await.unwrap();
        let data: Box<dyn DataTrait> = bincode::deserialize(&sample.payload.to_vec()).unwrap();
        results.insert(state.output.clone(), Arc::new(data));

        Ok(results)
    }
}

impl SourceTrait for ZenohReceiver {
    fn get_state(&self) -> Box<dyn StateTrait> {
        Box::new(self.state.clone())
    }

    fn get_output_rule(&self, _ctx: ZFContext) -> Box<FnOutputRule> {
        Box::new(default_output_rule)
    }

    fn get_run(&self, _ctx: ZFContext) -> FnSourceRun {
        Box::new(|ctx: ZFContext| -> FutRunResult { Box::pin(Self::run(ctx)) })
    }
}

zenoh_flow::export_zenoh_receiver!(register);

extern "C" fn register(
    registrar: &mut dyn zenoh_flow::loader::ZFSourceRegistrarTrait,
    session: Arc<Session>,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<()> {
    registrar.register_zfsource(
        "ZenohReceiver",
        Box::new(ZenohReceiver::new(session, configuration))
            as Box<dyn zenoh_flow::operator::SourceTrait + Send>,
    );
    Ok(())
}

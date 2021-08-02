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

use std::collections::HashMap;
use zenoh_flow::{
    serde::{Deserialize, Serialize},
    types::{
        FnInputRule, FnSinkRun, FutSinkResult, InputRuleResult, SinkTrait, StateTrait, Token,
        ZFContext, ZFInput,
    },
    zenoh_flow_derive::ZFState,
    zf_empty_state, ZFResult,
};

#[derive(Serialize, Deserialize, Debug, ZFState)]
struct ExampleGenericSink {}

impl ExampleGenericSink {
    pub fn ir_1(_ctx: ZFContext, _inputs: &mut HashMap<String, Token>) -> InputRuleResult {
        Ok(true)
    }

    pub async fn run_1(_ctx: ZFContext, inputs: ZFInput) -> ZFResult<()> {
        println!("#######");
        for (k, v) in inputs.into_iter() {
            println!("Example Generic Sink Received on LinkId {:?} -> {:?}", k, v);
        }
        println!("#######");
        Ok(())
    }
}

impl SinkTrait for ExampleGenericSink {
    fn get_input_rule(&self, ctx: ZFContext) -> Box<FnInputRule> {
        let gctx = ctx.lock();
        match gctx.mode {
            0 => Box::new(Self::ir_1),
            _ => panic!("No way"),
        }
    }

    fn get_run(&self, ctx: ZFContext) -> FnSinkRun {
        let gctx = ctx.lock();
        match gctx.mode {
            0 => Box::new(|ctx: ZFContext, inputs: ZFInput| -> FutSinkResult {
                Box::pin(Self::run_1(ctx, inputs))
            }),
            _ => panic!("No way"),
        }
    }

    fn get_state(&self) -> Box<dyn StateTrait> {
        zf_empty_state!()
    }
}

zenoh_flow::export_sink!(register);

extern "C" fn register(
    _configuration: Option<HashMap<String, String>>,
) -> ZFResult<Box<dyn zenoh_flow::SinkTrait + Send>> {
    Ok(Box::new(ExampleGenericSink {}) as Box<dyn zenoh_flow::SinkTrait + Send>)
}

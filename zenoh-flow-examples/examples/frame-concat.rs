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

use async_std::sync::{Arc, Mutex};
use std::collections::HashMap;
use zenoh_flow::{
    default_input_rule, default_output_rule, downcast, get_input, types::ZFResult,
    zenoh_flow_derive::ZFState, zf_data, zf_spin_lock, ZFComponent, ZFComponentInputRule,
    ZFComponentOutputRule, ZFDataTrait, ZFOperatorTrait, ZFStateTrait,
};
use zenoh_flow_examples::ZFBytes;

use opencv::core;

static INPUT1: &str = "Frame1";
static INPUT2: &str = "Frame2";
static OUTPUT: &str = "Frame";

#[derive(ZFState, Clone)]
struct FrameConcatState {
    pub encode_options: Arc<Mutex<opencv::types::VectorOfi32>>,
}

// because of opencv
impl std::fmt::Debug for FrameConcatState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ConcatState:...",)
    }
}

impl FrameConcatState {
    fn new() -> Self {
        Self {
            encode_options: Arc::new(Mutex::new(opencv::types::VectorOfi32::new())),
        }
    }
}

struct FrameConcat;

impl ZFComponent for FrameConcat {
    fn initialize(
        &self,
        _configuration: &Option<HashMap<String, String>>,
    ) -> Box<dyn zenoh_flow::ZFStateTrait> {
        Box::new(FrameConcatState::new())
    }

    fn clean(&self, _state: &mut Box<dyn ZFStateTrait>) -> ZFResult<()> {
        Ok(())
    }
}

impl ZFComponentInputRule for FrameConcat {
    fn input_rule(
        &self,
        _context: &mut zenoh_flow::ZFContext,
        state: &mut Box<dyn zenoh_flow::ZFStateTrait>,
        tokens: &mut HashMap<String, zenoh_flow::Token>,
    ) -> ZFResult<bool> {
        default_input_rule(state, tokens)
    }
}

impl ZFComponentOutputRule for FrameConcat {
    fn output_rule(
        &self,
        _context: &mut zenoh_flow::ZFContext,
        state: &mut Box<dyn zenoh_flow::ZFStateTrait>,
        outputs: &HashMap<String, Arc<dyn zenoh_flow::ZFDataTrait>>,
    ) -> ZFResult<HashMap<zenoh_flow::ZFPortID, zenoh_flow::ZFComponentOutput>> {
        default_output_rule(state, outputs)
    }
}

impl ZFOperatorTrait for FrameConcat {
    fn run(
        &self,
        _context: &mut zenoh_flow::ZFContext,
        dyn_state: &mut Box<dyn zenoh_flow::ZFStateTrait>,
        inputs: &mut HashMap<String, zenoh_flow::runtime::message::ZFDataMessage>,
    ) -> ZFResult<HashMap<zenoh_flow::ZFPortID, Arc<dyn zenoh_flow::ZFDataTrait>>> {
        let mut results: HashMap<String, Arc<dyn ZFDataTrait>> = HashMap::new();

        let state = downcast!(FrameConcatState, dyn_state).unwrap();
        let encode_options = zf_spin_lock!(state.encode_options);

        let (_, frame1) = get_input!(ZFBytes, String::from(INPUT1), inputs)?;
        let (_, frame2) = get_input!(ZFBytes, String::from(INPUT2), inputs)?;

        // Decode Image
        let frame1 = opencv::imgcodecs::imdecode(
            &opencv::types::VectorOfu8::from_iter(frame1.0),
            opencv::imgcodecs::IMREAD_COLOR,
        )
        .unwrap();

        let frame2 = opencv::imgcodecs::imdecode(
            &opencv::types::VectorOfu8::from_iter(frame2.0),
            opencv::imgcodecs::IMREAD_COLOR,
        )
        .unwrap();

        let mut frame = core::Mat::default();

        // concat frames
        core::vconcat2(&frame1, &frame2, &mut frame).unwrap();

        let mut buf = opencv::types::VectorOfu8::new();
        opencv::imgcodecs::imencode(".jpg", &frame, &mut buf, &encode_options).unwrap();

        results.insert(String::from(OUTPUT), zf_data!(ZFBytes(buf.into())));

        Ok(results)
    }
}

zenoh_flow::export_operator!(register);

fn register() -> ZFResult<Arc<dyn ZFOperatorTrait>> {
    Ok(Arc::new(FrameConcat) as Arc<dyn ZFOperatorTrait>)
}

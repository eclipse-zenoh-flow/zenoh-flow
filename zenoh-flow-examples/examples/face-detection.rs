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
use rand::Rng;
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use zenoh_flow::{
    downcast, downcast_mut, get_input,
    runtime::message::ZFMessage,
    serde::{Deserialize, Serialize},
    types::{
        DataTrait, FnInputRule, FnOutputRule, FnRun, InputRuleResult, OperatorTrait,
        OutputRuleResult, RunResult, StateTrait, Token, ZFContext, ZFError, ZFInput, ZFLinkId,
        ZFResult,
    },
    zenoh_flow_derive::ZFState,
    zf_data, zf_spin_lock,
};
use zenoh_flow_examples::{ZFBytes, ZFOpenCVBytes};

use opencv::{core, highgui, imgproc, objdetect, prelude::*, types, videoio, Result};

static INPUT: &str = "Frame";
static OUTPUT: &str = "Frame";

#[derive(Debug)]
struct FaceDetection {
    pub state: FDState,
}

#[derive(Clone)]
struct FDInnerState {
    pub face: Arc<Mutex<objdetect::CascadeClassifier>>,
    pub encode_options: Arc<Mutex<opencv::types::VectorOfi32>>,
}

#[derive(Serialize, Deserialize, ZFState, Clone)]
struct FDState {
    #[serde(skip_serializing, skip_deserializing)]
    pub inner: Option<FDInnerState>,
}

// because of opencv
impl std::fmt::Debug for FDState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "FDState:...",)
    }
}

impl FaceDetection {
    fn new(configuration: HashMap<String, String>) -> Self {
        let neural_network = match configuration.get("neural-network") {
            Some(net) => net,
            None => "haarcascades/haarcascade_frontalface_alt.xml",
        };

        let xml = core::find_file(neural_network, true, false).unwrap();
        let mut face = objdetect::CascadeClassifier::new(&xml).unwrap();
        let encode_options = opencv::types::VectorOfi32::new();

        let inner = Some(FDInnerState {
            face: Arc::new(Mutex::new(face)),
            encode_options: Arc::new(Mutex::new(encode_options)),
        });
        let state = FDState { inner };

        Self { state }
    }

    pub fn ir_1(_ctx: ZFContext, inputs: &mut HashMap<ZFLinkId, Token>) -> InputRuleResult {
        if let Some(token) = inputs.get(INPUT) {
            match token {
                Token::Ready(_) => Ok(true),
                Token::NotReady(_) => Ok(false),
            }
        } else {
            Err(ZFError::MissingInput(String::from(INPUT)))
        }
    }

    pub fn run_1(ctx: ZFContext, mut inputs: ZFInput) -> RunResult {
        let mut results: HashMap<ZFLinkId, Arc<dyn DataTrait>> = HashMap::new();

        let mut guard = ctx.lock(); //getting state
        let _state = downcast!(FDState, guard.state).unwrap(); //downcasting to right type

        let inner = _state.inner.as_ref().unwrap();

        let mut face = zf_spin_lock!(inner.face);
        let encode_options = zf_spin_lock!(inner.encode_options);

        let data = get_input!(ZFBytes, String::from(INPUT), inputs).unwrap();

        // Decode Image
        let mut frame = opencv::imgcodecs::imdecode(
            &opencv::types::VectorOfu8::from_iter(data.bytes.clone()),
            opencv::imgcodecs::IMREAD_COLOR,
        )
        .unwrap();

        let mut gray = Mat::default();
        imgproc::cvt_color(&frame, &mut gray, imgproc::COLOR_BGR2GRAY, 0).unwrap();
        let mut reduced = Mat::default();
        imgproc::resize(
            &gray,
            &mut reduced,
            core::Size {
                width: 0,
                height: 0,
            },
            0.25f64,
            0.25f64,
            imgproc::INTER_LINEAR,
        )
        .unwrap();
        let mut faces = types::VectorOfRect::new();
        face.detect_multi_scale(
            &reduced,
            &mut faces,
            1.1,
            2,
            objdetect::CASCADE_SCALE_IMAGE,
            core::Size {
                width: 30,
                height: 30,
            },
            core::Size {
                width: 0,
                height: 0,
            },
        )
        .unwrap();
        for face in faces {
            let scaled_face = core::Rect {
                x: face.x * 4,
                y: face.y * 4,
                width: face.width * 4,
                height: face.height * 4,
            };
            imgproc::rectangle(
                &mut frame,
                scaled_face,
                core::Scalar::new(0f64, 255f64, -1f64, -1f64),
                10,
                1,
                0,
            )
            .unwrap();
        }

        let mut buf = opencv::types::VectorOfu8::new();
        opencv::imgcodecs::imencode(".jpg", &frame, &mut buf, &encode_options).unwrap();

        let data = ZFBytes {
            bytes: buf.to_vec(),
        };

        results.insert(String::from(OUTPUT), zf_data!(data));

        drop(face);

        Ok(results)
    }

    pub fn or_1(
        _ctx: ZFContext,
        outputs: HashMap<ZFLinkId, Arc<dyn DataTrait>>,
    ) -> OutputRuleResult {
        let mut results = HashMap::new();
        for (k, v) in outputs {
            // should be ZFMessage::from_data
            results.insert(k, Arc::new(ZFMessage::from_data(v)));
        }
        Ok(results)
    }
}

impl OperatorTrait for FaceDetection {
    fn get_input_rule(&self, ctx: ZFContext) -> Box<FnInputRule> {
        Box::new(Self::ir_1)
    }

    fn get_output_rule(&self, ctx: ZFContext) -> Box<FnOutputRule> {
        Box::new(Self::or_1)
    }

    fn get_run(&self, ctx: ZFContext) -> Box<FnRun> {
        Box::new(Self::run_1)
    }

    fn get_state(&self) -> Box<dyn StateTrait> {
        Box::new(self.state.clone())
    }
}

// //Also generated by macro
zenoh_flow::export_operator!(register);

extern "C" fn register(
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<Box<dyn zenoh_flow::OperatorTrait + Send>> {
    match configuration {
        Some(config) => {
            Ok(Box::new(FaceDetection::new(config)) as Box<dyn zenoh_flow::OperatorTrait + Send>)
        }
        None => Ok(Box::new(FaceDetection::new(HashMap::new()))
            as Box<dyn zenoh_flow::OperatorTrait + Send>),
    }
}

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
    downcast, get_input,
    serde::{Deserialize, Serialize},
    types::{
        DataTrait, FnInputRule, FnOutputRule, FnRun, InputRuleResult, OperatorTrait, RunResult,
        StateTrait, Token, ZFContext, ZFError, ZFInput, ZFResult,
    },
    zenoh_flow_derive::ZFState,
    zf_data, zf_spin_lock,
};
use zenoh_flow_examples::ZFBytes;

use opencv::{core, imgproc, objdetect, prelude::*, types};

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
        let default_neural_network = "haarcascades/haarcascade_frontalface_alt.xml".to_owned();
        let neural_network = configuration
            .get("neural-network")
            .unwrap_or(&default_neural_network);

        let xml = core::find_file(neural_network, true, false).unwrap();
        let face = objdetect::CascadeClassifier::new(&xml).unwrap();
        let encode_options = opencv::types::VectorOfi32::new();

        let inner = Some(FDInnerState {
            face: Arc::new(Mutex::new(face)),
            encode_options: Arc::new(Mutex::new(encode_options)),
        });
        let state = FDState { inner };

        Self { state }
    }

    pub fn ir_1(_ctx: ZFContext, inputs: &mut HashMap<String, Token>) -> InputRuleResult {
        if let Some(token) = inputs.get(INPUT) {
            match token {
                Token::Ready(_) => Ok(true),
                Token::NotReady => Ok(false),
            }
        } else {
            Err(ZFError::MissingInput(String::from(INPUT)))
        }
    }

    pub fn run_1(ctx: ZFContext, mut inputs: ZFInput) -> RunResult {
        let mut results: HashMap<String, Arc<dyn DataTrait>> = HashMap::new();

        let guard = ctx.lock(); //getting state
        let _state = downcast!(FDState, guard.state).unwrap(); //downcasting to right type

        let inner = _state.inner.as_ref().unwrap();

        let mut face = zf_spin_lock!(inner.face);
        let encode_options = zf_spin_lock!(inner.encode_options);

        let (_, data) = get_input!(ZFBytes, String::from(INPUT), inputs).unwrap();

        // Decode Image
        let mut frame = opencv::imgcodecs::imdecode(
            &opencv::types::VectorOfu8::from_iter(data.bytes),
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
}

impl OperatorTrait for FaceDetection {
    fn get_input_rule(&self, _ctx: ZFContext) -> Box<FnInputRule> {
        Box::new(Self::ir_1)
    }

    fn get_output_rule(&self, _ctx: ZFContext) -> Box<FnOutputRule> {
        Box::new(zenoh_flow::default_output_rule)
    }

    fn get_run(&self, _ctx: ZFContext) -> Box<FnRun> {
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

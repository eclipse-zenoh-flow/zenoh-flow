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
    default_input_rule, default_output_rule, downcast, get_input, runtime::message::ZFDataMessage,
    zenoh_flow_derive::ZFState, zf_data, zf_spin_lock, ZFComponent, ZFComponentInputRule,
    ZFComponentOutputRule, ZFDataTrait, ZFOperatorTrait, ZFPortID, ZFResult, ZFStateTrait,
};
use zenoh_flow_examples::ZFBytes;

use opencv::{core, imgproc, objdetect, prelude::*, types};

static INPUT: &str = "Frame";
static OUTPUT: &str = "Frame";

#[derive(Debug)]
struct FaceDetection;

#[derive(ZFState, Clone)]
struct FDState {
    pub face: Arc<Mutex<objdetect::CascadeClassifier>>,
    pub encode_options: Arc<Mutex<opencv::types::VectorOfi32>>,
}

// because of opencv
impl std::fmt::Debug for FDState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "FDState:...",)
    }
}

impl FDState {
    fn new(configuration: &Option<HashMap<String, String>>) -> Self {
        let default_neural_network = &"haarcascades/haarcascade_frontalface_alt.xml".to_owned();
        let neural_network = if let Some(configuration) = configuration {
            configuration
                .get("neural-network")
                .unwrap_or(default_neural_network)
        } else {
            default_neural_network
        };

        let xml = core::find_file(neural_network, true, false).unwrap();
        let face = objdetect::CascadeClassifier::new(&xml).unwrap();
        let encode_options = opencv::types::VectorOfi32::new();

        Self {
            face: Arc::new(Mutex::new(face)),
            encode_options: Arc::new(Mutex::new(encode_options)),
        }
    }
}

impl ZFComponent for FaceDetection {
    fn initial_state(
        &self,
        configuration: &Option<HashMap<String, String>>,
    ) -> Box<dyn zenoh_flow::ZFStateTrait> {
        Box::new(FDState::new(configuration))
    }
}

impl ZFComponentInputRule for FaceDetection {
    fn input_rule(
        &self,
        _context: &mut zenoh_flow::ZFContext,
        state: &mut Box<dyn zenoh_flow::ZFStateTrait>,
        tokens: &mut HashMap<String, zenoh_flow::Token>,
    ) -> ZFResult<bool> {
        default_input_rule(state, tokens)
    }
}

impl ZFComponentOutputRule for FaceDetection {
    fn output_rule(
        &self,
        _context: &mut zenoh_flow::ZFContext,
        state: &mut Box<dyn zenoh_flow::ZFStateTrait>,
        outputs: &HashMap<String, Arc<dyn zenoh_flow::ZFDataTrait>>,
    ) -> ZFResult<HashMap<zenoh_flow::ZFPortID, zenoh_flow::ZFComponentOutput>> {
        default_output_rule(state, outputs)
    }
}

impl ZFOperatorTrait for FaceDetection {
    fn run(
        &self,
        _context: &mut zenoh_flow::ZFContext,
        dyn_state: &mut Box<dyn ZFStateTrait>,
        inputs: &mut HashMap<String, ZFDataMessage>,
    ) -> ZFResult<HashMap<ZFPortID, Arc<dyn ZFDataTrait>>> {
        let mut results: HashMap<String, Arc<dyn ZFDataTrait>> = HashMap::new();

        let state = downcast!(FDState, dyn_state).unwrap();

        let mut face = zf_spin_lock!(state.face);
        let encode_options = zf_spin_lock!(state.encode_options);

        let (_, data) = get_input!(ZFBytes, String::from(INPUT), inputs).unwrap();

        // Decode Image
        let mut frame = opencv::imgcodecs::imdecode(
            &opencv::types::VectorOfu8::from_iter(data.0),
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

        results.insert(String::from(OUTPUT), zf_data!(ZFBytes(buf.into())));

        drop(face);

        Ok(results)
    }
}

// Also generated by macro
zenoh_flow::export_operator!(register);

fn register() -> ZFResult<Box<dyn ZFOperatorTrait + Send>> {
    Ok(Box::new(FaceDetection) as Box<dyn ZFOperatorTrait + Send>)
}

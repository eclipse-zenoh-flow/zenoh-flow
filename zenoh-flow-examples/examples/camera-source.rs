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
use async_trait::async_trait;
use opencv::{core, prelude::*, videoio};
use std::collections::HashMap;
use zenoh_flow::{
    default_output_rule, downcast_mut, types::ZFResult, zenoh_flow_derive::ZFState, zf_data,
    zf_spin_lock, ZFComponent, ZFComponentOutputRule, ZFDataTrait, ZFSourceTrait,
};
use zenoh_flow_examples::ZFBytes;

#[derive(Debug)]
struct CameraSource;

#[derive(ZFState, Clone)]
struct CameraState {
    pub camera: Arc<Mutex<videoio::VideoCapture>>,
    pub encode_options: Arc<Mutex<opencv::types::VectorOfi32>>,
    pub resolution: (i32, i32),
    pub delay: u64,
}

// because of opencv
impl std::fmt::Debug for CameraState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "CameraState: resolution:{:?} delay:{:?}",
            self.resolution, self.delay
        )
    }
}

static SOURCE: &str = "Frame";

impl CameraState {
    fn new(configuration: &Option<HashMap<String, String>>) -> Self {
        let (camera, resolution, delay) = match configuration {
            Some(configuration) => {
                let camera = match configuration.get("camera") {
                    Some(configured_camera) => {
                        videoio::VideoCapture::from_file(configured_camera, videoio::CAP_ANY)
                            .unwrap()
                    }
                    None => videoio::VideoCapture::new(0, videoio::CAP_ANY).unwrap(),
                };

                let configured_resolution = match configuration.get("resolution") {
                    Some(res) => {
                        let v = res.split('x').collect::<Vec<&str>>();
                        (v[0].parse::<i32>().unwrap(), v[1].parse::<i32>().unwrap())
                    }
                    None => (800, 600),
                };

                let delay = match configuration.get("fps") {
                    Some(fps) => {
                        let fps = fps.parse::<f64>().unwrap();
                        let delay: f64 = 1f64 / fps;
                        (delay * 1000f64) as u64
                    }
                    None => 40,
                };

                (camera, configured_resolution, delay)
            }

            None => (
                (videoio::VideoCapture::new(0, videoio::CAP_ANY).unwrap()),
                (800, 600),
                40,
            ),
        };

        let opened = videoio::VideoCapture::is_opened(&camera).unwrap();
        if !opened {
            panic!("Unable to open default camera!");
        }
        let encode_options = opencv::types::VectorOfi32::new();

        Self {
            camera: Arc::new(Mutex::new(camera)),
            encode_options: Arc::new(Mutex::new(encode_options)),
            resolution,
            delay,
        }
    }
}

impl ZFComponent for CameraSource {
    fn initial_state(
        &self,
        configuration: &Option<HashMap<String, String>>,
    ) -> Box<dyn zenoh_flow::ZFStateTrait> {
        Box::new(CameraState::new(configuration))
    }
}

impl ZFComponentOutputRule for CameraSource {
    fn output_rule(
        &self,
        _context: &mut zenoh_flow::ZFContext,
        state: &mut Box<dyn zenoh_flow::ZFStateTrait>,
        outputs: &HashMap<String, Arc<dyn ZFDataTrait>>,
    ) -> ZFResult<HashMap<zenoh_flow::ZFPortID, zenoh_flow::ZFComponentOutput>> {
        default_output_rule(state, outputs)
    }
}

#[async_trait]
impl ZFSourceTrait for CameraSource {
    async fn run(
        &self,
        _context: &mut zenoh_flow::ZFContext,
        dyn_state: &mut Box<dyn zenoh_flow::ZFStateTrait>,
    ) -> ZFResult<HashMap<zenoh_flow::ZFPortID, Arc<dyn ZFDataTrait>>> {
        let mut results: HashMap<String, Arc<dyn ZFDataTrait>> = HashMap::with_capacity(1);

        let state = downcast_mut!(CameraState, dyn_state).unwrap();

        let mut cam = zf_spin_lock!(state.camera);
        let encode_options = zf_spin_lock!(state.encode_options);

        let mut frame = core::Mat::default();
        cam.read(&mut frame).unwrap();

        let mut reduced = Mat::default();
        opencv::imgproc::resize(
            &frame,
            &mut reduced,
            opencv::core::Size::new(state.resolution.0, state.resolution.0),
            0.0,
            0.0,
            opencv::imgproc::INTER_LINEAR,
        )
        .unwrap();

        let mut buf = opencv::types::VectorOfu8::new();
        opencv::imgcodecs::imencode(".jpg", &reduced, &mut buf, &encode_options).unwrap();

        results.insert(String::from(SOURCE), zf_data!(ZFBytes(buf.into())));

        async_std::task::sleep(std::time::Duration::from_millis(state.delay)).await;

        Ok(results)
    }
}

// Also generated by macro
zenoh_flow::export_source!(register);

fn register() -> ZFResult<Box<dyn ZFSourceTrait + Send>> {
    Ok(Box::new(CameraSource) as Box<dyn ZFSourceTrait + Send>)
}

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
use opencv::{core, prelude::*, videoio};
use std::collections::HashMap;
use zenoh_flow::{
    downcast_mut,
    serde::{Deserialize, Serialize},
    types::{
        DataTrait, FnOutputRule, FnSourceRun, FutRunOutput, RunOutput, SourceTrait, StateTrait,
        ZFContext, ZFResult,
    },
    zenoh_flow_derive::ZFState,
    zf_data, zf_spin_lock,
};
use zenoh_flow_examples::ZFBytes;

#[derive(Debug)]
struct CameraSource {
    pub state: CameraState,
}

#[derive(Clone)]
struct InnerCameraAccess {
    pub camera: Arc<Mutex<videoio::VideoCapture>>,
    pub encode_options: Arc<Mutex<opencv::types::VectorOfi32>>,
}

#[derive(Serialize, Deserialize, ZFState, Clone)]
struct CameraState {
    #[serde(skip_serializing, skip_deserializing)]
    pub inner: Option<InnerCameraAccess>,

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

impl CameraSource {
    fn new(configuration: HashMap<String, String>) -> Self {
        let configured_resolution = match configuration.get("resolution") {
            Some(res) => {
                let v = res.split("x").collect::<Vec<&str>>();
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
        let mut camera = match configuration.get("camera") {
            Some(configured_camera) => {
                videoio::VideoCapture::from_file(configured_camera, videoio::CAP_ANY).unwrap()
            }
            None => videoio::VideoCapture::new(0, videoio::CAP_ANY).unwrap(),
        };
        let opened = videoio::VideoCapture::is_opened(&camera).unwrap();
        if !opened {
            panic!("Unable to open default camera!");
        }
        let encode_options = opencv::types::VectorOfi32::new();
        let inner = InnerCameraAccess {
            camera: Arc::new(Mutex::new(camera)),
            encode_options: Arc::new(Mutex::new(encode_options)),
        };

        let state = CameraState {
            inner: Some(inner),
            resolution: configured_resolution,
            delay,
        };

        Self { state }
    }

    async fn run_1(ctx: ZFContext) -> RunOutput {
        let mut results: HashMap<String, Arc<dyn DataTrait>> = HashMap::new();

        let mut guard = ctx.async_lock().await;
        let mut _state = downcast_mut!(CameraState, guard.state).unwrap(); //downcasting to right type

        let inner = _state.inner.as_ref().unwrap();
        {
            // just to force rust understand that the not sync variables are dropped before the sleep
            let mut cam = zf_spin_lock!(inner.camera);
            let encode_options = zf_spin_lock!(inner.encode_options);

            let mut frame = core::Mat::default();
            cam.read(&mut frame).unwrap();

            let mut reduced = Mat::default();
            opencv::imgproc::resize(
                &frame,
                &mut reduced,
                opencv::core::Size::new(_state.resolution.0, _state.resolution.0),
                0.0,
                0.0,
                opencv::imgproc::INTER_LINEAR,
            )
            .unwrap();

            let mut buf = opencv::types::VectorOfu8::new();
            opencv::imgcodecs::imencode(".jpg", &reduced, &mut buf, &encode_options).unwrap();

            // let data = ZFOpenCVBytes {bytes: Mutex::new(RefCell::new(buf))};

            let data = ZFBytes {
                bytes: buf.to_vec(),
            };

            results.insert(String::from(SOURCE), zf_data!(data));

            drop(cam);
            drop(encode_options);
            drop(frame);
            drop(reduced);
        }

        async_std::task::sleep(std::time::Duration::from_millis(_state.delay.clone())).await;

        Ok(results)
    }
}

impl SourceTrait for CameraSource {
    fn get_run(&self, ctx: ZFContext) -> FnSourceRun {
        let gctx = ctx.lock();
        match gctx.mode {
            0 => Box::new(|ctx: ZFContext| -> FutRunOutput { Box::pin(Self::run_1(ctx)) }),
            _ => panic!("No way"),
        }
    }

    fn get_output_rule(&self, _ctx: ZFContext) -> Box<FnOutputRule> {
        Box::new(zenoh_flow::default_output_rule)
    }

    fn get_state(&self) -> Box<dyn StateTrait> {
        Box::new(self.state.clone())
    }
}

// //Also generated by macro
zenoh_flow::export_source!(register);

extern "C" fn register(
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<Box<dyn zenoh_flow::SourceTrait + Send>> {
    match configuration {
        Some(config) => {
            Ok(Box::new(CameraSource::new(config)) as Box<dyn zenoh_flow::SourceTrait + Send>)
        }
        None => {
            Ok(Box::new(CameraSource::new(HashMap::new()))
                as Box<dyn zenoh_flow::SourceTrait + Send>)
        }
    }
}

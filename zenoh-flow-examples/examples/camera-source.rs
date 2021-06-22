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
use rand::Rng;
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use zenoh_flow::{
    downcast_mut,
    operator::{DataTrait, FnSourceRun, FutRunResult, RunResult, SourceTrait, StateTrait},
    serde::{Deserialize, Serialize},
    types::{ZFContext, ZFError, ZFLinkId, ZFResult},
    zenoh_flow_macros::ZFState,
    zf_data, zf_spin_lock,
};
use zenoh_flow_examples::{ZFBytes, ZFOpenCVBytes};

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
        let configured_camera = configuration.get("camera").unwrap();
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

        let mut camera =
            videoio::VideoCapture::from_file(configured_camera, videoio::CAP_ANY).unwrap(); // 0 is the default camera
        let opened = videoio::VideoCapture::is_opened(&camera).unwrap();
        if !opened {
            panic!("Unable to open default camera!");
        }
        let mut encode_options = opencv::types::VectorOfi32::new();
        encode_options.push(opencv::imgcodecs::IMWRITE_JPEG_QUALITY);
        encode_options.push(90);
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

    async fn run_1(ctx: ZFContext) -> RunResult {
        let mut results: HashMap<ZFLinkId, Arc<Box<dyn DataTrait>>> = HashMap::new();

        let mut guard = ctx.lock();
        let mut _state = downcast_mut!(CameraState, guard.state).unwrap(); //downcasting to right type

        let inner = _state.inner.as_ref().unwrap();

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
        opencv::imgcodecs::imencode(".jpeg", &reduced, &mut buf, &encode_options).unwrap();

        // let data = ZFOpenCVBytes {bytes: Mutex::new(RefCell::new(buf))};

        let data = ZFBytes {
            bytes: buf.to_vec(),
        };

        results.insert(String::from(SOURCE), zf_data!(data));

        async_std::task::sleep(std::time::Duration::from_millis(_state.delay));

        drop(cam);
        drop(encode_options);

        Ok(results)
    }
}

impl SourceTrait for CameraSource {
    fn get_run(&self, ctx: ZFContext) -> FnSourceRun {
        let gctx = ctx.lock();
        match gctx.mode {
            0 => Box::new(|ctx: ZFContext| -> FutRunResult { Box::pin(Self::run_1(ctx)) }),
            _ => panic!("No way"),
        }
    }

    fn get_state(&self) -> Box<dyn StateTrait> {
        Box::new(self.state.clone())
    }
}

// //Also generated by macro
zenoh_flow::export_source!(register);

extern "C" fn register(
    registrar: &mut dyn zenoh_flow::loader::ZFSourceRegistrarTrait,
    configuration: Option<HashMap<String, String>>,
) -> ZFResult<()> {
    match configuration {
        Some(config) => {
            registrar.register_zfsource(
                "camera-source",
                Box::new(CameraSource::new(config))
                    as Box<dyn zenoh_flow::operator::SourceTrait + Send>,
            );
            Ok(())
        }
        None => Err(ZFError::MissingConfiguration),
    }
}

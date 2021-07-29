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
        DataTrait, FnOutputRule, FnSourceRun, FutRunResult, RunResult, SourceTrait, StateTrait,
        ZFContext, ZFError, ZFResult,
    },
    zenoh_flow_derive::ZFState,
    zf_data, zf_spin_lock,
};
use zenoh_flow_examples::ZFBytes;

#[derive(Debug)]
struct VideoSource {
    pub state: VideoState,
}

#[derive(Clone)]
struct InnerVideoAccess {
    pub camera: Arc<Mutex<videoio::VideoCapture>>,
    pub encode_options: Arc<Mutex<opencv::types::VectorOfi32>>,
}

#[derive(Serialize, Deserialize, ZFState, Clone)]
struct VideoState {
    #[serde(skip_serializing, skip_deserializing)]
    pub inner: Option<InnerVideoAccess>,
    pub delay: u64,
    pub source_file: String,
}

// because of opencv
impl std::fmt::Debug for VideoState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "VideoState: file:{:?} delay:{:?}",
            self.source_file, self.delay
        )
    }
}

static SOURCE: &str = "Frame";

impl VideoSource {
    fn new(configuration: HashMap<String, String>) -> Self {
        let source_file = configuration.get("file").unwrap();
        let delay = match configuration.get("fps") {
            Some(fps) => {
                let fps = fps.parse::<f64>().unwrap();
                let delay: f64 = 1f64 / fps;
                (delay * 1000f64) as u64
            }
            None => 40,
        };

        let camera = videoio::VideoCapture::from_file(source_file, videoio::CAP_ANY).unwrap(); // 0 is the default camera
        let opened = videoio::VideoCapture::is_opened(&camera).unwrap();
        if !opened {
            panic!("Unable to open default camera!");
        }
        let encode_options = opencv::types::VectorOfi32::new();
        let inner = InnerVideoAccess {
            camera: Arc::new(Mutex::new(camera)),
            encode_options: Arc::new(Mutex::new(encode_options)),
        };

        let state = VideoState {
            inner: Some(inner),
            source_file: source_file.to_string(),
            delay,
        };

        Self { state }
    }

    async fn run_1(ctx: ZFContext) -> RunResult {
        let mut results: HashMap<String, Arc<dyn DataTrait>> = HashMap::new();

        let mut guard = ctx.async_lock().await;
        let mut _state = downcast_mut!(VideoState, guard.state).unwrap(); //downcasting to right type

        let inner = _state.inner.as_ref().unwrap();
        {
            // just to force rust understand that the not sync variables are dropped before the sleep
            let mut cam = zf_spin_lock!(inner.camera);
            let encode_options = zf_spin_lock!(inner.encode_options);

            let mut frame = core::Mat::default();
            match cam.read(&mut frame) {
                Ok(false) => {
                    *cam = videoio::VideoCapture::from_file(&_state.source_file, videoio::CAP_ANY)
                        .unwrap(); // 0 is the default camera
                    let opened = videoio::VideoCapture::is_opened(&cam).unwrap();
                    if !opened {
                        panic!("Unable to open default camera!");
                    }
                    cam.read(&mut frame);
                }
                Ok(true) => (),
                Err(_) => {
                    *cam = videoio::VideoCapture::from_file(&_state.source_file, videoio::CAP_ANY)
                        .unwrap(); // 0 is the default camera
                    let opened = videoio::VideoCapture::is_opened(&cam).unwrap();
                    if !opened {
                        panic!("Unable to open default camera!");
                    }
                    cam.read(&mut frame);
                }
            };

            let mut buf = opencv::types::VectorOfu8::new();
            opencv::imgcodecs::imencode(".jpg", &frame, &mut buf, &encode_options).unwrap();

            // let data = ZFOpenCVBytes {bytes: Mutex::new(RefCell::new(buf))};

            let data = ZFBytes {
                bytes: buf.to_vec(),
            };

            results.insert(String::from(SOURCE), zf_data!(data));

            drop(cam);
            drop(encode_options);
            drop(frame);
        }

        async_std::task::sleep(std::time::Duration::from_millis(_state.delay)).await;

        Ok(results)
    }
}

impl SourceTrait for VideoSource {
    fn get_run(&self, ctx: ZFContext) -> FnSourceRun {
        let gctx = ctx.lock();
        match gctx.mode {
            0 => Box::new(|ctx: ZFContext| -> FutRunResult { Box::pin(Self::run_1(ctx)) }),
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
            Ok(Box::new(VideoSource::new(config)) as Box<dyn zenoh_flow::SourceTrait + Send>)
        }
        None => Err(ZFError::MissingConfiguration),
    }
}

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
use zenoh_flow::model::link::{ZFFromEndpoint, ZFToEndpoint};
use zenoh_flow::{
    downcast, downcast_mut, get_input,
    serde::{Deserialize, Serialize},
    types::{
        DataTrait, FnInputRule, FnOutputRule, FnSinkRun, FnSourceRun, FutRunResult, FutSinkResult,
        InputRuleResult, RunResult, SinkTrait, SourceTrait, StateTrait, Token, ZFContext, ZFError,
        ZFInput, ZFLinkId, ZFResult,
    },
    zenoh_flow_derive::ZFState,
    zf_data, zf_spin_lock,
};
use zenoh_flow_examples::ZFBytes;

use opencv::{core, highgui, prelude::*, videoio};

static SOURCE: &str = "Frame";
static INPUT: &str = "Frame";

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

impl CameraSource {
    fn new() -> Self {
        let mut camera = videoio::VideoCapture::new(0, videoio::CAP_ANY).unwrap(); // 0 is the default camera
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
            resolution: (800, 600),
            delay: 40,
        };

        Self { state }
    }

    async fn run_1(ctx: ZFContext) -> RunResult {
        let mut results: HashMap<ZFLinkId, Arc<dyn DataTrait>> = HashMap::new();

        let mut guard = ctx.async_lock().await;
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
        Box::new(|ctx: ZFContext| -> FutRunResult { Box::pin(Self::run_1(ctx)) })
    }

    fn get_output_rule(&self, _ctx: ZFContext) -> Box<FnOutputRule> {
        Box::new(zenoh_flow::default_output_rule)
    }

    fn get_state(&self) -> Box<dyn StateTrait> {
        Box::new(self.state.clone())
    }
}

#[derive(Debug)]
struct VideoSink {
    pub state: VideoState,
}

#[derive(Serialize, Deserialize, ZFState, Clone, Debug)]
struct VideoState {
    pub window_name: String,
}

impl VideoSink {
    pub fn new() -> Self {
        let window_name = &format!("Video-Sink");
        highgui::named_window(window_name, 1).unwrap();
        let state = VideoState {
            window_name: window_name.to_string(),
        };
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

    pub async fn run_1(ctx: ZFContext, mut inputs: ZFInput) -> ZFResult<()> {
        let guard = ctx.async_lock().await; //getting state,
        let _state = downcast!(VideoState, guard.state).unwrap(); //downcasting to right type

        let data = get_input!(ZFBytes, String::from(INPUT), inputs).unwrap();

        let decoded = opencv::imgcodecs::imdecode(
            &opencv::types::VectorOfu8::from_iter(data.bytes.clone()),
            opencv::imgcodecs::IMREAD_COLOR,
        )
        .unwrap();

        if decoded.size().unwrap().width > 0 {
            highgui::imshow(&_state.window_name, &decoded).unwrap();
        }

        highgui::wait_key(10).unwrap();
        Ok(())
    }
}

impl SinkTrait for VideoSink {
    fn get_input_rule(&self, ctx: ZFContext) -> Box<FnInputRule> {
        Box::new(Self::ir_1)
    }

    fn get_run(&self, ctx: ZFContext) -> FnSinkRun {
        Box::new(|ctx: ZFContext, inputs: ZFInput| -> FutSinkResult {
            Box::pin(Self::run_1(ctx, inputs))
        })
    }

    fn get_state(&self) -> Box<dyn StateTrait> {
        Box::new(self.state.clone())
    }
}

#[async_std::main]
async fn main() {
    let mut zf_graph = zenoh_flow::runtime::graph::DataFlowGraph::new();

    let source = Box::new(CameraSource::new());
    let sink = Box::new(VideoSink::new());

    zf_graph
        .add_static_source(
            "camera-source".to_string(),
            // "camera".to_string(),
            String::from(SOURCE),
            source,
            None,
        )
        .unwrap();

    zf_graph
        .add_static_sink(
            "video-sink".to_string(),
            // "window".to_string(),
            String::from(INPUT),
            sink,
            None,
        )
        .unwrap();

    zf_graph
        .add_link(
            ZFFromEndpoint {
                id: "camera-source".to_string(),
                output: String::from(SOURCE),
            },
            ZFToEndpoint {
                id: "video-sink".to_string(),
                input: String::from(INPUT),
            },
            None,
            None,
            None,
        )
        .unwrap();

    zf_graph.make_connections("local").await;

    let runners = zf_graph.get_runners();
    for runner in runners {
        async_std::task::spawn(async move {
            let mut runner = runner.lock().await;
            runner.run().await.unwrap();
        });
    }

    let () = std::future::pending().await;
}

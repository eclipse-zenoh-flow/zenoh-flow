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
use opencv::{core, highgui, prelude::*, videoio};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use zenoh_flow::model::link::{ZFLinkFromDescriptor, ZFLinkToDescriptor};
use zenoh_flow::{
    downcast, downcast_mut, get_input,
    model::link::ZFPortDescriptor,
    serde::{Deserialize, Serialize},
    types::{
        DataTrait, FnInputRule, FnOutputRule, FnSinkRun, FnSourceRun, FutRunOutput, FutSinkOutput,
        InputRuleOutput, RunOutput, SinkTrait, SourceTrait, StateTrait, Token, ZFContext, ZFError,
        ZFInput, ZFResult,
    },
    zenoh_flow_derive::ZFState,
    zf_data, zf_spin_lock,
};
use zenoh_flow_examples::ZFBytes;

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
        let camera = videoio::VideoCapture::new(0, videoio::CAP_ANY).unwrap(); // 0 is the default camera
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

    async fn run_1(ctx: ZFContext) -> RunOutput {
        let mut results: HashMap<String, Arc<dyn DataTrait>> = HashMap::new();

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
    fn get_run(&self, _ctx: ZFContext) -> FnSourceRun {
        Box::new(|ctx: ZFContext| -> FutRunOutput { Box::pin(Self::run_1(ctx)) })
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
        let window_name = &"Video-Sink".to_string();
        highgui::named_window(window_name, 1).unwrap();
        let state = VideoState {
            window_name: window_name.to_string(),
        };
        Self { state }
    }

    pub fn ir_1(_ctx: ZFContext, inputs: &mut HashMap<String, Token>) -> InputRuleOutput {
        if let Some(token) = inputs.get(INPUT) {
            match token {
                Token::Ready(_) => Ok(true),
                Token::NotReady => Ok(false),
            }
        } else {
            Err(ZFError::MissingInput(String::from(INPUT)))
        }
    }

    pub async fn run_1(ctx: ZFContext, mut inputs: ZFInput) -> ZFResult<()> {
        let guard = ctx.async_lock().await; //getting state,
        let _state = downcast!(VideoState, guard.state).unwrap(); //downcasting to right type

        let (_, data) = get_input!(ZFBytes, String::from(INPUT), inputs).unwrap();

        let decoded = opencv::imgcodecs::imdecode(
            &opencv::types::VectorOfu8::from_iter(data.bytes),
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
    fn get_input_rule(&self, _ctx: ZFContext) -> Box<FnInputRule> {
        Box::new(Self::ir_1)
    }

    fn get_run(&self, _ctx: ZFContext) -> FnSinkRun {
        Box::new(|ctx: ZFContext, inputs: ZFInput| -> FutSinkOutput {
            Box::pin(Self::run_1(ctx, inputs))
        })
    }

    fn get_state(&self) -> Box<dyn StateTrait> {
        Box::new(self.state.clone())
    }
}

#[async_std::main]
async fn main() {
    env_logger::init();

    let mut zf_graph = zenoh_flow::runtime::graph::DataFlowGraph::new();

    let source = Box::new(CameraSource::new());
    let sink = Box::new(VideoSink::new());
    let hlc = Arc::new(uhlc::HLC::default());

    zf_graph
        .add_static_source(
            hlc,
            "camera-source".to_string(),
            ZFPortDescriptor {
                port_id: String::from(SOURCE),
                port_type: String::from("image"),
            },
            source,
            None,
        )
        .unwrap();

    zf_graph
        .add_static_sink(
            "video-sink".to_string(),
            ZFPortDescriptor {
                port_id: String::from(INPUT),
                port_type: String::from("image"),
            },
            sink,
            None,
        )
        .unwrap();

    zf_graph
        .add_link(
            ZFLinkFromDescriptor {
                component_id: "camera-source".to_string(),
                output_id: String::from(SOURCE),
            },
            ZFLinkToDescriptor {
                component_id: "video-sink".to_string(),
                input_id: String::from(INPUT),
            },
            None,
            None,
            None,
        )
        .unwrap();

    let dot_notation = zf_graph.to_dot_notation();

    let mut file = File::create("video-pipeline.dot").unwrap();
    write!(file, "{}", dot_notation).unwrap();
    file.sync_all().unwrap();

    zf_graph.make_connections("self").await.unwrap();

    let runners = zf_graph.get_runners();
    for runner in runners {
        async_std::task::spawn(async move {
            let mut runner = runner.lock().await;
            runner.run().await.unwrap();
        });
    }

    let () = std::future::pending().await;
}

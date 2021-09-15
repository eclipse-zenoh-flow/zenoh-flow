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

use async_ctrlc::CtrlC;
use async_trait::async_trait;
use opencv::{core, highgui, prelude::*, videoio};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use zenoh_flow::async_std::stream::StreamExt;
use zenoh_flow::async_std::sync::{Arc, Mutex};
use zenoh_flow::model::link::{ZFLinkFromDescriptor, ZFLinkToDescriptor};
use zenoh_flow::zf_spin_lock;
use zenoh_flow::{
    default_input_rule, default_output_rule, downcast, get_input, model::link::ZFPortDescriptor,
    zenoh_flow_derive::ZFState, zf_data, Component, Data, InputRule, OutputRule, PortId, Sink,
    Source, ZFError,
};
use zenoh_flow::{State, ZFResult};
use zenoh_flow_examples::ZFBytes;

static SOURCE: &str = "Frame";
static INPUT: &str = "Frame";

#[derive(Debug)]
struct CameraSource;

#[derive(ZFState, Clone)]
struct CameraState {
    pub camera: Arc<Mutex<videoio::VideoCapture>>,
    pub encode_options: Arc<Mutex<opencv::types::VectorOfi32>>,
    pub resolution: (i32, i32),
    pub delay: u64,
}

// Because of opencv
impl std::fmt::Debug for CameraState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "CameraState: resolution:{:?} delay:{:?}",
            self.resolution, self.delay
        )
    }
}

impl CameraState {
    fn new() -> Self {
        let camera = videoio::VideoCapture::new(0, videoio::CAP_ANY).unwrap(); // 0 is the default camera
        let opened = videoio::VideoCapture::is_opened(&camera).unwrap();
        if !opened {
            panic!("Unable to open default camera!");
        }
        let mut encode_options = opencv::types::VectorOfi32::new();
        encode_options.push(opencv::imgcodecs::IMWRITE_JPEG_QUALITY);
        encode_options.push(90);

        Self {
            camera: Arc::new(Mutex::new(camera)),
            encode_options: Arc::new(Mutex::new(encode_options)),
            resolution: (800, 600),
            delay: 40,
        }
    }
}

#[async_trait]
impl Source for CameraSource {
    async fn run(
        &self,
        _context: &mut zenoh_flow::Context,
        dyn_state: &mut Box<dyn zenoh_flow::State>,
    ) -> zenoh_flow::ZFResult<HashMap<zenoh_flow::PortId, Arc<dyn zenoh_flow::Data>>> {
        let mut results: HashMap<zenoh_flow::PortId, Arc<dyn Data>> = HashMap::new();

        // Downcasting to right type
        let state = downcast!(CameraState, dyn_state).unwrap();

        {
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
            opencv::imgcodecs::imencode(".jpeg", &reduced, &mut buf, &encode_options).unwrap();

            let data = ZFBytes(buf.into());

            results.insert(SOURCE.into(), zf_data!(data));

            drop(cam);
            drop(encode_options);
        }

        async_std::task::sleep(std::time::Duration::from_millis(state.delay)).await;
        Ok(results)
    }
}

impl OutputRule for CameraSource {
    fn output_rule(
        &self,
        _context: &mut zenoh_flow::Context,
        state: &mut Box<dyn zenoh_flow::State>,
        outputs: &HashMap<zenoh_flow::PortId, Arc<dyn Data>>,
    ) -> zenoh_flow::ZFResult<HashMap<zenoh_flow::PortId, zenoh_flow::ComponentOutput>> {
        default_output_rule(state, outputs)
    }
}

impl Component for CameraSource {
    fn initialize(
        &self,
        _configuration: &Option<HashMap<String, String>>,
    ) -> Box<dyn zenoh_flow::State> {
        Box::new(CameraState::new())
    }

    fn clean(&self, _state: &mut Box<dyn State>) -> ZFResult<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct VideoSink;

#[derive(ZFState, Clone, Debug)]
struct VideoState {
    pub window_name: String,
}

impl VideoState {
    pub fn new() -> Self {
        let window_name = &"Video-Sink".to_string();
        highgui::named_window(window_name, 1).unwrap();
        Self {
            window_name: window_name.to_string(),
        }
    }
}

impl InputRule for VideoSink {
    fn input_rule(
        &self,
        _context: &mut zenoh_flow::Context,
        state: &mut Box<dyn zenoh_flow::State>,
        tokens: &mut HashMap<PortId, zenoh_flow::Token>,
    ) -> zenoh_flow::ZFResult<bool> {
        default_input_rule(state, tokens)
    }
}

impl Component for VideoSink {
    fn initialize(
        &self,
        _configuration: &Option<HashMap<String, String>>,
    ) -> Box<dyn zenoh_flow::State> {
        Box::new(VideoState::new())
    }

    fn clean(&self, state: &mut Box<dyn State>) -> ZFResult<()> {
        let state = downcast!(VideoState, state).ok_or(ZFError::MissingState)?;
        highgui::destroy_window(&state.window_name).unwrap();
        Ok(())
    }
}

#[async_trait]
impl Sink for VideoSink {
    async fn run(
        &self,
        _context: &mut zenoh_flow::Context,
        dyn_state: &mut Box<dyn zenoh_flow::State>,
        inputs: &mut HashMap<PortId, zenoh_flow::runtime::message::DataMessage>,
    ) -> zenoh_flow::ZFResult<()> {
        // Downcasting to right type
        let state = downcast!(VideoState, dyn_state).unwrap();

        let (_, data) = get_input!(ZFBytes, String::from(INPUT), inputs).unwrap();

        let decoded = opencv::imgcodecs::imdecode(
            &opencv::types::VectorOfu8::from_iter(data.0),
            opencv::imgcodecs::IMREAD_COLOR,
        )
        .unwrap();

        if decoded.size().unwrap().width > 0 {
            highgui::imshow(&state.window_name, &decoded).unwrap();
        }

        highgui::wait_key(10).unwrap();
        Ok(())
    }
}

#[async_std::main]
async fn main() {
    env_logger::init();

    let mut zf_graph = zenoh_flow::runtime::graph::DataFlowGraph::new();

    let source = Arc::new(CameraSource);
    let sink = Arc::new(VideoSink);
    let hlc = Arc::new(uhlc::HLC::default());

    zf_graph
        .add_static_source(
            hlc,
            "camera-source".into(),
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
            "video-sink".into(),
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
                component: "camera-source".into(),
                output: String::from(SOURCE),
            },
            ZFLinkToDescriptor {
                component: "video-sink".into(),
                input: String::from(INPUT),
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

    let mut managers = vec![];

    let runners = zf_graph.get_runners();
    for runner in &runners {
        let m = runner.start();
        managers.push(m)
    }

    let ctrlc = CtrlC::new().expect("Unable to create Ctrl-C handler");
    let mut stream = ctrlc.enumerate().take(1);
    stream.next().await;
    println!("Received Ctrl-C start teardown");

    for m in managers.iter() {
        m.kill().await.unwrap()
    }

    for runner in runners {
        runner.clean().await.unwrap();
    }
}

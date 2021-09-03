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
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use zenoh_flow::async_std::stream::StreamExt;
use zenoh_flow::async_std::sync::Arc;
use zenoh_flow::model::link::{ZFLinkFromDescriptor, ZFLinkToDescriptor};
use zenoh_flow::{
    default_input_rule, default_output_rule, ZFComponent, ZFComponentInputRule,
    ZFComponentOutputRule, ZFDataTrait, ZFSinkTrait, ZFSourceTrait,
};
use zenoh_flow::{model::link::ZFPortDescriptor, zf_data, zf_empty_state};
use zenoh_flow_examples::ZFUsize;

static SOURCE: &str = "Counter";

static COUNTER: AtomicUsize = AtomicUsize::new(0);

struct CountSource;

impl CountSource {
    fn new(configuration: Option<HashMap<String, String>>) -> Self {
        match configuration {
            Some(conf) => {
                let initial = conf.get("initial").unwrap().parse::<usize>().unwrap();
                COUNTER.store(initial, Ordering::SeqCst);
                CountSource {}
            }
            None => CountSource {},
        }
    }
}

#[async_trait]
impl ZFSourceTrait for CountSource {
    async fn run(
        &self,
        _state: &mut Box<dyn zenoh_flow::ZFStateTrait>,
    ) -> zenoh_flow::ZFResult<HashMap<zenoh_flow::ZFPortID, Arc<dyn zenoh_flow::ZFDataTrait>>> {
        let mut results: HashMap<String, Arc<dyn ZFDataTrait>> = HashMap::new();
        let d = ZFUsize(COUNTER.fetch_add(1, Ordering::AcqRel));
        results.insert(String::from(SOURCE), zf_data!(d));
        async_std::task::sleep(std::time::Duration::from_secs(1)).await;
        Ok(results)
    }
}

impl ZFComponentOutputRule for CountSource {
    fn output_rule(
        &self,
        state: &mut Box<dyn zenoh_flow::ZFStateTrait>,
        outputs: &HashMap<String, Arc<dyn ZFDataTrait>>,
    ) -> zenoh_flow::ZFResult<HashMap<zenoh_flow::ZFPortID, zenoh_flow::ZFComponentOutput>> {
        default_output_rule(state, outputs)
    }
}

impl ZFComponent for CountSource {
    fn initial_state(
        &self,
        _configuration: &Option<HashMap<String, String>>,
    ) -> Box<dyn zenoh_flow::ZFStateTrait> {
        zf_empty_state!()
    }
}

struct ExampleGenericSink;

#[async_trait]
impl ZFSinkTrait for ExampleGenericSink {
    async fn run(
        &self,
        _state: &mut Box<dyn zenoh_flow::ZFStateTrait>,
        inputs: &mut HashMap<String, zenoh_flow::runtime::message::ZFDataMessage>,
    ) -> zenoh_flow::ZFResult<()> {
        println!("#######");
        for (k, v) in inputs {
            println!("Example Generic Sink Received on LinkId {:?} -> {:?}", k, v);
        }
        println!("#######");
        Ok(())
    }
}

impl ZFComponentInputRule for ExampleGenericSink {
    fn input_rule(
        &self,
        state: &mut Box<dyn zenoh_flow::ZFStateTrait>,
        tokens: &mut HashMap<String, zenoh_flow::Token>,
    ) -> zenoh_flow::ZFResult<bool> {
        default_input_rule(state, tokens)
    }
}

impl ZFComponent for ExampleGenericSink {
    fn initial_state(
        &self,
        _configuration: &Option<HashMap<String, String>>,
    ) -> Box<dyn zenoh_flow::ZFStateTrait> {
        zf_empty_state!()
    }
}

#[async_std::main]
async fn main() {
    env_logger::init();

    let mut zf_graph = zenoh_flow::runtime::graph::DataFlowGraph::new();

    let source = Box::new(CountSource::new(None));
    let sink = Box::new(ExampleGenericSink {});
    let hlc = Arc::new(uhlc::HLC::default());

    zf_graph
        .add_static_source(
            hlc,
            "counter-source".to_string(),
            ZFPortDescriptor {
                port_id: String::from(SOURCE),
                port_type: String::from("int"),
            },
            source,
            None,
        )
        .unwrap();

    zf_graph
        .add_static_sink(
            "generic-sink".to_string(),
            ZFPortDescriptor {
                port_id: String::from(SOURCE),
                port_type: String::from("int"),
            },
            sink,
            None,
        )
        .unwrap();

    zf_graph
        .add_link(
            ZFLinkFromDescriptor {
                component_id: "counter-source".to_string(),
                output_id: String::from(SOURCE),
            },
            ZFLinkToDescriptor {
                component_id: "generic-sink".to_string(),
                input_id: String::from(SOURCE),
            },
            None,
            None,
            None,
        )
        .unwrap();

    let dot_notation = zf_graph.to_dot_notation();

    let mut file = File::create("simple-pipeline.dot").unwrap();
    write!(file, "{}", dot_notation).unwrap();
    file.sync_all().unwrap();

    zf_graph.make_connections("self").await.unwrap();

    let mut managers = vec![];

    let runners = zf_graph.get_runners();
    for runner in runners {
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

    futures::future::join_all(managers).await;
}

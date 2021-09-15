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
    default_input_rule, default_output_rule, Component, Context, Data, PortId,
    ZFComponentInputRule, ZFComponentOutputRule, ZFSinkTrait, ZFSourceTrait,
};
use zenoh_flow::{model::link::ZFPortDescriptor, zf_data, zf_empty_state};
use zenoh_flow::{StateTrait, ZFResult};
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
        _context: &mut Context,
        _state: &mut Box<dyn zenoh_flow::StateTrait>,
    ) -> zenoh_flow::ZFResult<HashMap<zenoh_flow::PortId, Arc<dyn zenoh_flow::Data>>> {
        let mut results: HashMap<PortId, Arc<dyn Data>> = HashMap::new();
        let d = ZFUsize(COUNTER.fetch_add(1, Ordering::AcqRel));
        results.insert(SOURCE.into(), zf_data!(d));
        async_std::task::sleep(std::time::Duration::from_secs(1)).await;
        Ok(results)
    }
}

impl ZFComponentOutputRule for CountSource {
    fn output_rule(
        &self,
        _context: &mut Context,
        state: &mut Box<dyn zenoh_flow::StateTrait>,
        outputs: &HashMap<PortId, Arc<dyn Data>>,
    ) -> zenoh_flow::ZFResult<HashMap<zenoh_flow::PortId, zenoh_flow::ZFComponentOutput>> {
        default_output_rule(state, outputs)
    }
}

impl Component for CountSource {
    fn initialize(
        &self,
        _configuration: &Option<HashMap<String, String>>,
    ) -> Box<dyn zenoh_flow::StateTrait> {
        zf_empty_state!()
    }

    fn clean(&self, _state: &mut Box<dyn StateTrait>) -> ZFResult<()> {
        Ok(())
    }
}

struct ExampleGenericSink;

#[async_trait]
impl ZFSinkTrait for ExampleGenericSink {
    async fn run(
        &self,
        _context: &mut Context,
        _state: &mut Box<dyn zenoh_flow::StateTrait>,
        inputs: &mut HashMap<PortId, zenoh_flow::runtime::message::ZFDataMessage>,
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
        _context: &mut Context,
        state: &mut Box<dyn zenoh_flow::StateTrait>,
        tokens: &mut HashMap<PortId, zenoh_flow::Token>,
    ) -> zenoh_flow::ZFResult<bool> {
        default_input_rule(state, tokens)
    }
}

impl Component for ExampleGenericSink {
    fn initialize(
        &self,
        _configuration: &Option<HashMap<String, String>>,
    ) -> Box<dyn zenoh_flow::StateTrait> {
        zf_empty_state!()
    }

    fn clean(&self, _state: &mut Box<dyn StateTrait>) -> ZFResult<()> {
        Ok(())
    }
}

#[async_std::main]
async fn main() {
    env_logger::init();

    let mut zf_graph = zenoh_flow::runtime::graph::DataFlowGraph::new();

    let source = Arc::new(CountSource::new(None));
    let sink = Arc::new(ExampleGenericSink {});
    let hlc = Arc::new(uhlc::HLC::default());

    zf_graph
        .add_static_source(
            hlc,
            "counter-source".into(),
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
            "generic-sink".into(),
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
                component: "counter-source".into(),
                output: String::from(SOURCE),
            },
            ZFLinkToDescriptor {
                component: "generic-sink".into(),
                input: String::from(SOURCE),
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

    futures::future::join_all(managers).await;

    for runner in runners {
        runner.clean().await.unwrap();
    }
}

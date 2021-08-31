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

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use zenoh_flow::async_std::stream::StreamExt;
use zenoh_flow::async_std::sync::Arc;
use zenoh_flow::model::link::{ZFLinkFromDescriptor, ZFLinkToDescriptor};
use zenoh_flow::{
    model::link::ZFPortDescriptor,
    serde::{Deserialize, Serialize},
    types::{
        DataTrait, FnInputRule, FnOutputRule, FnSinkRun, FnSourceRun, FutRunOutput, FutSinkOutput,
        InputRuleOutput, RunOutput, SinkTrait, SourceTrait, StateTrait, Token, ZFContext, ZFInput,
        ZFResult,
    },
    zenoh_flow_derive::ZFState,
    zf_data, zf_empty_state,
};
use zenoh_flow_examples::RandomData;

static SOURCE: &str = "Counter";

static COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Serialize, Deserialize, Debug, ZFState)]
struct CountSource {}

impl CountSource {
    fn new(configuration: Option<HashMap<String, String>>) -> Self {
        match configuration {
            Some(conf) => {
                let initial = conf.get("initial").unwrap().parse::<u64>().unwrap();
                COUNTER.store(initial, Ordering::SeqCst);
                CountSource {}
            }
            None => CountSource {},
        }
    }

    async fn run_1(_ctx: ZFContext) -> RunOutput {
        let mut results: HashMap<String, Arc<dyn DataTrait>> = HashMap::new();
        let d = RandomData {
            d: COUNTER.fetch_add(1, Ordering::AcqRel),
        };
        results.insert(String::from(SOURCE), zf_data!(d));
        async_std::task::sleep(std::time::Duration::from_secs(1)).await;
        Ok(results)
    }
}

impl SourceTrait for CountSource {
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
        zf_empty_state!()
    }
}

#[derive(Serialize, Deserialize, Debug, ZFState)]
struct ExampleGenericSink {}

impl ExampleGenericSink {
    pub fn ir_1(_ctx: ZFContext, _inputs: &mut HashMap<String, Token>) -> InputRuleOutput {
        Ok(true)
    }

    pub async fn run_1(_ctx: ZFContext, inputs: ZFInput) -> ZFResult<()> {
        println!("#######");
        for (k, v) in inputs.into_iter() {
            println!("Example Generic Sink Received on LinkId {:?} -> {:?}", k, v);
        }
        println!("#######");
        Ok(())
    }
}

impl SinkTrait for ExampleGenericSink {
    fn get_input_rule(&self, ctx: ZFContext) -> Box<FnInputRule> {
        let gctx = ctx.lock();
        match gctx.mode {
            0 => Box::new(Self::ir_1),
            _ => panic!("No way"),
        }
    }

    fn get_run(&self, ctx: ZFContext) -> FnSinkRun {
        let gctx = ctx.lock();
        match gctx.mode {
            0 => Box::new(|ctx: ZFContext, inputs: ZFInput| -> FutSinkOutput {
                Box::pin(Self::run_1(ctx, inputs))
            }),
            _ => panic!("No way"),
        }
    }

    fn get_state(&self) -> Box<dyn StateTrait> {
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

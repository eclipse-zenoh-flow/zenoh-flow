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

use async_std::sync::Arc;
use async_trait::async_trait;
use flume::{bounded, Receiver};
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::atomic::{AtomicUsize, Ordering};
use zenoh_flow::model::link::{LinkFromDescriptor, LinkToDescriptor, PortDescriptor};
use zenoh_flow::runtime::dataflow::instance::DataflowInstance;
use zenoh_flow::runtime::RuntimeContext;
use zenoh_flow::zenoh_flow_derive::ZFData;
use zenoh_flow::{
    default_input_rule, default_output_rule, zf_empty_state, Context, Data, Deserializable, Node,
    NodeOutput, Operator, PortId, Sink, Source, State, ZFData, ZFError, ZFResult,
};

// Data Type

#[derive(Debug, Clone, ZFData)]
pub struct ZFUsize(pub usize);

impl ZFData for ZFUsize {
    fn try_serialize(&self) -> ZFResult<Vec<u8>> {
        Ok(self.0.to_ne_bytes().to_vec())
    }
}

impl Deserializable for ZFUsize {
    fn try_deserialize(bytes: &[u8]) -> ZFResult<Self>
    where
        Self: Sized,
    {
        let value =
            usize::from_ne_bytes(bytes.try_into().map_err(|_| ZFError::DeseralizationError)?);
        Ok(ZFUsize(value))
    }
}

static SOURCE: &str = "Counter";
static DESTINATION: &str = "Counter";

static COUNTER: AtomicUsize = AtomicUsize::new(0);

// SOURCE

struct CountSource {
    rx: Receiver<()>,
}

unsafe impl Send for CountSource {}
unsafe impl Sync for CountSource {}

impl CountSource {
    pub fn new(rx: Receiver<()>) -> Self {
        CountSource { rx }
    }
}

#[async_trait]
impl Source for CountSource {
    async fn run(&self, _context: &mut Context, _state: &mut State) -> zenoh_flow::ZFResult<Data> {
        let _ = self.rx.recv_async().await;
        COUNTER.fetch_add(1, Ordering::AcqRel);
        let d = ZFUsize(COUNTER.load(Ordering::Relaxed));
        Ok(Data::from::<ZFUsize>(d))
    }
}

impl Node for CountSource {
    fn initialize(&self, _configuration: &Option<HashMap<String, String>>) -> State {
        zf_empty_state!()
    }

    fn finalize(&self, _state: &mut State) -> ZFResult<()> {
        Ok(())
    }
}

// SINK

struct ExampleGenericSink;

#[async_trait]
impl Sink for ExampleGenericSink {
    async fn run(
        &self,
        _context: &mut Context,
        _state: &mut State,
        mut input: zenoh_flow::runtime::message::DataMessage,
    ) -> zenoh_flow::ZFResult<()> {
        let data = input.data.try_get::<ZFUsize>()?;

        assert_eq!(data.0, COUNTER.load(Ordering::Relaxed));

        println!("Example Generic Sink Received: {:?}", input);
        Ok(())
    }
}

impl Node for ExampleGenericSink {
    fn initialize(&self, _configuration: &Option<HashMap<String, String>>) -> State {
        zf_empty_state!()
    }

    fn finalize(&self, _state: &mut State) -> ZFResult<()> {
        Ok(())
    }
}

// OPERATOR

#[derive(Debug)]
struct NoOp;

impl Operator for NoOp {
    fn input_rule(
        &self,
        _context: &mut zenoh_flow::Context,
        state: &mut State,
        tokens: &mut HashMap<PortId, zenoh_flow::Token>,
    ) -> zenoh_flow::ZFResult<bool> {
        default_input_rule(state, tokens)
    }

    fn run(
        &self,
        _context: &mut zenoh_flow::Context,
        _state: &mut State,
        inputs: &mut HashMap<PortId, zenoh_flow::runtime::message::DataMessage>,
    ) -> zenoh_flow::ZFResult<HashMap<zenoh_flow::PortId, Data>> {
        let mut results: HashMap<PortId, Data> = HashMap::new();

        let data = inputs
            .get_mut(SOURCE)
            .ok_or_else(|| ZFError::InvalidData("No data".to_string()))?
            .data
            .try_get::<ZFUsize>()?;

        assert_eq!(data.0, COUNTER.load(Ordering::Relaxed));

        results.insert(DESTINATION.into(), Data::from::<ZFUsize>(data.clone()));
        Ok(results)
    }

    fn output_rule(
        &self,
        _context: &mut zenoh_flow::Context,
        state: &mut State,
        outputs: HashMap<PortId, Data>,
    ) -> zenoh_flow::ZFResult<HashMap<zenoh_flow::PortId, NodeOutput>> {
        default_output_rule(state, outputs)
    }
}

impl Node for NoOp {
    fn initialize(&self, _configuration: &Option<HashMap<String, String>>) -> State {
        zf_empty_state!()
    }

    fn finalize(&self, _state: &mut State) -> ZFResult<()> {
        Ok(())
    }
}

// Run dataflow in single runtime
async fn single_runtime() {
    env_logger::init();

    let (tx, rx) = bounded::<()>(1); // Channel used to trigger source

    let session =
        async_std::sync::Arc::new(zenoh::net::open(zenoh::net::config::peer()).await.unwrap());
    let hlc = async_std::sync::Arc::new(uhlc::HLC::default());
    let rt_uuid = uuid::Uuid::new_v4();
    let ctx = RuntimeContext {
        session,
        hlc,
        runtime_name: format!("test-runtime-{}", rt_uuid).into(),
        runtime_uuid: rt_uuid,
    };

    let mut dataflow =
        zenoh_flow::runtime::dataflow::Dataflow::new(ctx.clone(), "test".into(), None);

    let source = Arc::new(CountSource::new(rx));
    let sink = Arc::new(ExampleGenericSink {});
    let operator = Arc::new(NoOp {});

    dataflow.add_static_source(
        "counter-source".into(),
        None,
        PortDescriptor {
            port_id: String::from(SOURCE),
            port_type: String::from("int"),
        },
        source.initialize(&None),
        source,
    );

    dataflow.add_static_sink(
        "generic-sink".into(),
        PortDescriptor {
            port_id: String::from(SOURCE),
            port_type: String::from("int"),
        },
        sink.initialize(&None),
        sink,
    );

    dataflow.add_static_operator(
        "noop".into(),
        vec![PortDescriptor {
            port_id: String::from(SOURCE),
            port_type: String::from("int"),
        }],
        vec![PortDescriptor {
            port_id: String::from(DESTINATION),
            port_type: String::from("int"),
        }],
        operator.initialize(&None),
        operator,
    );

    dataflow
        .add_link(
            LinkFromDescriptor {
                node: "counter-source".into(),
                output: String::from(SOURCE),
            },
            LinkToDescriptor {
                node: "noop".into(),
                input: String::from(SOURCE),
            },
            None,
            None,
            None,
        )
        .unwrap();

    dataflow
        .add_link(
            LinkFromDescriptor {
                node: "noop".into(),
                output: String::from(DESTINATION),
            },
            LinkToDescriptor {
                node: "generic-sink".into(),
                input: String::from(SOURCE),
            },
            None,
            None,
            None,
        )
        .unwrap();

    let instance = DataflowInstance::try_instantiate(dataflow).unwrap();

    let mut managers = vec![];

    let runners = instance.get_runners();
    for runner in &runners {
        let m = runner.start();
        managers.push(m)
    }

    tx.send_async(()).await.unwrap();

    zenoh_flow::async_std::task::sleep(std::time::Duration::from_secs(1)).await;

    for m in managers.iter() {
        m.kill().await.unwrap()
    }

    futures::future::join_all(managers).await;
}

#[test]
fn run_single_runtime() {
    let h1 = async_std::task::spawn(async move { single_runtime().await });

    async_std::task::block_on(async move { h1.await })
}

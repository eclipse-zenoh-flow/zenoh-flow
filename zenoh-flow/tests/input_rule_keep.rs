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
use zenoh_flow::model::link::{LinkFromDescriptor, LinkToDescriptor, PortDescriptor};
use zenoh_flow::runtime::dataflow::instance::DataflowInstance;
use zenoh_flow::runtime::RuntimeContext;
use zenoh_flow::zenoh_flow_derive::ZFData;
use zenoh_flow::{
    default_output_rule, zf_empty_state, Configuration, Context, Data, Deserializable, Node,
    NodeOutput, Operator, PortId, Sink, Source, State, Token, ZFData, ZFError, ZFResult, ZFState,
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

static SOURCE_1: &str = "Source1";
static SOURCE_2: &str = "Source2";
static DESTINATION: &str = "Counter";

// SOURCE 1

struct Source1 {
    rx: Receiver<()>,
}

unsafe impl Send for Source1 {}
unsafe impl Sync for Source1 {}

impl Source1 {
    pub fn new(rx: Receiver<()>) -> Self {
        Source1 { rx }
    }
}

#[async_trait]
impl Source for Source1 {
    async fn run(&self, _context: &mut Context, _state: &mut State) -> zenoh_flow::ZFResult<Data> {
        let _ = self.rx.recv_async().await;
        Ok(Data::from::<ZFUsize>(ZFUsize(1)))
    }
}

impl Node for Source1 {
    fn initialize(&self, _configuration: &Option<Configuration>) -> ZFResult<State> {
        zf_empty_state!()
    }

    fn finalize(&self, _state: &mut State) -> ZFResult<()> {
        Ok(())
    }
}

// SOURCE 2

struct Source2 {
    rx: Receiver<()>,
}

#[derive(Debug)]
struct Source2State {
    pub value: usize,
}

impl ZFState for Source2State {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

unsafe impl Send for Source2 {}
unsafe impl Sync for Source2 {}

impl Source2 {
    pub fn new(rx: Receiver<()>) -> Self {
        Source2 { rx }
    }
}

#[async_trait]
impl Source for Source2 {
    async fn run(&self, _context: &mut Context, state: &mut State) -> zenoh_flow::ZFResult<Data> {
        let _ = self.rx.recv_async().await;
        let mut s = state.try_get::<Source2State>()?;
        s.value += 2;
        Ok(Data::from::<ZFUsize>(ZFUsize(s.value)))
    }
}

impl Node for Source2 {
    fn initialize(&self, _configuration: &Option<Configuration>) -> ZFResult<State> {
        Ok(State::from(Source2State { value: 0 }))
    }

    fn finalize(&self, _state: &mut State) -> ZFResult<()> {
        Ok(())
    }
}

// SINK

struct ExampleGenericSink;

#[derive(Debug)]
struct SinkState {
    nb_calls: usize,
}

impl ZFState for SinkState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[async_trait]
impl Sink for ExampleGenericSink {
    async fn run(
        &self,
        _context: &mut Context,
        state: &mut State,
        mut input: zenoh_flow::runtime::message::DataMessage,
    ) -> zenoh_flow::ZFResult<()> {
        let data = input.data.try_get::<ZFUsize>()?;
        let mut s = state.try_get::<SinkState>()?;
        s.nb_calls += 1;

        if s.nb_calls == 1 {
            assert_eq!(data.0, 3);
        } else if s.nb_calls == 2 {
            assert_eq!(data.0, 5);
        }

        println!("Example Generic Sink Received: {:?}", input);
        Ok(())
    }
}

impl Node for ExampleGenericSink {
    fn initialize(&self, _configuration: &Option<Configuration>) -> ZFResult<State> {
        Ok(State::from(SinkState { nb_calls: 0 }))
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
        _state: &mut State,
        tokens: &mut HashMap<PortId, zenoh_flow::Token>,
    ) -> zenoh_flow::ZFResult<bool> {
        let mut run = true;

        for (port_id, token) in tokens.iter_mut() {
            if port_id.as_ref() == SOURCE_1 {
                if let Token::Ready(ready_token) = token {
                    ready_token.set_action_keep();
                } else {
                    run = false;
                }
            }

            if port_id.as_ref() == SOURCE_2 {
                if let Token::Pending = token {
                    run = false;
                }
            }
        }

        Ok(run)
    }

    fn run(
        &self,
        _context: &mut zenoh_flow::Context,
        _state: &mut State,
        inputs: &mut HashMap<PortId, zenoh_flow::runtime::message::DataMessage>,
    ) -> zenoh_flow::ZFResult<HashMap<zenoh_flow::PortId, Data>> {
        let mut results: HashMap<PortId, Data> = HashMap::new();

        let mut data1_msg = inputs
            .remove(SOURCE_1)
            .ok_or_else(|| ZFError::InvalidData("No data".to_string()))?;
        let data1 = data1_msg.data.try_get::<ZFUsize>()?;

        let mut data2_msg = inputs
            .remove(SOURCE_2)
            .ok_or_else(|| ZFError::InvalidData("No data".to_string()))?;
        let data2 = data2_msg.data.try_get::<ZFUsize>()?;

        assert_eq!(data1.0, 1);

        results.insert(
            DESTINATION.into(),
            Data::from::<ZFUsize>(ZFUsize(data1.0 + data2.0)),
        );
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
    fn initialize(&self, _configuration: &Option<Configuration>) -> ZFResult<State> {
        zf_empty_state!()
    }

    fn finalize(&self, _state: &mut State) -> ZFResult<()> {
        Ok(())
    }
}

// Run dataflow in single runtime
async fn single_runtime() {
    env_logger::init();

    let (tx1, rx1) = bounded::<()>(1); // Channel used to trigger source1
    let (tx2, rx2) = bounded::<()>(2); // Channel used to trigger source2

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

    let source1 = Arc::new(Source1::new(rx1));
    let source2 = Arc::new(Source2::new(rx2));
    let sink = Arc::new(ExampleGenericSink {});
    let operator = Arc::new(NoOp {});

    dataflow.add_static_source(
        SOURCE_1.into(),
        None,
        PortDescriptor {
            port_id: String::from(SOURCE_1),
            port_type: String::from("int"),
        },
        source1.initialize(&None).unwrap(),
        source1,
    );

    dataflow.add_static_source(
        SOURCE_2.into(),
        None,
        PortDescriptor {
            port_id: String::from(SOURCE_2),
            port_type: String::from("int"),
        },
        source2.initialize(&None).unwrap(),
        source2,
    );

    dataflow.add_static_sink(
        "generic-sink".into(),
        PortDescriptor {
            port_id: String::from(SOURCE_1),
            port_type: String::from("int"),
        },
        sink.initialize(&None).unwrap(),
        sink,
    );

    dataflow.add_static_operator(
        "noop".into(),
        vec![
            PortDescriptor {
                port_id: String::from(SOURCE_1),
                port_type: String::from("int"),
            },
            PortDescriptor {
                port_id: String::from(SOURCE_2),
                port_type: String::from("int"),
            },
        ],
        vec![PortDescriptor {
            port_id: String::from(DESTINATION),
            port_type: String::from("int"),
        }],
        operator.initialize(&None).unwrap(),
        operator,
    );

    dataflow
        .add_link(
            LinkFromDescriptor {
                node: SOURCE_1.into(),
                output: String::from(SOURCE_1),
            },
            LinkToDescriptor {
                node: "noop".into(),
                input: String::from(SOURCE_1),
            },
            None,
            None,
            None,
        )
        .unwrap();

    dataflow
        .add_link(
            LinkFromDescriptor {
                node: SOURCE_2.into(),
                output: String::from(SOURCE_2),
            },
            LinkToDescriptor {
                node: "noop".into(),
                input: String::from(SOURCE_2),
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
                input: String::from(SOURCE_1),
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

    tx1.send_async(()).await.unwrap();
    tx2.send_async(()).await.unwrap();
    tx2.send_async(()).await.unwrap();

    zenoh_flow::async_std::task::sleep(std::time::Duration::from_secs(1)).await;

    for m in managers.iter() {
        m.kill().await.unwrap()
    }

    futures::future::join_all(managers).await;
}

#[test]
fn action_keep() {
    let h1 = async_std::task::spawn(async move { single_runtime().await });

    async_std::task::block_on(async move { h1.await })
}

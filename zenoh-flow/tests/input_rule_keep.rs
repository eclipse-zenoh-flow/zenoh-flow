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

mod types;

use async_std::sync::Arc;
use std::collections::HashMap;
use types::{VecSink, VecSource, ZFUsize};
use zenoh_flow::model::link::{LinkFromDescriptor, LinkToDescriptor, PortDescriptor};
use zenoh_flow::runtime::dataflow::instance::DataflowInstance;
use zenoh_flow::runtime::RuntimeContext;
use zenoh_flow::{
    default_output_rule, zf_empty_state, Configuration, Data, DeadlineMiss, Node, NodeOutput,
    Operator, PortId, State, Token, ZFError, ZFResult,
};

static SOURCE_1: &str = "Source1";
static SOURCE_2: &str = "Source2";
static OPERATOR: &str = "Operator";
static SINK: &str = "Sink";

#[derive(Debug)]
struct OperatorKeep;

impl Node for OperatorKeep {
    fn initialize(&self, _configuration: &Option<Configuration>) -> ZFResult<State> {
        zf_empty_state!()
    }

    fn finalize(&self, _state: &mut State) -> ZFResult<()> {
        Ok(())
    }
}

impl Operator for OperatorKeep {
    fn input_rule(
        &self,
        _context: &mut zenoh_flow::Context,
        _state: &mut State,
        tokens: &mut HashMap<PortId, zenoh_flow::Token>,
    ) -> zenoh_flow::ZFResult<bool> {
        let mut run = true;

        // Keep the value from SOURCE_1. We will only send a single value that is equal to 1.
        //
        // The Input Rule will only return true once both values are present.
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

        results.insert(
            SINK.into(),
            Data::from::<ZFUsize>(ZFUsize(data1.0 + data2.0)),
        );
        Ok(results)
    }

    fn output_rule(
        &self,
        _context: &mut zenoh_flow::Context,
        state: &mut State,
        outputs: HashMap<PortId, Data>,
        deadline_miss: Option<DeadlineMiss>,
    ) -> zenoh_flow::ZFResult<HashMap<zenoh_flow::PortId, NodeOutput>> {
        assert!(
            deadline_miss.is_none(),
            "Expected `deadline_miss` to be `None`."
        );
        default_output_rule(state, outputs)
    }
}

// Run dataflow in single runtime
async fn single_runtime() {
    env_logger::init();

    let (tx_sink, rx_sink) = flume::bounded::<()>(1);

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

    let source1 = Arc::new(VecSource::new(vec![1]));
    let source2 = Arc::new(VecSource::new(vec![2, 4]));
    let sink = Arc::new(VecSink::new(tx_sink, vec![3, 5]));
    let operator = Arc::new(OperatorKeep {});

    dataflow.add_static_source(
        SOURCE_1.into(),
        None,
        PortDescriptor {
            port_id: SOURCE_1.into(),
            port_type: "int".into(),
        },
        source1.initialize(&None).unwrap(),
        source1,
    );

    dataflow.add_static_source(
        SOURCE_2.into(),
        None,
        PortDescriptor {
            port_id: SOURCE_2.into(),
            port_type: "int".into(),
        },
        source2.initialize(&None).unwrap(),
        source2,
    );

    dataflow.add_static_sink(
        SINK.into(),
        PortDescriptor {
            port_id: SINK.into(),
            port_type: "int".into(),
        },
        sink.initialize(&None).unwrap(),
        sink,
    );

    dataflow.add_static_operator(
        OPERATOR.into(),
        vec![
            PortDescriptor {
                port_id: SOURCE_1.into(),
                port_type: "int".into(),
            },
            PortDescriptor {
                port_id: SOURCE_2.into(),
                port_type: "int".into(),
            },
        ],
        vec![PortDescriptor {
            port_id: SINK.into(),
            port_type: "int".into(),
        }],
        None,
        operator.initialize(&None).unwrap(),
        operator,
    );

    dataflow
        .add_link(
            LinkFromDescriptor {
                node: SOURCE_1.into(),
                output: SOURCE_1.into(),
            },
            LinkToDescriptor {
                node: OPERATOR.into(),
                input: SOURCE_1.into(),
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
                output: SOURCE_2.into(),
            },
            LinkToDescriptor {
                node: OPERATOR.into(),
                input: SOURCE_2.into(),
            },
            None,
            None,
            None,
        )
        .unwrap();

    dataflow
        .add_link(
            LinkFromDescriptor {
                node: OPERATOR.into(),
                output: SINK.into(),
            },
            LinkToDescriptor {
                node: SINK.into(),
                input: SINK.into(),
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

    // Wait for the Sink to finish asserting and then kill all nodes.
    let _ = rx_sink.recv_async().await.unwrap();

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

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
use std::time::Duration;
use types::{VecSink, VecSource, ZFUsize};
use zenoh_flow::model::link::{LinkFromDescriptor, LinkToDescriptor, PortDescriptor};
use zenoh_flow::runtime::dataflow::instance::DataflowInstance;
use zenoh_flow::runtime::RuntimeContext;
use zenoh_flow::{
    default_input_rule, default_output_rule, zf_empty_state, Configuration, Data, Node, NodeOutput,
    Operator, PortId, State, ZFError, ZFResult,
};

static SOURCE: &str = "Source";
static OPERATOR: &str = "Operator";
static SINK: &str = "Sink";

#[derive(Debug)]
struct OperatorDeadline;

impl Node for OperatorDeadline {
    fn initialize(&self, _configuration: &Option<Configuration>) -> ZFResult<State> {
        zf_empty_state!()
    }

    fn finalize(&self, _state: &mut State) -> ZFResult<()> {
        Ok(())
    }
}

impl Operator for OperatorDeadline {
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

        // Sleep for one second: the deadline miss should be triggered as it’s set to 0.5s.
        std::thread::sleep(std::time::Duration::from_secs(1));

        let mut data_msg = inputs
            .remove(SOURCE)
            .ok_or_else(|| ZFError::InvalidData("No data".to_string()))?;
        let data = data_msg.data.try_get::<ZFUsize>()?;

        results.insert(SINK.into(), Data::from::<ZFUsize>(ZFUsize(data.0)));

        Ok(results)
    }

    fn output_rule(
        &self,
        _context: &mut zenoh_flow::Context,
        state: &mut State,
        outputs: HashMap<PortId, Data>,
        deadline_miss: bool,
    ) -> zenoh_flow::ZFResult<HashMap<zenoh_flow::PortId, NodeOutput>> {
        assert!(deadline_miss, "Expected `deadline_miss` to be: true");
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

    let source = Arc::new(VecSource::new(vec![1]));
    let sink = Arc::new(VecSink::new(tx_sink, vec![1]));
    let operator = Arc::new(OperatorDeadline {});

    dataflow.add_static_source(
        SOURCE.into(),
        None,
        PortDescriptor {
            port_id: SOURCE.into(),
            port_type: "int".into(),
        },
        source.initialize(&None).unwrap(),
        source,
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
        vec![PortDescriptor {
            port_id: SOURCE.into(),
            port_type: "int".into(),
        }],
        vec![PortDescriptor {
            port_id: SINK.into(),
            port_type: "int".into(),
        }],
        Some(Duration::from_millis(500)),
        operator.initialize(&None).unwrap(),
        operator,
    );

    dataflow
        .add_link(
            LinkFromDescriptor {
                node: SOURCE.into(),
                output: SOURCE.into(),
            },
            LinkToDescriptor {
                node: OPERATOR.into(),
                input: SOURCE.into(),
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
fn local_deadline() {
    let h1 = async_std::task::spawn(async move { single_runtime().await });

    async_std::task::block_on(async move { h1.await })
}

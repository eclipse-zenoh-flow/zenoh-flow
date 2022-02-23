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
use zenoh_flow::model::link::PortDescriptor;
use zenoh_flow::model::{InputDescriptor, OutputDescriptor};
use zenoh_flow::runtime::dataflow::instance::DataflowInstance;
use zenoh_flow::runtime::dataflow::loader::{Loader, LoaderConfig};
use zenoh_flow::runtime::loops::LoopIteration;
use zenoh_flow::runtime::RuntimeContext;
use zenoh_flow::{
    default_input_rule, default_output_rule, zf_empty_state, Configuration, Data, InputToken,
    LocalDeadlineMiss, Node, NodeOutput, Operator, PortId, State, ZFError, ZFResult,
};

static SOURCE: &str = "Source";
static OPERATOR_INGRESS_OUTER: &str = "operator-ingress-outer";
static OPERATOR_INGRESS_INNER: &str = "operator-ingress-inner";
static OPERATOR_EGRESS_INNER: &str = "operator-egress-inner";
static OPERATOR_EGRESS_OUTER: &str = "operator-egress-outer";
static FEEDBACK_OUTER: &str = "feedback-outer";
static FEEDBACK_INNER: &str = "feedback-inner";
static SINK: &str = "Sink";

#[derive(Debug)]
struct OperatorIngressOuter;

impl Node for OperatorIngressOuter {
    fn initialize(&self, _configuration: &Option<Configuration>) -> ZFResult<State> {
        zf_empty_state!()
    }

    fn finalize(&self, _state: &mut State) -> ZFResult<()> {
        Ok(())
    }
}

impl Operator for OperatorIngressOuter {
    fn input_rule(
        &self,
        _context: &mut zenoh_flow::Context,
        _state: &mut State,
        tokens: &mut HashMap<PortId, zenoh_flow::InputToken>,
    ) -> zenoh_flow::ZFResult<bool> {
        if matches!(tokens.get(SOURCE).unwrap(), InputToken::Ready(_)) {
            return Ok(true);
        }

        if let InputToken::Ready(feedback_token) = tokens.get(FEEDBACK_OUTER).unwrap() {
            let loop_contexts = feedback_token.get_loop_contexts();
            assert_eq!(loop_contexts.len(), 1);
            assert!(
                loop_contexts[0].get_ingress() == &Into::<Arc<str>>::into(OPERATOR_INGRESS_OUTER)
            );

            return Ok(true);
        }

        Ok(false)
    }

    fn run(
        &self,
        _context: &mut zenoh_flow::Context,
        _state: &mut State,
        inputs: &mut HashMap<PortId, zenoh_flow::runtime::message::DataMessage>,
    ) -> zenoh_flow::ZFResult<HashMap<zenoh_flow::PortId, Data>> {
        let mut results: HashMap<PortId, Data> = HashMap::with_capacity(1);
        let data: Data;

        if let Some(mut source_input) = inputs.remove(SOURCE) {
            let source_data = source_input.get_inner_data().try_get::<ZFUsize>()?;
            data = Data::from::<ZFUsize>(ZFUsize(source_data.0));
        } else {
            let mut feedback_input = inputs.remove(FEEDBACK_OUTER).unwrap();

            let loop_contexts = feedback_input.get_loop_contexts();
            assert_eq!(loop_contexts.len(), 1);

            let loop_iteration = loop_contexts
                .iter()
                .find(|&ctx| ctx.get_ingress() == &Into::<Arc<str>>::into(OPERATOR_INGRESS_OUTER))
                .unwrap()
                .get_iteration();
            assert!(
                matches!(loop_iteration, LoopIteration::Infinite),
                "Expected LoopIteration to be Infinite, found finite"
            );

            let feedback_data = feedback_input.get_inner_data().try_get::<ZFUsize>()?;
            data = Data::from::<ZFUsize>(ZFUsize(feedback_data.0 * 2));
        }

        results.insert(OPERATOR_INGRESS_INNER.into(), data);

        Ok(results)
    }

    fn output_rule(
        &self,
        _context: &mut zenoh_flow::Context,
        state: &mut State,
        outputs: HashMap<PortId, Data>,
        deadline_miss: Option<LocalDeadlineMiss>,
    ) -> zenoh_flow::ZFResult<HashMap<zenoh_flow::PortId, NodeOutput>> {
        assert!(
            deadline_miss.is_none(),
            "Expected `deadline_miss` to be `None`"
        );
        default_output_rule(state, outputs)
    }
}

#[derive(Debug)]
struct OperatorIngressInner;

impl Node for OperatorIngressInner {
    fn initialize(&self, _configuration: &Option<Configuration>) -> ZFResult<State> {
        zf_empty_state!()
    }

    fn finalize(&self, _state: &mut State) -> ZFResult<()> {
        Ok(())
    }
}

impl Operator for OperatorIngressInner {
    fn input_rule(
        &self,
        _context: &mut zenoh_flow::Context,
        _state: &mut State,
        tokens: &mut HashMap<PortId, zenoh_flow::InputToken>,
    ) -> zenoh_flow::ZFResult<bool> {
        if matches!(
            tokens.get(OPERATOR_INGRESS_OUTER).unwrap(),
            InputToken::Ready(_)
        ) {
            return Ok(true);
        }

        if let InputToken::Ready(feedback_token) = tokens.get(FEEDBACK_INNER).unwrap() {
            assert_eq!(feedback_token.get_loop_contexts().len(), 2);
            return Ok(true);
        }

        Ok(false)
    }

    fn run(
        &self,
        _context: &mut zenoh_flow::Context,
        _state: &mut State,
        inputs: &mut HashMap<PortId, zenoh_flow::runtime::message::DataMessage>,
    ) -> zenoh_flow::ZFResult<HashMap<zenoh_flow::PortId, Data>> {
        let mut results: HashMap<PortId, Data> = HashMap::with_capacity(1);
        let data: Data;

        if let Some(mut operator_ingress_outer_input) = inputs.remove(OPERATOR_INGRESS_OUTER) {
            let loop_contexts = operator_ingress_outer_input.get_loop_contexts();
            assert_eq!(loop_contexts.len(), 1);

            let operator_ingress_outer_data = operator_ingress_outer_input
                .get_inner_data()
                .try_get::<ZFUsize>()?;
            data = Data::from::<ZFUsize>(ZFUsize(operator_ingress_outer_data.0));
        } else {
            let mut feedback_input = inputs.remove(FEEDBACK_INNER).unwrap();

            let loop_contexts = feedback_input.get_loop_contexts();
            assert_eq!(loop_contexts.len(), 2);

            let loop_iteration = loop_contexts
                .iter()
                .find(|&ctx| ctx.get_ingress() == &Into::<Arc<str>>::into(OPERATOR_INGRESS_INNER))
                .unwrap()
                .get_iteration();
            let increment = if let LoopIteration::Finite(counter) = loop_iteration {
                counter
            } else {
                return Err(ZFError::GenericError);
            };

            let feedback_data = feedback_input.get_inner_data().try_get::<ZFUsize>()?;
            data = Data::from::<ZFUsize>(ZFUsize(feedback_data.0 + increment as usize));
        }

        results.insert(OPERATOR_EGRESS_INNER.into(), data);

        Ok(results)
    }

    fn output_rule(
        &self,
        _context: &mut zenoh_flow::Context,
        state: &mut State,
        outputs: HashMap<PortId, Data>,
        deadline_miss: Option<LocalDeadlineMiss>,
    ) -> zenoh_flow::ZFResult<HashMap<zenoh_flow::PortId, NodeOutput>> {
        assert!(
            deadline_miss.is_none(),
            "Expected `deadline_miss` to be `None`"
        );
        default_output_rule(state, outputs)
    }
}

#[derive(Debug)]
struct OperatorEgressInner;

impl Node for OperatorEgressInner {
    fn initialize(&self, _configuration: &Option<Configuration>) -> ZFResult<State> {
        zf_empty_state!()
    }

    fn finalize(&self, _state: &mut State) -> ZFResult<()> {
        Ok(())
    }
}

impl Operator for OperatorEgressInner {
    fn input_rule(
        &self,
        _context: &mut zenoh_flow::Context,
        state: &mut State,
        tokens: &mut HashMap<PortId, zenoh_flow::InputToken>,
    ) -> zenoh_flow::ZFResult<bool> {
        default_input_rule(state, tokens)
    }

    fn run(
        &self,
        _context: &mut zenoh_flow::Context,
        _state: &mut State,
        inputs: &mut HashMap<PortId, zenoh_flow::runtime::message::DataMessage>,
    ) -> zenoh_flow::ZFResult<HashMap<zenoh_flow::PortId, Data>> {
        let mut results: HashMap<PortId, Data> = HashMap::with_capacity(1);

        let mut ingress_inner_input = inputs.remove(OPERATOR_INGRESS_INNER).unwrap();
        let loop_contexts = ingress_inner_input.get_loop_contexts();
        assert_eq!(loop_contexts.len(), 2);

        let loop_iteration = loop_contexts
            .iter()
            .find(|&ctx| ctx.get_egress() == &Into::<Arc<str>>::into(OPERATOR_EGRESS_INNER))
            .unwrap()
            .get_iteration();
        let iteration = if let LoopIteration::Finite(counter) = loop_iteration {
            counter
        } else {
            return Err(ZFError::GenericError);
        };

        let data = ingress_inner_input.get_inner_data().try_get::<ZFUsize>()?;

        if iteration < 5 {
            results.insert(
                FEEDBACK_INNER.into(),
                Data::from::<ZFUsize>(ZFUsize(data.0)),
            );
        } else {
            results.insert(
                OPERATOR_EGRESS_OUTER.into(),
                Data::from::<ZFUsize>(ZFUsize(data.0)),
            );
        }

        Ok(results)
    }

    fn output_rule(
        &self,
        _context: &mut zenoh_flow::Context,
        state: &mut State,
        outputs: HashMap<PortId, Data>,
        deadline_miss: Option<LocalDeadlineMiss>,
    ) -> zenoh_flow::ZFResult<HashMap<zenoh_flow::PortId, NodeOutput>> {
        assert!(
            deadline_miss.is_none(),
            "Expected `deadline_miss` to be `None`"
        );
        default_output_rule(state, outputs)
    }
}

#[derive(Debug)]
struct OperatorEgressOuter;

impl Node for OperatorEgressOuter {
    fn initialize(&self, _configuration: &Option<Configuration>) -> ZFResult<State> {
        zf_empty_state!()
    }

    fn finalize(&self, _state: &mut State) -> ZFResult<()> {
        Ok(())
    }
}

impl Operator for OperatorEgressOuter {
    fn input_rule(
        &self,
        _context: &mut zenoh_flow::Context,
        state: &mut State,
        tokens: &mut HashMap<PortId, zenoh_flow::InputToken>,
    ) -> zenoh_flow::ZFResult<bool> {
        default_input_rule(state, tokens)
    }

    fn run(
        &self,
        _context: &mut zenoh_flow::Context,
        _state: &mut State,
        inputs: &mut HashMap<PortId, zenoh_flow::runtime::message::DataMessage>,
    ) -> zenoh_flow::ZFResult<HashMap<zenoh_flow::PortId, Data>> {
        let mut results: HashMap<PortId, Data> = HashMap::with_capacity(2);

        let mut egress_inner_input = inputs.remove(OPERATOR_EGRESS_INNER).unwrap();
        let loop_contexts = egress_inner_input.get_loop_contexts();
        assert_eq!(loop_contexts.len(), 1);

        let loop_iteration = loop_contexts
            .iter()
            .find(|&ctx| ctx.get_egress() == &Into::<Arc<str>>::into(OPERATOR_EGRESS_OUTER))
            .unwrap()
            .get_iteration();

        assert!(
            matches!(loop_iteration, LoopIteration::Infinite),
            "Expected LoopIteration to be Infinite, found finite"
        );

        results.insert(
            FEEDBACK_OUTER.into(),
            egress_inner_input.get_inner_data().clone(),
        );
        results.insert(SINK.into(), egress_inner_input.get_inner_data().clone());

        Ok(results)
    }

    fn output_rule(
        &self,
        _context: &mut zenoh_flow::Context,
        state: &mut State,
        outputs: HashMap<PortId, Data>,
        deadline_miss: Option<LocalDeadlineMiss>,
    ) -> zenoh_flow::ZFResult<HashMap<zenoh_flow::PortId, NodeOutput>> {
        assert!(
            deadline_miss.is_none(),
            "Expected `deadline_miss` to be `None`"
        );
        default_output_rule(state, outputs)
    }
}

// Run dataflow in single runtime
async fn single_runtime() {
    env_logger::init();

    let (tx_sink, rx_sink) = flume::bounded::<()>(1);

    let session = Arc::new(zenoh::open(zenoh::config::Config::default()).await.unwrap());
    let hlc = async_std::sync::Arc::new(uhlc::HLC::default());
    let rt_uuid = uuid::Uuid::new_v4();
    let ctx = RuntimeContext {
        session,
        hlc,
        loader: Arc::new(Loader::new(LoaderConfig::new())),
        runtime_name: format!("test-runtime-{}", rt_uuid).into(),
        runtime_uuid: rt_uuid,
    };

    let mut dataflow =
        zenoh_flow::runtime::dataflow::Dataflow::new(ctx.clone(), "test".into(), None);

    let source = Arc::new(VecSource::new(vec![0]));
    let sink = Arc::new(VecSink::new(tx_sink, vec![45, 15]));
    let operator_ingress_outer = Arc::new(OperatorIngressOuter {});
    let operator_ingress_inner = Arc::new(OperatorIngressInner {});
    let operator_egress_inner = Arc::new(OperatorEgressInner {});
    let operator_egress_outer = Arc::new(OperatorEgressOuter {});

    dataflow
        .try_add_static_source(
            SOURCE.into(),
            None,
            PortDescriptor {
                port_id: SOURCE.into(),
                port_type: "int".into(),
            },
            source.initialize(&None).unwrap(),
            source,
        )
        .unwrap();

    dataflow
        .try_add_static_operator(
            OPERATOR_INGRESS_OUTER.into(),
            vec![PortDescriptor {
                port_id: SOURCE.into(),
                port_type: "int".into(),
            }],
            vec![PortDescriptor {
                port_id: OPERATOR_INGRESS_INNER.into(),
                port_type: "int".into(),
            }],
            None,
            operator_ingress_outer.initialize(&None).unwrap(),
            operator_ingress_outer,
        )
        .unwrap();

    dataflow
        .try_add_static_operator(
            OPERATOR_INGRESS_INNER.into(),
            vec![PortDescriptor {
                port_id: OPERATOR_INGRESS_OUTER.into(),
                port_type: "int".into(),
            }],
            vec![PortDescriptor {
                port_id: OPERATOR_EGRESS_INNER.into(),
                port_type: "int".into(),
            }],
            None,
            operator_ingress_inner.initialize(&None).unwrap(),
            operator_ingress_inner,
        )
        .unwrap();

    dataflow
        .try_add_static_operator(
            OPERATOR_EGRESS_INNER.into(),
            vec![PortDescriptor {
                port_id: OPERATOR_INGRESS_INNER.into(),
                port_type: "int".into(),
            }],
            vec![PortDescriptor {
                port_id: OPERATOR_EGRESS_OUTER.into(),
                port_type: "int".into(),
            }],
            None,
            operator_egress_inner.initialize(&None).unwrap(),
            operator_egress_inner,
        )
        .unwrap();

    dataflow
        .try_add_static_operator(
            OPERATOR_EGRESS_OUTER.into(),
            vec![PortDescriptor {
                port_id: OPERATOR_EGRESS_INNER.into(),
                port_type: "int".into(),
            }],
            vec![PortDescriptor {
                port_id: SINK.into(),
                port_type: "int".into(),
            }],
            None,
            operator_egress_outer.initialize(&None).unwrap(),
            operator_egress_outer,
        )
        .unwrap();

    dataflow
        .try_add_static_sink(
            SINK.into(),
            PortDescriptor {
                port_id: OPERATOR_EGRESS_OUTER.into(),
                port_type: "int".into(),
            },
            sink.initialize(&None).unwrap(),
            sink,
        )
        .unwrap();

    dataflow
        .try_add_link(
            OutputDescriptor {
                node: SOURCE.into(),
                output: SOURCE.into(),
            },
            InputDescriptor {
                node: OPERATOR_INGRESS_OUTER.into(),
                input: SOURCE.into(),
            },
            None,
            None,
            None,
        )
        .unwrap();

    dataflow
        .try_add_link(
            OutputDescriptor {
                node: OPERATOR_INGRESS_OUTER.into(),
                output: OPERATOR_INGRESS_INNER.into(),
            },
            InputDescriptor {
                node: OPERATOR_INGRESS_INNER.into(),
                input: OPERATOR_INGRESS_OUTER.into(),
            },
            None,
            None,
            None,
        )
        .unwrap();

    dataflow
        .try_add_link(
            OutputDescriptor {
                node: OPERATOR_INGRESS_INNER.into(),
                output: OPERATOR_EGRESS_INNER.into(),
            },
            InputDescriptor {
                node: OPERATOR_EGRESS_INNER.into(),
                input: OPERATOR_INGRESS_INNER.into(),
            },
            None,
            None,
            None,
        )
        .unwrap();

    dataflow
        .try_add_link(
            OutputDescriptor {
                node: OPERATOR_EGRESS_INNER.into(),
                output: OPERATOR_EGRESS_OUTER.into(),
            },
            InputDescriptor {
                node: OPERATOR_EGRESS_OUTER.into(),
                input: OPERATOR_EGRESS_INNER.into(),
            },
            None,
            None,
            None,
        )
        .unwrap();

    dataflow
        .try_add_link(
            OutputDescriptor {
                node: OPERATOR_EGRESS_OUTER.into(),
                output: SINK.into(),
            },
            InputDescriptor {
                node: SINK.into(),
                input: OPERATOR_EGRESS_OUTER.into(),
            },
            None,
            None,
            None,
        )
        .unwrap();

    // Incorrect loop, egress and ingress were switched.
    assert!(dataflow
        .try_add_loop(
            OPERATOR_EGRESS_OUTER.into(),
            OPERATOR_INGRESS_OUTER.into(),
            "feedback".into(),
            "int".into(),
            false
        )
        .is_err());

    // Correct loops.
    assert!(dataflow
        .try_add_loop(
            OPERATOR_INGRESS_INNER.into(),
            OPERATOR_EGRESS_INNER.into(),
            FEEDBACK_INNER.into(),
            "int".into(),
            false
        )
        .is_ok());

    assert!(dataflow
        .try_add_loop(
            OPERATOR_INGRESS_OUTER.into(),
            OPERATOR_EGRESS_OUTER.into(),
            FEEDBACK_OUTER.into(),
            "int".into(),
            true
        )
        .is_ok());

    let mut instance = DataflowInstance::try_instantiate(dataflow).unwrap();

    let ids = instance.get_nodes();
    for id in &ids {
        instance.start_node(id).await.unwrap();
    }

    // Wait for the Sink to finish asserting and then kill all nodes.
    let _ = rx_sink.recv_async().await.unwrap();

    for id in &instance.get_sources() {
        instance.stop_node(id).await.unwrap()
    }

    for id in &instance.get_operators() {
        instance.stop_node(id).await.unwrap()
    }

    for id in &instance.get_sinks() {
        instance.stop_node(id).await.unwrap()
    }
}

#[test]
fn test_loop() {
    let h1 = async_std::task::spawn(async move { single_runtime().await });

    assert!(
        async_std::task::block_on(async_std::future::timeout(
            Duration::from_secs(5),
            async move { h1.await }
        ))
        .is_ok(),
        "Deadlock detected."
    );
}

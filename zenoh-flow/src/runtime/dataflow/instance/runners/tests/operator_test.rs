//
// Copyright (c) 2022 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

use async_std::sync::{Arc, Mutex};
use std::{collections::HashMap, convert::TryInto};
use uhlc::HLC;
use zenoh::prelude::*;

use crate::{
    default_output_rule,
    runtime::{
        dataflow::{
            instance::{
                link::{LinkReceiver, LinkSender},
                runners::{
                    operator::{OperatorIO, OperatorRunner},
                    NodeRunner,
                },
            },
            loader::{Loader, LoaderConfig},
        },
        InstanceContext, RuntimeContext,
    },
    Configuration, Context, Data, DataMessage, Deserializable, DowncastAny, EmptyState, InputToken,
    Message, Node, NodeOutput, Operator, PortId, PortType, State, TokenAction, ZFData, ZFError,
    ZFResult,
};

// ZFUsize implements Data.
#[derive(Debug, Clone)]
pub struct ZFUsize(pub usize);

impl DowncastAny for ZFUsize {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

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

// Creating Operator.
struct TestOperator {
    input_1: Arc<str>,
    input_2: Arc<str>,
    output: Arc<str>,
}

impl Node for TestOperator {
    fn initialize(&self, _configuration: &Option<Configuration>) -> ZFResult<State> {
        Ok(State::from::<EmptyState>(EmptyState {}))
    }

    fn finalize(&self, _state: &mut State) -> ZFResult<()> {
        Ok(())
    }
}

impl Operator for TestOperator {
    fn input_rule(
        &self,
        _context: &mut Context,
        _state: &mut State,
        tokens: &mut HashMap<PortId, InputToken>,
    ) -> ZFResult<bool> {
        let mut run = true;

        // 1st part: KPN input rules, we return `false` if a single input is missing.
        tokens.values().for_each(|token| {
            if let InputToken::Pending = token {
                run = false
            }
        });

        // 2nd part: 1st time we return `true` we keep the value of `input_1`. Second time we
        // consume it.
        if run {
            let input_1 = tokens
                .get_mut(&self.input_1)
                .ok_or_else(|| ZFError::MissingInput((self.input_1.as_ref()).into()))?;
            if let InputToken::Ready(token) = input_1 {
                if token.get_action() == &TokenAction::Keep {
                    input_1.set_action_consume();
                } else {
                    input_1.set_action_keep();
                }
            }
        }

        Ok(run)
    }

    fn run(
        &self,
        _context: &mut Context,
        _state: &mut State,
        inputs: &mut HashMap<PortId, DataMessage>,
    ) -> ZFResult<HashMap<PortId, Data>> {
        let mut results: HashMap<PortId, Data> = HashMap::new();

        let mut data1_msg = inputs
            .remove(&self.input_1)
            .ok_or_else(|| ZFError::InvalidData("No data".to_string()))?;
        let data1 = data1_msg.data.try_get::<ZFUsize>()?;

        let mut data2_msg = inputs
            .remove(&self.input_2)
            .ok_or_else(|| ZFError::InvalidData("No data".to_string()))?;
        let data2 = data2_msg.data.try_get::<ZFUsize>()?;

        results.insert(
            self.output.clone(),
            Data::from::<ZFUsize>(ZFUsize(data1.0 + data2.0)),
        );
        Ok(results)
    }

    fn output_rule(
        &self,
        _context: &mut Context,
        state: &mut State,
        outputs: HashMap<PortId, Data>,
    ) -> ZFResult<HashMap<PortId, NodeOutput>> {
        default_output_rule(state, outputs)
    }
}

async fn send_usize(hlc: &Arc<HLC>, sender: &LinkSender, number: usize) {
    sender
        .send(Arc::new(Message::Data(DataMessage::new(
            Data::from::<ZFUsize>(ZFUsize(number)),
            hlc.new_timestamp(),
        ))))
        .await
        .unwrap();
}

async fn recv_usize(receiver: &LinkReceiver) -> usize {
    let (_, message) = receiver.recv().await.unwrap();
    if let Message::Data(mut data_message) = message.as_ref().clone() {
        return data_message.data.try_get::<ZFUsize>().unwrap().0;
    }

    panic!("Received an unexpected `Message::Control`.")
}

#[test]
fn input_rule_keep() {
    let session = zenoh::open(zenoh::config::Config::default())
        .wait()
        .unwrap();
    let hlc = Arc::new(uhlc::HLC::default());
    let uuid = uuid::Uuid::new_v4();
    let runtime_context = RuntimeContext {
        session: Arc::new(session),
        hlc: hlc.clone(),
        loader: Arc::new(Loader::new(LoaderConfig::new())),
        runtime_name: "test-runtime-input-rule-keep".into(),
        runtime_uuid: uuid,
    };
    let instance_context = InstanceContext {
        flow_id: "test-input-rule-keep-flow".into(),
        instance_id: uuid::Uuid::new_v4(),
        runtime: runtime_context,
    };

    // Creating inputs.
    let input_1: PortId = "INPUT-1".into();
    let (tx_input_1, rx_input_1) = flume::unbounded::<Arc<Message>>();
    let receiver_input_1: LinkReceiver = LinkReceiver {
        id: input_1.clone(),
        receiver: rx_input_1,
    };
    let sender_input_1: LinkSender = LinkSender {
        id: input_1.clone(),
        sender: tx_input_1,
    };

    let input_2: PortId = "INPUT-2".into();
    let (tx_input_2, rx_input_2) = flume::unbounded::<Arc<Message>>();
    let receiver_input_2: LinkReceiver = LinkReceiver {
        id: input_2.clone(),
        receiver: rx_input_2,
    };
    let sender_input_2: LinkSender = LinkSender {
        id: input_2.clone(),
        sender: tx_input_2,
    };
    let mut io_inputs: HashMap<PortId, LinkReceiver> = HashMap::with_capacity(2);
    io_inputs.insert(input_1.clone(), receiver_input_1);
    io_inputs.insert(input_2.clone(), receiver_input_2);
    let mut inputs: HashMap<PortId, PortType> = HashMap::with_capacity(2);
    inputs.insert(input_1.clone(), "usize".into());
    inputs.insert(input_2.clone(), "usize".into());

    // Creating output.
    let output: PortId = "OUTPUT".into();
    let (tx_output, rx_output) = flume::unbounded::<Arc<Message>>();
    let receiver_output: LinkReceiver = LinkReceiver {
        id: output.clone(),
        receiver: rx_output,
    };
    let sender_output: LinkSender = LinkSender {
        id: output.clone(),
        sender: tx_output,
    };
    let mut io_outputs: HashMap<PortId, Vec<LinkSender>> = HashMap::with_capacity(1);
    io_outputs.insert(output.clone(), vec![sender_output]);
    let mut outputs: HashMap<PortId, PortType> = HashMap::with_capacity(1);
    outputs.insert(output.clone(), "usize".into());

    let operator_io = OperatorIO {
        inputs: io_inputs,
        outputs: io_outputs,
    };

    let operator = TestOperator {
        input_1,
        input_2,
        output,
    };

    let operator_runner = OperatorRunner {
        id: "test".into(),
        context: instance_context.clone(),
        io: Arc::new(Mutex::new(operator_io)),
        inputs,
        outputs,
        state: Arc::new(Mutex::new(operator.initialize(&None).unwrap())),
        is_running: Arc::new(Mutex::new(false)),
        operator: Arc::new(operator),
        _library: None,
    };

    let runner = NodeRunner::new(Arc::new(operator_runner), instance_context);

    async_std::task::block_on(async {
        let runner_manager = runner.start();

        send_usize(&hlc, &sender_input_2, 2).await; // IR: false -> (Pending, 2 (consume))
        send_usize(&hlc, &sender_input_2, 4).await; // -- IR are not triggered, value is queued
        send_usize(&hlc, &sender_input_1, 1).await; // IR: true -> (1 (keep), 2 (consume))
        assert_eq!(3, recv_usize(&receiver_output).await);
        // IR: true -> (1 (consume), 4 (consume)) --- the value 4 was taken from the queue.
        assert_eq!(5, recv_usize(&receiver_output).await);

        send_usize(&hlc, &sender_input_1, 3).await; // IR: false -> (3 (consume), Pending)
        send_usize(&hlc, &sender_input_2, 6).await; // IR: true -> (3 (keep), 6 (consume))
        assert_eq!(9, recv_usize(&receiver_output).await);
        send_usize(&hlc, &sender_input_2, 8).await; // IR: true -> (3 (consume), 8 (consume))
        assert_eq!(11, recv_usize(&receiver_output).await);

        send_usize(&hlc, &sender_input_2, 10).await; // IR: false -> (Pending, 10 (consume))
        send_usize(&hlc, &sender_input_1, 5).await; // IR: true -> (5 (keep), 10 (consume))
        assert_eq!(15, recv_usize(&receiver_output).await);
        send_usize(&hlc, &sender_input_2, 12).await; // IR: true -> (5 (consume), 12 (consume))
        assert_eq!(17, recv_usize(&receiver_output).await);

        runner_manager.kill().await.unwrap();
        runner_manager.await.unwrap();
    });
}

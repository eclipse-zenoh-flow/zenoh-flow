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

use crate::model::deadline::E2EDeadlineRecord;
use crate::model::{InputDescriptor, OutputDescriptor};
use crate::runtime::dataflow::instance::link::{LinkReceiver, LinkSender};
use crate::runtime::dataflow::instance::runners::operator::{OperatorIO, OperatorRunner};
use crate::runtime::dataflow::instance::runners::NodeRunner;
use crate::runtime::dataflow::loader::{Loader, LoaderConfig};
use crate::runtime::deadline::E2EDeadline;
use crate::runtime::{InstanceContext, RuntimeContext};
use crate::{
    default_output_rule, Configuration, Context, Data, DataMessage, Deserializable, DowncastAny,
    EmptyState, InputToken, LocalDeadlineMiss, Message, Node, NodeId, NodeOutput, Operator, PortId,
    PortType, State, ZFData, ZFError, ZFResult,
};
use async_std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{collections::HashMap, convert::TryInto};
use uhlc::HLC;
use zenoh::prelude::*;

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

async fn send_usize(
    hlc: &Arc<HLC>,
    sender: &LinkSender,
    number: usize,
    deadlines: Vec<E2EDeadline>,
) {
    sender
        .send(Arc::new(Message::Data(DataMessage::new(
            Data::from::<ZFUsize>(ZFUsize(number)),
            hlc.new_timestamp(),
            deadlines,
        ))))
        .await
        .unwrap();
}

// -------------------------------------------------------------------------------------------------
// Scenarios tested:
//
// 1) a deadline has be violated -> a deadline miss message is added to the ReadyToken;
//
// 2) two deadlines: 1 was violated, the other not, only one deadline miss message added to the
//    ReadyToken;
//
// 3) deadline in the incoming message, that was violated, but it doesn’t concern the Operator =>
//    there is no deadline miss in the ReadyToken + the deadline is propagated;
//
// 4) the Operator is at the "start" of an E2EDeadline, it is indeed propagated.
// -------------------------------------------------------------------------------------------------
struct TestOperatorDeadlineViolated {
    input1: Arc<str>,
    output: Arc<str>,
}

impl Node for TestOperatorDeadlineViolated {
    fn initialize(&self, _configuration: &Option<Configuration>) -> ZFResult<State> {
        Ok(State::from::<EmptyState>(EmptyState {}))
    }

    fn finalize(&self, _state: &mut State) -> ZFResult<()> {
        Ok(())
    }
}

impl Operator for TestOperatorDeadlineViolated {
    fn input_rule(
        &self,
        _context: &mut Context,
        _state: &mut State,
        tokens: &mut HashMap<PortId, InputToken>,
    ) -> ZFResult<bool> {
        let token_input1 = tokens.get(&self.input1).unwrap();
        match token_input1 {
            InputToken::Pending => panic!("Unexpected `Pending` token"),
            InputToken::Ready(data_token) => {
                assert_eq!(data_token.get_missed_end_to_end_deadlines().len(), 1);
            }
        }

        Ok(true)
    }

    fn run(
        &self,
        _context: &mut Context,
        _state: &mut State,
        inputs: &mut HashMap<PortId, DataMessage>,
    ) -> ZFResult<HashMap<PortId, Data>> {
        let mut results: HashMap<PortId, Data> = HashMap::new();

        let mut data_msg = inputs
            .remove(&self.input1)
            .ok_or_else(|| ZFError::InvalidData("No data".to_string()))?;

        let data = data_msg.data.try_get::<ZFUsize>()?;

        results.insert(self.output.clone(), Data::from::<ZFUsize>(ZFUsize(data.0)));
        Ok(results)
    }

    fn output_rule(
        &self,
        _context: &mut Context,
        state: &mut State,
        outputs: HashMap<PortId, Data>,
        local_deadline_miss: Option<LocalDeadlineMiss>,
    ) -> ZFResult<HashMap<PortId, NodeOutput>> {
        println!("output_rule");
        assert!(
            local_deadline_miss.is_none(),
            "Expected `deadline_miss` to be `None`."
        );
        default_output_rule(state, outputs)
    }
}

#[test]
fn e2e_deadline() {
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
    let input1: PortId = "INPUT-1".into();
    let (tx_input1, rx_input1): (flume::Sender<Arc<Message>>, flume::Receiver<Arc<Message>>) =
        flume::unbounded::<Arc<Message>>();
    let receiver_input1: LinkReceiver = LinkReceiver {
        id: input1.clone(),
        receiver: rx_input1,
    };
    let sender_input1: LinkSender = LinkSender {
        id: input1.clone(),
        sender: tx_input1,
    };

    let input2: PortId = "INPUT-2".into();
    let (_tx_input2, rx_input2): (flume::Sender<Arc<Message>>, flume::Receiver<Arc<Message>>) =
        flume::unbounded::<Arc<Message>>();
    let receiver_input2: LinkReceiver = LinkReceiver {
        id: input2.clone(),
        receiver: rx_input2,
    };

    let mut io_inputs: HashMap<PortId, LinkReceiver> = HashMap::with_capacity(2);
    io_inputs.insert(input1.clone(), receiver_input1);
    io_inputs.insert(input2.clone(), receiver_input2);
    let mut inputs: HashMap<PortId, PortType> = HashMap::with_capacity(2);
    inputs.insert(input1.clone(), "usize".into());
    inputs.insert(input2.clone(), "usize".into());

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

    let operator = TestOperatorDeadlineViolated {
        input1: input1.clone(),
        output: output.clone(),
    };
    let operator_id: NodeId = "TestOperatorDeadlineViolated".into();
    let operator_deadline = E2EDeadlineRecord {
        from: OutputDescriptor {
            node: operator_id.clone(),
            output: output.clone(),
        },
        to: InputDescriptor {
            node: "future-not-violated".into(),
            input: input1.clone(),
        },
        duration: Duration::from_secs(5),
    };

    let operator_runner = OperatorRunner {
        id: operator_id.clone(),
        context: instance_context.clone(),
        io: Arc::new(Mutex::new(operator_io)),
        inputs,
        outputs,
        local_deadline: None,
        is_running: Arc::new(Mutex::new(false)),
        state: Arc::new(Mutex::new(operator.initialize(&None).unwrap())),
        operator: Arc::new(operator),
        _library: None,
        end_to_end_deadlines: vec![operator_deadline.clone()],
        ciclo: None,
    };

    let runner = NodeRunner::new(Arc::new(operator_runner), instance_context);

    assert!(
        // We use a timeout as, if an `assert` inside the Operator fails, the `task::block_on` will
        // enter a deadlock.
        async_std::task::block_on(async_std::future::timeout(Duration::from_secs(5), async {
            let runner_manager = runner.start();

            let start = hlc.new_timestamp();

            // This deadline will be violated as we are going to sleep for 500ms. However, the Operator
            // is not supposed to check it as it’s not the "to" node.
            let deadline_violated_to_propagate = E2EDeadline {
                duration: Duration::from_millis(100),
                from: OutputDescriptor {
                    node: "past".into(),
                    output: output.clone(),
                },
                to: InputDescriptor {
                    node: "future".into(),
                    input: input1.clone(),
                },
                start,
            };

            // We sleep for 500 ms.
            async_std::task::sleep(Duration::from_millis(500)).await;
            // Then we send two deadlines that ends at the Operator, started at `start` (so 500 ms
            // before) and the duration of the deadline is 100 ms.
            //
            // Only the deadline tied to input1 should result in a deadline miss because we sent it
            // on "input1" — although there are two deadlines that apply on the Operator.
            //
            // We check in the `run` of the Operator that we indeed have a deadline miss. So the
            // test is performed there.
            send_usize(
                &hlc,
                &sender_input1,
                1,
                vec![
                    E2EDeadline {
                        duration: Duration::from_millis(100),
                        from: OutputDescriptor {
                            node: "past".into(),
                            output: output.clone(),
                        },
                        to: InputDescriptor {
                            node: operator_id.clone(),
                            input: input1.clone(),
                        },
                        start,
                    },
                    E2EDeadline {
                        duration: Duration::from_millis(100),
                        from: OutputDescriptor {
                            node: "past".into(),
                            output: output.clone(),
                        },
                        to: InputDescriptor {
                            node: operator_id.clone(),
                            input: input2.clone(),
                        },
                        start,
                    },
                    deadline_violated_to_propagate.clone(),
                ],
            )
            .await;

            // Finally, we check that the deadline that does not concern the Operator (although
            // it’s violated) + the deadline that starts at the Operator are propagated.
            let (_, message) = receiver_output.recv().await.unwrap();
            if let Message::Data(data_message) = message.as_ref() {
                assert_eq!(data_message.end_to_end_deadlines.len(), 2);
                assert!(data_message
                    .end_to_end_deadlines
                    .contains(&deadline_violated_to_propagate));
                assert!(data_message
                    .end_to_end_deadlines
                    .iter()
                    .any(|e2e_deadline| *e2e_deadline == operator_deadline));
            }

            runner_manager.kill().await.unwrap();
            runner_manager.await.unwrap();
        }))
        .is_ok(),
        "Deadlock detected (maybe an `assert` inside the `input rule` | `run` | `output rule` failed?)"
    );
}

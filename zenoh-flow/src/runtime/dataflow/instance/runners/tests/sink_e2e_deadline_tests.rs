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
use crate::model::link::PortDescriptor;
use crate::model::{InputDescriptor, OutputDescriptor};
use crate::runtime::dataflow::instance::link::{LinkReceiver, LinkSender};
use crate::runtime::dataflow::instance::runners::sink::SinkRunner;
use crate::runtime::dataflow::instance::runners::NodeRunner;
use crate::runtime::dataflow::loader::{Loader, LoaderConfig};
use crate::runtime::deadline::E2EDeadline;
use crate::runtime::{InstanceContext, RuntimeContext};
use crate::{
    Configuration, Context, Data, DataMessage, Deserializable, DowncastAny, EmptyState, Message,
    Node, NodeId, PortId, Sink, State, ZFData, ZFError, ZFResult,
};
use async_std::sync::{Arc, Mutex};
use async_trait::async_trait;
use std::convert::TryInto;
use std::time::Duration;
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

// -------------------------------------------------------------------------------------------------
// Scenarios tested:
//
// 1) the Sink is at the "end" of an E2EDeadline, the deadlines are checked.
// -------------------------------------------------------------------------------------------------
struct TestSinkE2EDeadline {}

impl Node for TestSinkE2EDeadline {
    fn initialize(&self, _configuration: &Option<Configuration>) -> ZFResult<State> {
        Ok(State::from::<EmptyState>(EmptyState {}))
    }

    fn finalize(&self, _state: &mut State) -> ZFResult<()> {
        Ok(())
    }
}

#[async_trait]
impl Sink for TestSinkE2EDeadline {
    async fn run(
        &self,
        _context: &mut Context,
        _state: &mut State,
        input: crate::DataMessage,
    ) -> ZFResult<()> {
        assert_eq!(input.missed_end_to_end_deadlines.len(), 1);
        assert_eq!(
            input.missed_end_to_end_deadlines[0].from.node,
            "past-missed".into()
        );
        Ok(())
    }
}

#[test]
fn sink_e2e_deadline() {
    let session = zenoh::open(zenoh::config::Config::default())
        .wait()
        .unwrap();
    let hlc = Arc::new(uhlc::HLC::default());
    let uuid = uuid::Uuid::new_v4();
    let runtime_context = RuntimeContext {
        session: Arc::new(session),
        hlc: hlc.clone(),
        loader: Arc::new(Loader::new(LoaderConfig::new())),
        runtime_name: "runtime--SINK-e2e-deadline-tests".into(),
        runtime_uuid: uuid,
    };
    let instance_context = InstanceContext {
        flow_id: "flow--SINK-e2e-deadline-tests".into(),
        instance_id: uuid::Uuid::new_v4(),
        runtime: runtime_context,
    };

    let input: PortId = "INPUT".into();
    let (tx_input, rx_input) = flume::unbounded::<Arc<Message>>();
    let receiver_input: LinkReceiver = LinkReceiver {
        id: input.clone(),
        receiver: rx_input,
    };
    let sender_input: LinkSender = LinkSender {
        id: input.clone(),
        sender: tx_input,
    };

    let sink_id: NodeId = "source".into();
    let sink = TestSinkE2EDeadline {};

    let e2e_deadline_miss = E2EDeadlineRecord {
        from: OutputDescriptor {
            node: "past-missed".into(),
            output: input.clone(),
        },
        to: InputDescriptor {
            node: sink_id.clone(),
            input: input.clone(),
        },
        duration: Duration::from_millis(100),
    };

    let e2e_deadline_ok = E2EDeadlineRecord {
        from: OutputDescriptor {
            node: "past-ok".into(),
            output: input.clone(),
        },
        to: InputDescriptor {
            node: sink_id.clone(),
            input: input.clone(),
        },
        duration: Duration::from_secs(1),
    };

    let sink_runner = SinkRunner {
        id: sink_id,
        context: instance_context.clone(),
        input: PortDescriptor {
            port_id: input,
            port_type: "ZFUsize".into(),
        },
        link: Arc::new(Mutex::new(Some(receiver_input))),
        is_running: Arc::new(Mutex::new(false)),
        state: Arc::new(Mutex::new(sink.initialize(&None).unwrap())),
        sink: Arc::new(sink),
        _library: None,
        _end_to_end_deadlines: vec![e2e_deadline_miss.clone(), e2e_deadline_ok.clone()],
    };

    let runner = NodeRunner::new(Arc::new(sink_runner), instance_context);
    assert!(
        async_std::task::block_on(async_std::future::timeout(Duration::from_secs(5), async {
            let runner_manager = runner.start();

            let start = hlc.new_timestamp();

            // Sleep half a second to invalidate `e2e_deadline_miss`.
            async_std::task::sleep(Duration::from_millis(500)).await;

            let e2e_deadlines = vec![
                E2EDeadline::new(e2e_deadline_miss, start),
                E2EDeadline::new(e2e_deadline_ok, start),
            ];

            let data_message =
                DataMessage::new(Data::from::<ZFUsize>(ZFUsize(1)), start, e2e_deadlines);
            let message = Arc::new(Message::Data(data_message));
            sender_input.send(message).await.unwrap();

            // Sleep for a second to give enough time to `sink.run` to perform the checks.
            async_std::task::sleep(Duration::from_secs(1)).await;

            runner_manager.kill().await.unwrap();
            runner_manager.await.unwrap();
        }))
        .is_ok(),
        "Deadlock detected."
    );
}

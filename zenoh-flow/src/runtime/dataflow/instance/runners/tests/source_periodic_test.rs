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

use more_asserts::assert_le;

use crate::model::link::PortDescriptor;
use crate::runtime::dataflow::instance::link::{LinkReceiver, LinkSender};
use crate::runtime::dataflow::instance::runners::source::SourceRunner;
use crate::runtime::dataflow::instance::runners::NodeRunner;
use crate::runtime::dataflow::loader::{Loader, LoaderConfig};
use crate::runtime::{InstanceContext, RuntimeContext};
use crate::{
    Configuration, Context, Data, Deserializable, DowncastAny, EmptyState, Message, Node, NodeId,
    PortId, Source, State, ZFData, ZFResult,
};
use async_std::sync::{Arc, Mutex};
use async_trait::async_trait;
use std::time::{Duration, Instant};
use zenoh::prelude::*;

// Using 10ms as default delta, this has proven to be effective on my MacBook.
static DEFAULT_DELTA_MS: u64 = 10;

// Using 1s as default period.
static DEFAULT_PERIOD_S: u64 = 1;

// ZFUsize implements Data.
#[derive(Debug, Clone)]
pub struct ZFTick(());

impl DowncastAny for ZFTick {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl ZFData for ZFTick {
    fn try_serialize(&self) -> ZFResult<Vec<u8>> {
        Ok(vec![])
    }
}

impl Deserializable for ZFTick {
    fn try_deserialize(_bytes: &[u8]) -> ZFResult<Self>
    where
        Self: Sized,
    {
        Ok(ZFTick(()))
    }
}

// -------------------------------------------------------------------------------------------------
// Scenarios tested:
//
// 1) the source produces data at the given period
// -------------------------------------------------------------------------------------------------
struct TestSourcePeriodic {}

impl Node for TestSourcePeriodic {
    fn initialize(&self, _configuration: &Option<Configuration>) -> ZFResult<State> {
        Ok(State::from::<EmptyState>(EmptyState {}))
    }

    fn finalize(&self, _state: &mut State) -> ZFResult<()> {
        Ok(())
    }
}

#[async_trait]
impl Source for TestSourcePeriodic {
    async fn run(&self, _context: &mut Context, _state: &mut State) -> ZFResult<Data> {
        // The sources produces a tick.
        Ok(Data::from::<ZFTick>(ZFTick(())))
    }
}

#[test]
fn source_periodic() {
    let session = zenoh::open(zenoh::config::Config::default())
        .wait()
        .unwrap();
    let hlc = Arc::new(uhlc::HLC::default());
    let uuid = uuid::Uuid::new_v4();
    let runtime_context = RuntimeContext {
        session: Arc::new(session),
        hlc,
        loader: Arc::new(Loader::new(LoaderConfig::new())),
        runtime_name: "runtime--source-periodic-tests".into(),
        runtime_uuid: uuid,
    };
    let instance_context = InstanceContext {
        flow_id: "flow--source-periodic-tests".into(),
        instance_id: uuid::Uuid::new_v4(),
        runtime: runtime_context,
    };

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

    let source_id: NodeId = "source".into();
    let source = TestSourcePeriodic {};

    let source_runner = SourceRunner {
        id: source_id,
        context: instance_context.clone(),
        period: Some(Duration::from_secs(DEFAULT_PERIOD_S)),
        output: PortDescriptor {
            port_id: output,
            port_type: "ZFTick".into(),
        },
        links: Arc::new(Mutex::new(vec![sender_output])),
        is_running: Arc::new(Mutex::new(false)),
        state: Arc::new(Mutex::new(source.initialize(&None).unwrap())),
        base_resource_name: "test".into(),
        current_recording_resource: Arc::new(Mutex::new(None)),
        is_recording: Arc::new(Mutex::new(false)),
        source: Arc::new(source),
        _library: None,
        current_recording_resource_id: Arc::new(Mutex::new(None)),
    };

    let runner = NodeRunner::new(Arc::new(source_runner), instance_context);
    assert!(
        async_std::task::block_on(async_std::future::timeout(Duration::from_secs(8), async {
            let now = Instant::now();
            let runner_manager = runner.start();
            let mut i = 0u64;
            // Runs 5 times, ~ 5 seconds given the period of the source
            while i <= 5 {
                let (_, _) = receiver_output.recv().await.unwrap();

                let dur = now.elapsed();

                let expected_lower_bound =
                    Duration::from_secs(i).saturating_sub(Duration::from_millis(DEFAULT_DELTA_MS));
                let expected_upper_bound =
                    Duration::from_secs(i).saturating_add(Duration::from_millis(DEFAULT_DELTA_MS));
                let flag = (dur >= expected_lower_bound) && (dur <= expected_upper_bound);

                // Checks if the time elapsed before receiving the message is
                // nT - d < t < nT + d

                assert_le!(dur, expected_upper_bound);
                assert_le!(expected_lower_bound, dur);
                assert!(flag);
                i += 1;
            }

            runner_manager.kill().await.unwrap();
            runner_manager.await.unwrap();
        }))
        .is_ok(),
        "Deadlock detected."
    );
}

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


use async_std::sync::Arc;
use async_trait::async_trait;
use flume::{bounded, Receiver};
use std::convert::TryInto;
use std::sync::atomic::{AtomicUsize, Ordering};
use zenoh_flow::error::ZFError;
use zenoh_flow::model::link::PortDescriptor;
use zenoh_flow::model::{InputDescriptor, OutputDescriptor};
use zenoh_flow::runtime::dataflow::instance::io::{Inputs, Outputs, Streams};
use zenoh_flow::runtime::dataflow::instance::DataflowInstance;
use zenoh_flow::runtime::dataflow::loader::{Loader, LoaderConfig};
use zenoh_flow::runtime::message::Message;
use zenoh_flow::runtime::RuntimeContext;
use zenoh_flow::traits::{AsyncIteration, Deserializable, Operator, Sink, Source, ZFData};
use zenoh_flow::types::{Configuration, Context, Data, ZFResult};
use zenoh_flow::zenoh_flow_derive::ZFData;

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
    async fn setup(
        &self,
        _ctx: &mut Context,
        _configuration: &Option<Configuration>,
        mut outputs: Outputs,
    ) -> ZFResult<Option<Arc<dyn AsyncIteration>>> {
        let output = outputs.take(SOURCE).unwrap();
        let c_trigger_rx = self.rx.clone();

        Ok(Some(Arc::new(move || async move {
            c_trigger_rx.recv_async().await.unwrap();
            COUNTER.fetch_add(1, Ordering::AcqRel);
            let d = ZFUsize(COUNTER.load(Ordering::Relaxed));
            let data = Data::from(d);
            output.send_async(data, None).await
        })))
    }
}

// SINK

struct ExampleGenericSink;

#[async_trait]
impl Sink for ExampleGenericSink {
    async fn setup(
        &self,
        _ctx: &mut Context,
        _configuration: &Option<Configuration>,
        mut inputs: Inputs,
    ) -> ZFResult<Option<Arc<dyn AsyncIteration>>> {
        let input = inputs.take(SOURCE).unwrap();

        Ok(Some(Arc::new(move || async move {
            if let Ok(Message::Data(mut msg)) = input.recv_async().await {
                let data = msg.get_inner_data().try_get::<ZFUsize>()?;
                assert_eq!(data.0, COUNTER.load(Ordering::Relaxed));
                println!("Example Generic Sink Received: {:?}", input);
            }
            Ok(())
        })))
    }
}

// OPERATOR

#[derive(Debug)]
struct NoOp;

#[async_trait]
impl Operator for NoOp {
    async fn setup(
        &self,
        _ctx: &mut Context,
        _configuration: &Option<Configuration>,
        mut inputs: Inputs,
        mut outputs: Outputs,
    ) -> ZFResult<Option<Arc<dyn AsyncIteration>>> {
        let input = inputs.take(SOURCE).unwrap();
        let output = outputs.take(DESTINATION).unwrap();

        Ok(Some(Arc::new(move || async move {
            if let Ok(Message::Data(mut msg)) = input.recv_async().await {
                let data = msg.get_inner_data().try_get::<ZFUsize>()?;
                assert_eq!(data.0, COUNTER.load(Ordering::Relaxed));
                let out_data = Data::from(data.clone());
                output.send_async(out_data, None).await?;
            }
            Ok(())
        })))
    }
}

// Run dataflow in single runtime
async fn single_runtime() {
    env_logger::init();

    let (tx, rx) = bounded::<()>(1); // Channel used to trigger source

    let session = Arc::new(zenoh::open(zenoh::config::Config::default()).await.unwrap());
    let hlc = async_std::sync::Arc::new(uhlc::HLC::default());
    let rt_uuid = uuid::Uuid::new_v4();
    let ctx = RuntimeContext {
        session,
        hlc: hlc.clone(),
        loader: Arc::new(Loader::new(LoaderConfig::new())),
        runtime_name: format!("test-runtime-{}", rt_uuid).into(),
        runtime_uuid: rt_uuid,
    };

    let mut dataflow =
        zenoh_flow::runtime::dataflow::Dataflow::new(ctx.clone(), "test".into(), None);

    let source = Arc::new(CountSource::new(rx));
    let sink = Arc::new(ExampleGenericSink {});
    let operator = Arc::new(NoOp {});

    dataflow
        .try_add_static_source(
            "counter-source".into(),
            None,
            PortDescriptor {
                port_id: SOURCE.into(),
                port_type: "int".into(),
            },
            source,
        )
        .unwrap();

    dataflow
        .try_add_static_sink(
            "generic-sink".into(),
            None,
            PortDescriptor {
                port_id: SOURCE.into(),
                port_type: "int".into(),
            },
            sink,
        )
        .unwrap();

    dataflow
        .try_add_static_operator(
            "noop".into(),
            None,
            vec![PortDescriptor {
                port_id: SOURCE.into(),
                port_type: "int".into(),
            }],
            vec![PortDescriptor {
                port_id: DESTINATION.into(),
                port_type: "int".into(),
            }],
            operator,
        )
        .unwrap();

    dataflow
        .try_add_link(
            OutputDescriptor {
                node: "counter-source".into(),
                output: SOURCE.into(),
            },
            InputDescriptor {
                node: "noop".into(),
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
                node: "noop".into(),
                output: DESTINATION.into(),
            },
            InputDescriptor {
                node: "generic-sink".into(),
                input: SOURCE.into(),
            },
            None,
            None,
            None,
        )
        .unwrap();

    let mut instance = DataflowInstance::try_instantiate(dataflow, hlc.clone()).unwrap();

    let ids = instance.get_nodes();
    for id in &ids {
        instance.start_node(id).await.unwrap();
    }
    tx.send_async(()).await.unwrap();

    zenoh_flow::async_std::task::sleep(std::time::Duration::from_secs(1)).await;

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
fn run_single_runtime() {
    let h1 = async_std::task::spawn(async move { single_runtime().await });

    async_std::task::block_on(async move { h1.await })
}

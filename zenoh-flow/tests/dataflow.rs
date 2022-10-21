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

use async_trait::async_trait;
use flume::{bounded, Receiver};
use std::convert::TryInto;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use zenoh::prelude::r#async::*;
use zenoh_flow::model::descriptor::{InputDescriptor, OutputDescriptor};
use zenoh_flow::model::record::{OperatorRecord, PortRecord, SinkRecord, SourceRecord};
use zenoh_flow::prelude::*;
use zenoh_flow::runtime::dataflow::instance::DataFlowInstance;
use zenoh_flow::runtime::dataflow::loader::{Loader, LoaderConfig};
use zenoh_flow::runtime::RuntimeContext;
use zenoh_flow::traits::{
    Deserializable, Node, OperatorFactoryTrait, SinkFactoryTrait, SourceFactoryTrait, ZFData,
};
use zenoh_flow::types::{Configuration, Context, Data, Inputs, Message, Outputs, Streams};
use zenoh_flow::zenoh_flow_derive::ZFData;
// Data Type

#[derive(Debug, Clone, ZFData)]
pub struct ZFUsize(pub usize);

impl ZFData for ZFUsize {
    fn try_serialize(&self) -> Result<Vec<u8>> {
        Ok(self.0.to_ne_bytes().to_vec())
    }
}

impl Deserializable for ZFUsize {
    fn try_deserialize(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        let value = usize::from_ne_bytes(
            bytes
                .try_into()
                .map_err(|e| zferror!(ErrorKind::DeseralizationError, "{}", e))?,
        );
        Ok(ZFUsize(value))
    }
}

static SOURCE: &str = "Counter";
static DESTINATION: &str = "Counter";
static PORT_CALLBACK: &str = "Counter_callback";

static COUNTER: AtomicUsize = AtomicUsize::new(0);
static COUNTER_CALLBACK: AtomicUsize = AtomicUsize::new(1);

// SOURCE

struct CountSource {
    rx: Receiver<()>,
    output: Output,
    output_callback: Output,
}

#[async_trait]
impl Node for CountSource {
    async fn iteration(&self) -> Result<()> {
        self.rx.recv_async().await.unwrap();
        COUNTER.fetch_add(1, Ordering::AcqRel);
        self.output
            .send_async(Data::from(ZFUsize(COUNTER.load(Ordering::Relaxed))), None)
            .await?;

        COUNTER_CALLBACK.fetch_add(1, Ordering::AcqRel);
        self.output_callback
            .send_async(
                Data::from(ZFUsize(COUNTER_CALLBACK.load(Ordering::Relaxed))),
                None,
            )
            .await
    }
}

struct CountSourceFactory {
    rx: Receiver<()>,
}

#[async_trait]
impl SourceFactoryTrait for CountSourceFactory {
    async fn new_source(
        &self,
        _context: &mut Context,
        _configuration: &Option<Configuration>,
        mut outputs: Outputs,
    ) -> Result<Option<Arc<dyn Node>>> {
        let output = outputs.take(SOURCE).unwrap();
        let output_callback = outputs.take(PORT_CALLBACK).unwrap();

        Ok(Some(Arc::new(CountSource {
            rx: self.rx.clone(),
            output,
            output_callback,
        })))
    }
}

// SINK

struct GenericSink {
    input: Input,
    input_callback: Input,
}

#[async_trait]
impl Node for GenericSink {
    async fn iteration(&self) -> Result<()> {
        if let Ok(Message::Data(mut msg)) = self.input.recv_async().await {
            let data = msg.get_inner_data().try_get::<ZFUsize>()?;
            assert_eq!(data.0, COUNTER.load(Ordering::Relaxed));
        }

        if let Ok(Message::Data(mut msg)) = self.input_callback.recv_async().await {
            let data = msg.get_inner_data().try_get::<ZFUsize>()?;
            assert_eq!(data.0, COUNTER_CALLBACK.load(Ordering::Relaxed));
        }

        Ok(())
    }
}

struct GenericSinkFactory;

#[async_trait]
impl SinkFactoryTrait for GenericSinkFactory {
    async fn new_sink(
        &self,
        _context: &mut Context,
        _configuration: &Option<Configuration>,
        mut inputs: Inputs,
    ) -> Result<Option<Arc<dyn Node>>> {
        let input = inputs.take(SOURCE).unwrap();
        let input_callback = inputs.take(PORT_CALLBACK).unwrap();

        Ok(Some(Arc::new(GenericSink {
            input,
            input_callback,
        })))
    }
}

// OPERATORS
struct NoOp {
    input: Input,
    output: Output,
}

#[async_trait]
impl Node for NoOp {
    async fn iteration(&self) -> Result<()> {
        if let Ok(Message::Data(mut msg)) = self.input.recv_async().await {
            let data = msg.get_inner_data().try_get::<ZFUsize>()?;
            assert_eq!(data.0, COUNTER.load(Ordering::Relaxed));
            let out_data = Data::from(data.clone());
            self.output.send_async(out_data, None).await?;
        }
        Ok(())
    }
}

struct NoOpFactory;

#[async_trait]
impl OperatorFactoryTrait for NoOpFactory {
    async fn new_operator(
        &self,
        _context: &mut Context,
        _configuration: &Option<Configuration>,
        mut inputs: Inputs,
        mut outputs: Outputs,
    ) -> Result<Option<Arc<dyn Node>>> {
        Ok(Some(Arc::new(NoOp {
            input: inputs.take(SOURCE).unwrap(),
            output: outputs.take(DESTINATION).unwrap(),
        })))
    }
}

struct NoOpCallbackFactory;

#[async_trait]
impl OperatorFactoryTrait for NoOpCallbackFactory {
    async fn new_operator(
        &self,
        context: &mut Context,
        _configuration: &Option<Configuration>,
        mut inputs: Inputs,
        mut outputs: Outputs,
    ) -> Result<Option<Arc<dyn Node>>> {
        let input = inputs.take(PORT_CALLBACK).unwrap();
        let output = outputs.take_into_arc(PORT_CALLBACK).unwrap();

        context.register_input_callback(
            input,
            Arc::new(move |message| {
                let output_cloned = output.clone();

                async move {
                    println!("Entering callback");
                    let data = match message {
                        Message::Data(mut data) => {
                            data.get_inner_data().try_get::<ZFUsize>()?.clone()
                        }
                        _ => return Err(zferror!(ErrorKind::Unsupported).into()),
                    };

                    assert_eq!(data.0, COUNTER_CALLBACK.load(Ordering::Relaxed));
                    output_cloned.send_async(Data::from(data), None).await?;
                    Ok(())
                }
            }),
        );

        Ok(None)
    }
}

// Run dataflow in single runtime
async fn single_runtime() {
    env_logger::init();

    let (tx, rx) = bounded::<()>(1); // Channel used to trigger source

    let session = Arc::new(
        zenoh::open(zenoh::config::Config::default())
            .res()
            .await
            .unwrap(),
    );
    let hlc = async_std::sync::Arc::new(uhlc::HLC::default());
    let rt_uuid = uuid::Uuid::new_v4();
    let runtime_name: RuntimeId = format!("test-runtime-{}", rt_uuid).into();
    let ctx = RuntimeContext {
        session,
        hlc: hlc.clone(),
        loader: Arc::new(Loader::new(LoaderConfig::new())),
        runtime_name: runtime_name.clone(),
        runtime_uuid: rt_uuid,
    };

    let mut dataflow = zenoh_flow::runtime::dataflow::DataFlow::new("test", ctx.clone());

    let source_record = SourceRecord {
        id: "counter-source".into(),
        uid: 0,
        outputs: vec![
            PortRecord {
                uid: 0,
                port_id: SOURCE.into(),
                port_type: "int".into(),
            },
            PortRecord {
                uid: 1,
                port_id: PORT_CALLBACK.into(),
                port_type: "int".into(),
            },
        ],
        uri: None,
        configuration: None,
        runtime: runtime_name.clone(),
    };

    dataflow.add_source_factory(source_record, Arc::new(CountSourceFactory { rx }));

    let sink_record = SinkRecord {
        id: "generic-sink".into(),
        uid: 1,
        inputs: vec![
            PortRecord {
                uid: 2,
                port_id: SOURCE.into(),
                port_type: "int".into(),
            },
            PortRecord {
                uid: 3,
                port_id: PORT_CALLBACK.into(),
                port_type: "int".into(),
            },
        ],
        uri: None,
        configuration: None,
        runtime: runtime_name.clone(),
    };

    dataflow.add_sink_factory(sink_record, Arc::new(GenericSinkFactory));

    let no_op_record = OperatorRecord {
        id: "noop".into(),
        uid: 2,
        inputs: vec![PortRecord {
            uid: 4,
            port_id: SOURCE.into(),
            port_type: "int".into(),
        }],
        outputs: vec![PortRecord {
            uid: 5,
            port_id: DESTINATION.into(),
            port_type: "int".into(),
        }],
        uri: None,
        configuration: None,
        runtime: runtime_name.clone(),
    };

    dataflow.add_operator_factory(no_op_record, Arc::new(NoOpFactory));

    let no_op_callback_record = OperatorRecord {
        id: "noop_callback".into(),
        uid: 3,
        inputs: vec![PortRecord {
            uid: 6,
            port_id: PORT_CALLBACK.into(),
            port_type: "int".into(),
        }],
        outputs: vec![PortRecord {
            uid: 7,
            port_id: PORT_CALLBACK.into(),
            port_type: "int".into(),
        }],
        uri: None,
        configuration: None,
        runtime: runtime_name.clone(),
    };

    dataflow.add_operator_factory(no_op_callback_record, Arc::new(NoOpCallbackFactory));

    dataflow.add_link(
        OutputDescriptor {
            node: "counter-source".into(),
            output: SOURCE.into(),
        },
        InputDescriptor {
            node: "noop".into(),
            input: SOURCE.into(),
        },
    );

    dataflow.add_link(
        OutputDescriptor {
            node: "noop".into(),
            output: DESTINATION.into(),
        },
        InputDescriptor {
            node: "generic-sink".into(),
            input: SOURCE.into(),
        },
    );

    dataflow.add_link(
        OutputDescriptor {
            node: "counter-source".into(),
            output: PORT_CALLBACK.into(),
        },
        InputDescriptor {
            node: "noop_callback".into(),
            input: PORT_CALLBACK.into(),
        },
    );

    dataflow.add_link(
        OutputDescriptor {
            node: "noop_callback".into(),
            output: PORT_CALLBACK.into(),
        },
        InputDescriptor {
            node: "generic-sink".into(),
            input: PORT_CALLBACK.into(),
        },
    );

    let mut instance = DataFlowInstance::try_instantiate(dataflow, hlc.clone())
        .await
        .unwrap();

    for id in instance.get_sinks() {
        instance.start_node(&id).unwrap();
    }

    for id in instance.get_operators() {
        instance.start_node(&id).unwrap();
    }

    for id in instance.get_sources() {
        instance.start_node(&id).unwrap();
    }

    tx.send_async(()).await.unwrap();

    async_std::task::sleep(std::time::Duration::from_secs(1)).await;

    for id in instance.get_sources() {
        instance.stop_node(&id).await.unwrap();
    }

    for id in instance.get_operators() {
        instance.stop_node(&id).await.unwrap();
    }

    for id in instance.get_sinks() {
        instance.stop_node(&id).await.unwrap();
    }
}

#[test]
fn run_single_runtime() {
    let h1 = async_std::task::spawn(async move { single_runtime().await });

    async_std::task::block_on(async move { h1.await })
}

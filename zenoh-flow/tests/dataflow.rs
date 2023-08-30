//
// Copyright (c) 2021 - 2023 ZettaScale Technology
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
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use zenoh::prelude::r#async::*;

use zenoh_flow::io::{Inputs, Outputs};
use zenoh_flow::model::descriptor::{InputDescriptor, OutputDescriptor};
use zenoh_flow::model::record::{OperatorRecord, PortRecord, SinkRecord, SourceRecord};
use zenoh_flow::runtime::dataflow::instance::DataFlowInstance;
use zenoh_flow::runtime::dataflow::loader::{Loader, LoaderConfig};
use zenoh_flow::runtime::RuntimeContext;
use zenoh_flow::types::{Configuration, Context, LinkMessage, Message, Payload};
use zenoh_flow::{
    prelude::*, DEFAULT_SHM_ALLOCATION_BACKOFF_NS, DEFAULT_SHM_ELEMENT_SIZE,
    DEFAULT_SHM_TOTAL_ELEMENTS,
};

static SOURCE: &str = "test-source";
static OPERATOR: &str = "test-operator";
static SINK: &str = "test-sink";

static IN_TYPED: &str = "in-typed";
static IN_RAW: &str = "in-raw";
static OUT_TYPED: &str = "out-typed";
static OUT_RAW: &str = "out-raw";

static RAW_VALUE: u64 = 10;
static TYPED_VALUE: u64 = 1;

// Use the `serde_json` crate to serialize and deserialize some u64 integers.
fn serialize_serde_json(buffer: &mut Vec<u8>, data: &u64, origin: &str) -> anyhow::Result<()> {
    println!("Serializer called in: {origin}!");
    serde_json::ser::to_writer(buffer, data).map_err(|e| anyhow::anyhow!(e))
}

fn deserialize_serde_json(bytes: &[u8], origin: &str) -> anyhow::Result<u64> {
    println!("Deserializer called in: {origin}!");
    serde_json::de::from_slice::<u64>(bytes).map_err(|e| anyhow::anyhow!(e))
}

// -------------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------------
// SOURCE
// -------------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------------

struct TestSource {
    output: Output<u64>,
    output_raw: OutputRaw,
}

#[async_trait]
impl Source for TestSource {
    async fn new(
        _context: Context,
        _configuration: Option<Configuration>,
        mut outputs: Outputs,
    ) -> Result<Self> {
        println!("[TestSource] constructor");
        let output = outputs
            .take(OUT_TYPED)
            .expect("No `OUT_TYPED` for TestSource")
            .typed(|buffer, data| serialize_serde_json(buffer, data, "TestSource"));
        let output_raw = outputs
            .take(OUT_RAW)
            .expect("No `OUT_RAW` for TestSource")
            .raw();

        Ok(TestSource { output, output_raw })
    }
}

#[async_trait]
impl Node for TestSource {
    async fn iteration(&self) -> Result<()> {
        println!("[TestSource] Starting iteration");
        self.output.send(TYPED_VALUE, None).await?;

        let mut buffer = Vec::new();
        serialize_serde_json(&mut buffer, &RAW_VALUE, "manual")
            .expect("Failed to serialize 10u64 using `serde_json`");
        self.output_raw.send(buffer, None).await?;

        println!("[TestSource] iteration done, sleeping");
        async_std::task::sleep(Duration::from_secs(10)).await;

        Ok(())
    }
}

// -------------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------------
// OPERATOR
// -------------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------------

struct TestOperator {
    input_typed: Input<u64>,
    input_raw: InputRaw,
    output_typed: Output<u64>,
    output_raw: OutputRaw,
}

#[async_trait]
impl Operator for TestOperator {
    async fn new(
        _context: Context,
        _configuration: Option<Configuration>,
        mut inputs: Inputs,
        mut outputs: Outputs,
    ) -> Result<Self> {
        println!("[TestOperator] constructor");
        Ok(TestOperator {
            input_typed: inputs
                .take(IN_TYPED)
                .expect("No input `IN_TYPED` for TestOperator")
                .typed(|bytes| deserialize_serde_json(bytes, "TestOperator")),
            input_raw: inputs
                .take(IN_RAW)
                .expect("No input `IN_RAW` for TestOperator")
                .raw(),
            output_typed: outputs
                .take(OUT_TYPED)
                .expect("No output `OUT_TYPED` for TestOperator")
                .typed(|buffer, data| serialize_serde_json(buffer, data, "TestOperator")),
            output_raw: outputs
                .take(OUT_RAW)
                .expect("No output `OUT_RAW` for TestOperator")
                .raw(),
        })
    }
}

#[async_trait]
impl Node for TestOperator {
    async fn iteration(&self) -> Result<()> {
        println!("[TestOperator] iteration being");

        let link_message = self.input_raw.recv().await?;
        let (typed_message, _timestamp) = self.input_typed.recv().await?;

        if let (LinkMessage::Data(ref data_message), Message::Data(data)) =
            (link_message, typed_message)
        {
            // Check the raw input value.
            //
            // NOTE: in the TestSource iteration we sent the data serialized. Hence we are expecting
            // to receive it as `Payload::Bytes`.
            match data_message.deref() {
                Payload::Bytes(bytes) => {
                    let value = deserialize_serde_json(bytes.as_slice(), "manual")
                        .expect("Failed to deserialize bytes with serde_json");
                    assert_eq!(value, RAW_VALUE);
                }
                Payload::Typed((_dyn_data, _)) => {
                    panic!("Unexpected typed message")
                }
            }

            // Check the typed input value.
            assert_eq!(TYPED_VALUE, *data);

            self.output_raw.send(data_message.clone(), None).await?;
            self.output_typed.send(data, None).await?;
        } else {
            panic!("Unexpected watermark message")
        }

        println!("[TestOperator] iteration done");
        Ok(())
    }
}

// -------------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------------
// SINK
// -------------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------------

struct TestSink {
    input_raw: InputRaw,
    input_typed: Input<u64>,
}

#[async_trait]
impl Sink for TestSink {
    async fn new(
        _context: Context,
        _configuration: Option<Configuration>,
        mut inputs: Inputs,
    ) -> Result<Self> {
        println!("[TestSink] constructor");
        let input_raw = inputs.take(IN_RAW).unwrap().raw();
        let input_typed = inputs
            .take(IN_TYPED)
            .expect("Missing input IN_TYPED for TestSink")
            .typed(|bytes| deserialize_serde_json(bytes, "TestSink"));

        Ok(TestSink {
            input_raw,
            input_typed,
        })
    }
}

#[async_trait]
impl Node for TestSink {
    async fn iteration(&self) -> Result<()> {
        println!("[TestSink] Starting iteration");

        let link_message = self.input_raw.recv().await?;
        let (typed_message, _timestamp) = self.input_typed.recv().await?;

        if let (LinkMessage::Data(ref data_message), Message::Data(data)) =
            (link_message, typed_message)
        {
            match data_message.deref() {
                Payload::Bytes(_) => {
                    panic!("Unexpected Payload::Bytes")
                }
                Payload::Typed((dyn_data, _)) => {
                    let value = (**dyn_data)
                        .as_any()
                        .downcast_ref::<u64>()
                        .expect("Failed to downcast");
                    // NOTE: Tricky bit, we connected the typed output of the `TestOperator` to the
                    // raw input of the `TestSink`. Hence, when checking the value we need to
                    // compare it with `TYPED_VALUE`.
                    assert_eq!(*value, TYPED_VALUE);
                }
            }

            // NOTE: Tricky bit, we connected the raw output of the `TestOperator` to the
            // typed input of the `TestSink`. Hence, when checking the value we need to
            // compare it with `RAW_VALUE`.
            //
            // We should also see a call to the deserializer function called "in: TestSink!" in the
            // logs (run the test with `-- --show-output`).
            assert_eq!(*data, RAW_VALUE);
        } else {
            panic!("Unexpected watermark message")
        }

        Ok(())
    }
}

// -------------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------------
// MANUAL INSTANTIATION OF THE DATAFLOW
// -------------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------------

async fn single_runtime() {
    let _ = env_logger::try_init();

    let session = Arc::new(
        zenoh::open(zenoh::config::Config::default())
            .res()
            .await
            .unwrap(),
    );
    let hlc = async_std::sync::Arc::new(uhlc::HLC::default());
    let rt_uuid = ZenohId::rand();
    let runtime_name: RuntimeId = format!("test-runtime-{rt_uuid}").into();
    let ctx = RuntimeContext {
        session,
        hlc: hlc.clone(),
        loader: Arc::new(Loader::new(LoaderConfig::new())),
        runtime_name: runtime_name.clone(),
        runtime_uuid: rt_uuid,
        shared_memory_element_size: DEFAULT_SHM_ELEMENT_SIZE as usize,
        shared_memory_elements: DEFAULT_SHM_TOTAL_ELEMENTS as usize,
        shared_memory_backoff: DEFAULT_SHM_ALLOCATION_BACKOFF_NS,
        use_shm: false,
    };

    let mut dataflow = zenoh_flow::runtime::dataflow::DataFlow::new("test", ctx.clone());

    let source_record = SourceRecord {
        id: SOURCE.into(),
        uid: 0,
        outputs: vec![
            PortRecord {
                uid: 0,
                port_id: OUT_TYPED.into(),
            },
            PortRecord {
                uid: 1,
                port_id: OUT_RAW.into(),
            },
        ],
        uri: None,
        configuration: None,
        runtime: runtime_name.clone(),
    };

    dataflow.add_source(
        source_record,
        |context: Context, configuration: Option<Configuration>, outputs: Outputs| {
            Box::pin(async {
                let node = TestSource::new(context, configuration, outputs).await?;
                Ok(Arc::new(node) as Arc<dyn Node>)
            })
        },
    );

    let operator_record = OperatorRecord {
        id: OPERATOR.into(),
        uid: 1,
        inputs: vec![
            PortRecord {
                uid: 1,
                port_id: IN_RAW.into(),
            },
            PortRecord {
                uid: 2,
                port_id: IN_TYPED.into(),
            },
        ],
        outputs: vec![
            PortRecord {
                uid: 3,
                port_id: OUT_RAW.into(),
            },
            PortRecord {
                uid: 4,
                port_id: OUT_TYPED.into(),
            },
        ],
        uri: None,
        configuration: None,
        runtime: runtime_name.clone(),
    };

    dataflow.add_operator(
        operator_record,
        |context: Context,
         configuration: Option<Configuration>,
         inputs: Inputs,
         outputs: Outputs| {
            Box::pin(async {
                let node = TestOperator::new(context, configuration, inputs, outputs).await?;
                Ok(Arc::new(node) as Arc<dyn Node>)
            })
        },
    );

    let sink_record = SinkRecord {
        id: SINK.into(),
        uid: 3,
        inputs: vec![
            PortRecord {
                uid: 9,
                port_id: IN_TYPED.into(),
            },
            PortRecord {
                uid: 10,
                port_id: IN_RAW.into(),
            },
        ],
        uri: None,
        configuration: None,
        runtime: runtime_name.clone(),
    };

    dataflow.add_sink(
        sink_record,
        |context: Context, configuration: Option<Configuration>, inputs: Inputs| {
            Box::pin(async {
                let node = TestSink::new(context, configuration, inputs).await?;
                Ok(Arc::new(node) as Arc<dyn Node>)
            })
        },
    );

    // SOURCE -> OPERATOR
    dataflow.add_link(
        OutputDescriptor {
            node: SOURCE.into(),
            output: OUT_RAW.into(),
        },
        InputDescriptor {
            node: OPERATOR.into(),
            input: IN_RAW.into(),
        },
    );

    dataflow.add_link(
        OutputDescriptor {
            node: SOURCE.into(),
            output: OUT_TYPED.into(),
        },
        InputDescriptor {
            node: OPERATOR.into(),
            input: IN_TYPED.into(),
        },
    );

    // OPERATOR -> SINK
    dataflow.add_link(
        // NOTE: We are connecting the raw output to the typed input.
        OutputDescriptor {
            node: OPERATOR.into(),
            output: OUT_RAW.into(),
        },
        InputDescriptor {
            node: SINK.into(),
            input: IN_TYPED.into(),
        },
    );

    dataflow.add_link(
        // NOTE: We are connecting the typed output to the raw input.
        OutputDescriptor {
            node: OPERATOR.into(),
            output: OUT_TYPED.into(),
        },
        InputDescriptor {
            node: SINK.into(),
            input: IN_RAW.into(),
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

    async_std::task::sleep(std::time::Duration::from_secs(2)).await;

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
    let _ = env_logger::try_init();

    let h1 = async_std::task::spawn(async move { single_runtime().await });

    async_std::task::block_on(h1)
}

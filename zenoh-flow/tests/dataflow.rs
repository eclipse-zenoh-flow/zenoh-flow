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
use std::sync::Arc;
use std::time::Duration;
use zenoh::prelude::r#async::*;
use zenoh_flow::io::{Inputs, Outputs};
use zenoh_flow::model::descriptor::{InputDescriptor, OutputDescriptor};
use zenoh_flow::model::record::{OperatorRecord, PortRecord, SinkRecord, SourceRecord};
use zenoh_flow::prelude::*;
use zenoh_flow::runtime::dataflow::instance::DataFlowInstance;
use zenoh_flow::runtime::dataflow::loader::{Loader, LoaderConfig};
use zenoh_flow::runtime::RuntimeContext;
use zenoh_flow::types::{Configuration, Context, LinkMessage, Message};

static SOURCE: &str = "counter-source";
static OP_RAW: &str = "operator-raw";
static OP_TYPED: &str = "operator-typed";
static SINK: &str = "generic-sink";

static IN_TYPED: &str = "in-typed";
static IN_RAW: &str = "in-raw";
static OUT_TYPED: &str = "out-typed";
static OUT_RAW: &str = "out-raw";

// SOURCE

struct CountSource {
    output: Output<usize>,
}

#[async_trait]
impl Source for CountSource {
    async fn new(
        _context: Context,
        _configuration: Option<Configuration>,
        mut outputs: Outputs,
    ) -> Result<Self> {
        println!("[CountSource] constructor");
        let output = outputs.take(OUT_TYPED).unwrap();

        Ok(CountSource { output })
    }
}

#[async_trait]
impl Node for CountSource {
    async fn iteration(&self) -> Result<()> {
        println!("[CountSource] Starting iteration");
        self.output.send(1usize, None).await?;

        println!("[CountSource] iteration done, sleeping");
        async_std::task::sleep(Duration::from_secs(10)).await;

        Ok(())
    }
}

// OPERATORS

/// An `OpRaw` uses, internally, only the data received in its `input_raw`.
///
/// The objective is to test the following "forward" of messages:
/// - InputRaw -> Output<T>
/// - InputRaw -> OutputRaw
struct OpRaw {
    input_typed: Input<usize>,
    input_raw: InputRaw,
    output_typed: Output<usize>,
    output_raw: OutputRaw,
}

#[async_trait]
impl Operator for OpRaw {
    async fn new(
        _context: Context,
        _configuration: Option<Configuration>,
        mut inputs: Inputs,
        mut outputs: Outputs,
    ) -> Result<Self> {
        println!("[OpRaw] constructor");
        Ok(OpRaw {
            input_typed: inputs.take(IN_TYPED).unwrap(),
            input_raw: inputs.take_raw(IN_RAW).unwrap(),
            output_typed: outputs.take(OUT_TYPED).unwrap(),
            output_raw: outputs.take_raw(OUT_RAW).unwrap(),
        })
    }
}

#[async_trait]
impl Node for OpRaw {
    async fn iteration(&self) -> Result<()> {
        println!("[OpRaw] iteration being");

        let link_message = self.input_raw.recv().await?;
        let (typed_message, _timestamp) = self.input_typed.recv().await?;

        if let (LinkMessage::Data(mut data_message), Message::Data(data)) =
            (link_message, typed_message)
        {
            assert_eq!(*data_message.try_get::<usize>()?, *data);

            self.output_raw.send(data_message.clone(), None).await?;
            self.output_typed.send(data_message, None).await?;
        } else {
            panic!("Unexpected watermark message")
        }

        println!("[OpRaw] iteration done");
        Ok(())
    }
}

/// An `OpTyped` uses, internally, only the data received in its `input_typed`.
///
/// The objective is to test the following "forward" of messages:
/// - Input<T> -> Output<T>
/// - Input<T> -> OutputRaw
struct OpTyped {
    input_typed: Input<usize>,
    input_raw: InputRaw,
    output_typed: Output<usize>,
    output_raw: OutputRaw,
}

#[async_trait]
impl Operator for OpTyped {
    async fn new(
        _context: Context,
        _configuration: Option<Configuration>,
        mut inputs: Inputs,
        mut outputs: Outputs,
    ) -> Result<Self> {
        println!("[OpRaw] constructor");
        Ok(OpTyped {
            input_typed: inputs.take(IN_TYPED).unwrap(),
            input_raw: inputs.take_raw(IN_RAW).unwrap(),
            output_typed: outputs.take(OUT_TYPED).unwrap(),
            output_raw: outputs.take_raw(OUT_RAW).unwrap(),
        })
    }
}

#[async_trait]
impl Node for OpTyped {
    async fn iteration(&self) -> Result<()> {
        println!("[OpTyped] iteration being");

        let link_message = self.input_raw.recv().await?;
        let (typed_message, _timestamp) = self.input_typed.recv().await?;

        if let (LinkMessage::Data(ref mut data_message), Message::Data(data)) =
            (link_message, typed_message)
        {
            let number = data_message.try_get::<usize>()?;
            assert_eq!(*number, *data);

            self.output_raw.send(*data, None).await?;
            self.output_typed.send(data, None).await?;
        } else {
            panic!("Unexpected watermark message")
        }

        println!("[OpTyped] iteration done");
        Ok(())
    }
}

// SINK

struct GenericSink {
    input_raw: InputRaw,
    input_typed: Input<usize>,
}

#[async_trait]
impl Sink for GenericSink {
    async fn new(
        _context: Context,
        _configuration: Option<Configuration>,
        mut inputs: Inputs,
    ) -> Result<Self> {
        println!("[GenericSink] constructor");
        let input_raw = inputs.take_raw(IN_RAW).unwrap();
        let input_typed = inputs.take(IN_TYPED).unwrap();

        Ok(GenericSink {
            input_raw,
            input_typed,
        })
    }
}

#[async_trait]
impl Node for GenericSink {
    async fn iteration(&self) -> Result<()> {
        println!("[GenericSink] Starting iteration");

        let link_message = self.input_raw.recv().await?;
        let (typed_message, _timestamp) = self.input_typed.recv().await?;

        if let (LinkMessage::Data(ref mut data_message), Message::Data(data)) =
            (link_message, typed_message)
        {
            println!("[GenericSink] Received on input_raw: {data_message:?}");
            println!("[GenericSink] Received on input_typed: {data:?}");
            let number = data_message.try_get::<usize>()?;
            assert_eq!(*number, *data);
        } else {
            panic!("Unexpected watermark message")
        }

        Ok(())
    }
}

// Run dataflow in single runtime
async fn single_runtime() {
    env_logger::init();

    let session = Arc::new(
        zenoh::open(zenoh::config::Config::default())
            .res()
            .await
            .unwrap(),
    );
    let hlc = async_std::sync::Arc::new(uhlc::HLC::default());
    let rt_uuid = uuid::Uuid::new_v4();
    let runtime_name: RuntimeId = format!("test-runtime-{rt_uuid}").into();
    let ctx = RuntimeContext {
        session,
        hlc: hlc.clone(),
        loader: Arc::new(Loader::new(LoaderConfig::new())),
        runtime_name: runtime_name.clone(),
        runtime_uuid: rt_uuid,
    };

    let mut dataflow = zenoh_flow::runtime::dataflow::DataFlow::new("test", ctx.clone());

    let source_record = SourceRecord {
        id: SOURCE.into(),
        uid: 0,
        outputs: vec![PortRecord {
            uid: 0,
            port_id: OUT_TYPED.into(),
            port_type: "int".into(),
        }],
        uri: None,
        configuration: None,
        runtime: runtime_name.clone(),
    };

    dataflow.add_source(
        source_record,
        |context: Context, configuration: Option<Configuration>, outputs: Outputs| {
            Box::pin(async {
                let node = CountSource::new(context, configuration, outputs).await?;
                Ok(Arc::new(node) as Arc<dyn Node>)
            })
        },
    );

    let op_raw_record = OperatorRecord {
        id: OP_RAW.into(),
        uid: 1,
        inputs: vec![
            PortRecord {
                uid: 1,
                port_id: IN_RAW.into(),
                port_type: "int".into(),
            },
            PortRecord {
                uid: 2,
                port_id: IN_TYPED.into(),
                port_type: "int".into(),
            },
        ],
        outputs: vec![
            PortRecord {
                uid: 3,
                port_id: OUT_RAW.into(),
                port_type: "int".into(),
            },
            PortRecord {
                uid: 4,
                port_id: OUT_TYPED.into(),
                port_type: "int".into(),
            },
        ],
        uri: None,
        configuration: None,
        runtime: runtime_name.clone(),
    };

    dataflow.add_operator(
        op_raw_record,
        |context: Context,
         configuration: Option<Configuration>,
         inputs: Inputs,
         outputs: Outputs| {
            Box::pin(async {
                let node = OpRaw::new(context, configuration, inputs, outputs).await?;
                Ok(Arc::new(node) as Arc<dyn Node>)
            })
        },
    );

    let op_typed_record = OperatorRecord {
        id: OP_TYPED.into(),
        uid: 2,
        inputs: vec![
            PortRecord {
                uid: 5,
                port_id: IN_RAW.into(),
                port_type: "int".into(),
            },
            PortRecord {
                uid: 6,
                port_id: IN_TYPED.into(),
                port_type: "int".into(),
            },
        ],
        outputs: vec![
            PortRecord {
                uid: 7,
                port_id: OUT_RAW.into(),
                port_type: "int".into(),
            },
            PortRecord {
                uid: 8,
                port_id: OUT_TYPED.into(),
                port_type: "int".into(),
            },
        ],
        uri: None,
        configuration: None,
        runtime: runtime_name.clone(),
    };

    dataflow.add_operator(
        op_typed_record,
        |context: Context,
         configuration: Option<Configuration>,
         inputs: Inputs,
         outputs: Outputs| {
            Box::pin(async {
                let node = OpTyped::new(context, configuration, inputs, outputs).await?;
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
                port_type: "int".into(),
            },
            PortRecord {
                uid: 10,
                port_id: IN_RAW.into(),
                port_type: "int".into(),
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
                let node = GenericSink::new(context, configuration, inputs).await?;
                Ok(Arc::new(node) as Arc<dyn Node>)
            })
        },
    );

    // SOURCE -> OP_RAW
    dataflow.add_link(
        OutputDescriptor {
            node: SOURCE.into(),
            output: OUT_TYPED.into(),
        },
        InputDescriptor {
            node: OP_RAW.into(),
            input: IN_RAW.into(),
        },
    );

    dataflow.add_link(
        OutputDescriptor {
            node: SOURCE.into(),
            output: OUT_TYPED.into(),
        },
        InputDescriptor {
            node: OP_RAW.into(),
            input: IN_TYPED.into(),
        },
    );

    // OP_RAW -> OP_TYPED
    dataflow.add_link(
        OutputDescriptor {
            node: OP_RAW.into(),
            output: OUT_RAW.into(), // CAVEAT: we cross, RAW -> TYPED
        },
        InputDescriptor {
            node: OP_TYPED.into(),
            input: IN_TYPED.into(),
        },
    );

    dataflow.add_link(
        OutputDescriptor {
            node: OP_RAW.into(),
            output: OUT_TYPED.into(), // CAVEAT: we cross, TYPED -> RAW
        },
        InputDescriptor {
            node: OP_TYPED.into(),
            input: IN_RAW.into(),
        },
    );

    // OP_TYPED -> SINK
    dataflow.add_link(
        OutputDescriptor {
            node: OP_TYPED.into(),
            output: OUT_RAW.into(), // CAVEAT: NO cross, RAW -> RAW
        },
        InputDescriptor {
            node: SINK.into(),
            input: IN_RAW.into(),
        },
    );

    dataflow.add_link(
        OutputDescriptor {
            node: OP_TYPED.into(),
            output: OUT_TYPED.into(), // CAVEAT: NO cross, TYPED -> TYPED
        },
        InputDescriptor {
            node: SINK.into(),
            input: IN_TYPED.into(),
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
    let h1 = async_std::task::spawn(async move { single_runtime().await });

    async_std::task::block_on(async move { h1.await })
}

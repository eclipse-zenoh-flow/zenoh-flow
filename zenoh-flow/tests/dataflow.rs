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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use zenoh::prelude::r#async::*;
use zenoh_flow::model::descriptor::{InputDescriptor, OutputDescriptor};
use zenoh_flow::model::record::{OperatorRecord, PortRecord, SinkRecord, SourceRecord};
use zenoh_flow::prelude::*;
use zenoh_flow::runtime::dataflow::instance::DataFlowInstance;
use zenoh_flow::runtime::dataflow::loader::{Loader, LoaderConfig};
use zenoh_flow::runtime::RuntimeContext;
use zenoh_flow::types::{Configuration, Context, Inputs, Message, Outputs, Streams};

static SOURCE: &str = "Counter";
static DESTINATION: &str = "Counter";

static COUNTER: AtomicUsize = AtomicUsize::new(0);

// SOURCE

struct CountSource {
    output: Output,
}

#[async_trait]
impl Source for CountSource {
    async fn new(
        _context: Context,
        _configuration: Option<Configuration>,
        mut outputs: Outputs,
    ) -> Result<Self> {
        println!("[CountSource] constructor");
        let output = outputs.take(SOURCE).unwrap();

        Ok(CountSource { output })
    }
}

#[async_trait]
impl Node for CountSource {
    async fn iteration(&self) -> Result<()> {
        println!("[CountSource] iteration being");

        COUNTER.fetch_add(1, Ordering::AcqRel);

        println!("[CountSource] sending on first output");
        self.output
            .send_async(COUNTER.load(Ordering::Relaxed), None)
            .await?;

        println!("[CountSource] iteration done, sleeping");
        async_std::task::sleep(Duration::from_secs(10)).await;

        Ok(())
    }
}

// SINK

struct GenericSink {
    input: Input,
}

#[async_trait]
impl Sink for GenericSink {
    async fn new(
        _context: Context,
        _configuration: Option<Configuration>,
        mut inputs: Inputs,
    ) -> Result<Self> {
        println!("[GenericSink] constructor");
        let input = inputs.take(SOURCE).unwrap();

        Ok(GenericSink { input })
    }
}

#[async_trait]
impl Node for GenericSink {
    async fn iteration(&self) -> Result<()> {
        println!("[GenericSink] iteration being");
        if let Ok(Message::Data(mut msg)) = self.input.recv_async().await {
            let data = msg.try_get::<usize>()?;
            assert_eq!(*data, COUNTER.load(Ordering::Relaxed));
        }

        Ok(())
    }
}

// OPERATORS

struct NoOp {
    input: Input,
    output: Output,
}

#[async_trait]
impl Operator for NoOp {
    async fn new(
        _context: Context,
        _configuration: Option<Configuration>,
        mut inputs: Inputs,
        mut outputs: Outputs,
    ) -> Result<Self> {
        println!("[NoOp] constructor");
        Ok(NoOp {
            input: inputs.take(SOURCE).unwrap(),
            output: outputs.take(DESTINATION).unwrap(),
        })
    }
}

#[async_trait]
impl Node for NoOp {
    async fn iteration(&self) -> Result<()> {
        println!("[NoOp] iteration being");
        if let Ok(Message::Data(mut msg)) = self.input.recv_async().await {
            let data = msg.try_get::<usize>()?;
            println!("[NoOp] got data {:?}", data);
            assert_eq!(*data, COUNTER.load(Ordering::Relaxed));
            self.output.send_async(*data, None).await?;
            println!("[NoOp] sent data");
        }
        println!("[NoOp] iteration done");
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
        outputs: vec![PortRecord {
            uid: 0,
            port_id: SOURCE.into(),
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

    let sink_record = SinkRecord {
        id: "generic-sink".into(),
        uid: 1,
        inputs: vec![PortRecord {
            uid: 2,
            port_id: SOURCE.into(),
            port_type: "int".into(),
        }],
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

    dataflow.add_operator(
        no_op_record,
        |context: Context,
         configuration: Option<Configuration>,
         inputs: Inputs,
         outputs: Outputs| {
            Box::pin(async {
                let node = NoOp::new(context, configuration, inputs, outputs).await?;
                Ok(Arc::new(node) as Arc<dyn Node>)
            })
        },
    );

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

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

// mod types;

// use async_std::sync::Arc;
// use async_trait::async_trait;
// use flume::{bounded, Receiver};
// use std::collections::HashMap;
// use types::{CounterState, ZFUsize};
// use zenoh_flow::model::link::PortDescriptor;
// use zenoh_flow::model::{InputDescriptor, OutputDescriptor};
// use zenoh_flow::runtime::dataflow::instance::DataflowInstance;
// use zenoh_flow::runtime::dataflow::loader::{Loader, LoaderConfig};
// use zenoh_flow::runtime::RuntimeContext;
// use zenoh_flow::{
//     default_output_rule, zf_empty_state, Configuration, Context, Data, InputToken, Node,
//     NodeOutput, Operator, PortId, Sink, Source, State, ZFError, ZFResult,
// };

// static SOURCE: &str = "Source";
// static DESTINATION: &str = "Counter";

// // SOURCE 1

// struct CounterSource {
//     rx: Receiver<()>,
// }

// unsafe impl Send for CounterSource {}
// unsafe impl Sync for CounterSource {}

// impl CounterSource {
//     pub fn new(rx: Receiver<()>) -> Self {
//         Self { rx }
//     }
// }

// #[async_trait]
// impl Source for CounterSource {
//     async fn run(&self, _context: &mut Context, state: &mut State) -> zenoh_flow::ZFResult<Data> {
//         let _ = self.rx.recv_async().await;
//         let s = state.try_get::<CounterState>()?;
//         Ok(Data::from::<ZFUsize>(ZFUsize(s.add_fetch())))
//     }
// }

// impl Node for CounterSource {
//     fn initialize(&self, _configuration: &Option<Configuration>) -> ZFResult<State> {
//         Ok(CounterState::new_as_state())
//     }

//     fn finalize(&self, _state: &mut State) -> ZFResult<()> {
//         Ok(())
//     }
// }

// // SINK

// struct ExampleGenericSink;

// #[async_trait]
// impl Sink for ExampleGenericSink {
//     async fn run(
//         &self,
//         _context: &mut Context,
//         state: &mut State,
//         mut input: zenoh_flow::runtime::message::DataMessage,
//     ) -> zenoh_flow::ZFResult<()> {
//         let data = input.get_inner_data().try_get::<ZFUsize>()?;
//         let s = state.try_get::<CounterState>()?;
//         //
//         // The entire test is performed here: we have set DropOdd to drop all values that are Odd.
//         // So every time the Sink receives data, the value received should be twice as much as its
//         // internal counter (represented here by `CounterState`).
//         //
//         assert_eq!(s.add_fetch() * 2, data.0);
//         Ok(())
//     }
// }

// impl Node for ExampleGenericSink {
//     fn initialize(&self, _configuration: &Option<Configuration>) -> ZFResult<State> {
//         Ok(CounterState::new_as_state())
//     }

//     fn finalize(&self, _state: &mut State) -> ZFResult<()> {
//         Ok(())
//     }
// }

// // OPERATOR

// #[derive(Debug)]
// struct DropOdd;

// impl Operator for DropOdd {
//     fn input_rule(
//         &self,
//         _context: &mut zenoh_flow::Context,
//         _state: &mut State,
//         tokens: &mut HashMap<PortId, zenoh_flow::InputToken>,
//     ) -> zenoh_flow::ZFResult<bool> {
//         let source: PortId = SOURCE.into();
//         let input_token = tokens
//             .get_mut(&source)
//             .ok_or_else(|| ZFError::InvalidData(SOURCE.to_string()))?;
//         if let InputToken::Ready(data_token) = input_token {
//             let data = data_token.get_data_mut();
//             if data.try_get::<ZFUsize>()?.0 % 2 != 0 {
//                 input_token.set_action_drop();
//                 return Ok(false);
//             }

//             return Ok(true);
//         }

//         Err(ZFError::InvalidData(SOURCE.to_string()))
//     }

//     fn run(
//         &self,
//         _context: &mut zenoh_flow::Context,
//         _state: &mut State,
//         inputs: &mut HashMap<PortId, zenoh_flow::runtime::message::DataMessage>,
//     ) -> zenoh_flow::ZFResult<HashMap<zenoh_flow::PortId, Data>> {
//         let mut results: HashMap<PortId, Data> = HashMap::new();

//         let source: PortId = SOURCE.into();
//         let mut data_msg = inputs
//             .remove(&source)
//             .ok_or_else(|| ZFError::InvalidData("No data".to_string()))?;
//         let data = data_msg.get_inner_data().try_get::<ZFUsize>()?;

//         assert_eq!(data.0 % 2, 0);

//         results.insert(DESTINATION.into(), Data::from::<ZFUsize>(ZFUsize(data.0)));
//         Ok(results)
//     }

//     fn output_rule(
//         &self,
//         _context: &mut zenoh_flow::Context,
//         state: &mut State,
//         outputs: HashMap<PortId, Data>,
//     ) -> zenoh_flow::ZFResult<HashMap<zenoh_flow::PortId, NodeOutput>> {
//         default_output_rule(state, outputs)
//     }
// }

// impl Node for DropOdd {
//     fn initialize(&self, _configuration: &Option<Configuration>) -> ZFResult<State> {
//         zf_empty_state!()
//     }

//     fn finalize(&self, _state: &mut State) -> ZFResult<()> {
//         Ok(())
//     }
// }

// // Run dataflow in single runtime
// async fn single_runtime() {
//     env_logger::init();

//     let (tx, rx) = bounded::<()>(1); // Channel used to trigger the source

//     let session = Arc::new(zenoh::open(zenoh::config::Config::default()).await.unwrap());
//     let hlc = async_std::sync::Arc::new(uhlc::HLC::default());
//     let rt_uuid = uuid::Uuid::new_v4();
//     let ctx = RuntimeContext {
//         session,
//         hlc,
//         loader: Arc::new(Loader::new(LoaderConfig::new())),
//         runtime_name: format!("test-runtime-{}", rt_uuid).into(),
//         runtime_uuid: rt_uuid,
//     };

//     let mut dataflow =
//         zenoh_flow::runtime::dataflow::Dataflow::new(ctx.clone(), "test".into(), None);

//     let source = Arc::new(CounterSource::new(rx));
//     let sink = Arc::new(ExampleGenericSink {});
//     let operator = Arc::new(DropOdd {});

//     dataflow
//         .try_add_static_source(
//             SOURCE.into(),
//             PortDescriptor {
//                 port_id: SOURCE.into(),
//                 port_type: "int".into(),
//             },
//             source.initialize(&None).unwrap(),
//             source,
//         )
//         .unwrap();

//     dataflow
//         .try_add_static_sink(
//             "generic-sink".into(),
//             PortDescriptor {
//                 port_id: DESTINATION.into(),
//                 port_type: "int".into(),
//             },
//             sink.initialize(&None).unwrap(),
//             sink,
//         )
//         .unwrap();

//     dataflow
//         .try_add_static_operator(
//             "noop".into(),
//             vec![PortDescriptor {
//                 port_id: SOURCE.into(),
//                 port_type: "int".into(),
//             }],
//             vec![PortDescriptor {
//                 port_id: DESTINATION.into(),
//                 port_type: "int".into(),
//             }],
//             operator.initialize(&None).unwrap(),
//             operator,
//         )
//         .unwrap();

//     dataflow
//         .try_add_link(
//             OutputDescriptor {
//                 node: SOURCE.into(),
//                 output: SOURCE.into(),
//             },
//             InputDescriptor {
//                 node: "noop".into(),
//                 input: SOURCE.into(),
//             },
//             None,
//             None,
//             None,
//         )
//         .unwrap();

//     dataflow
//         .try_add_link(
//             OutputDescriptor {
//                 node: "noop".into(),
//                 output: DESTINATION.into(),
//             },
//             InputDescriptor {
//                 node: "generic-sink".into(),
//                 input: DESTINATION.into(),
//             },
//             None,
//             None,
//             None,
//         )
//         .unwrap();

//     let mut instance = DataflowInstance::try_instantiate(dataflow).unwrap();

//     let ids = instance.get_nodes();
//     for id in &ids {
//         instance.start_node(id).await.unwrap();
//     }

//     tx.send_async(()).await.unwrap();
//     tx.send_async(()).await.unwrap();
//     tx.send_async(()).await.unwrap();
//     tx.send_async(()).await.unwrap();

//     zenoh_flow::async_std::task::sleep(std::time::Duration::from_secs(1)).await;

//     for id in &instance.get_sources() {
//         instance.stop_node(id).await.unwrap()
//     }

//     for id in &instance.get_operators() {
//         instance.stop_node(id).await.unwrap()
//     }

//     for id in &instance.get_sinks() {
//         instance.stop_node(id).await.unwrap()
//     }
// }

// #[test]
// fn action_drop() {
//     let h1 = async_std::task::spawn(async move { single_runtime().await });

//     async_std::task::block_on(async move { h1.await })
// }

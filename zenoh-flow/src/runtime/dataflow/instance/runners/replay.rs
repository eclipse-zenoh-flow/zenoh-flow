// //
// // Copyright (c) 2022 ZettaScale Technology
// //
// // This program and the accompanying materials are made available under the
// // terms of the Eclipse Public License 2.0 which is available at
// // http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// // which is available at https://www.apache.org/licenses/LICENSE-2.0.
// //
// // SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
// //
// // Contributors:
// //   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
// //

// use super::{Runner, RunnerKind};
// use crate::async_std::sync::{Arc, Mutex};
// use crate::runtime::dataflow::instance::link::{LinkReceiver, LinkSender};
// use crate::runtime::message::Message;
// use crate::runtime::InstanceContext;
// use crate::{NodeId, PortId, PortType, ZFError, ZFResult};
// use async_trait::async_trait;
// use std::collections::HashMap;

// /// The Replay node.
// #[derive(Clone)]
// pub struct ZenohReplay {
//     pub(crate) id: NodeId,
//     pub(crate) context: InstanceContext,
//     pub(crate) node_id: NodeId,
//     pub(crate) port_id: PortId,
//     pub(crate) port_type: PortType,
//     pub(crate) resource_name: String,
//     pub(crate) is_running: Arc<Mutex<bool>>,
//     pub(crate) links: Arc<Mutex<Vec<LinkSender>>>,
// }

// impl ZenohReplay {
//     /// Tries to create a replay node, that will replay from the given
//     /// `resource_name`.
//     ///
//     /// # Errors
//     /// It fails if the ports are not connected correctly.
//     pub fn try_new(
//         id: NodeId,
//         context: InstanceContext,
//         node_id: NodeId,
//         port_id: PortId,
//         port_type: PortType,
//         links: Vec<LinkSender>,
//         resource_name: String,
//     ) -> ZFResult<Self> {
//         if links.len() != 1 {
//             return Err(ZFError::IOError(format!(
//                 "Expected exactly one link for port < {} > for Replay < {} >, found: {}",
//                 &port_id,
//                 &node_id,
//                 links.len()
//             )));
//         }

//         Ok(Self {
//             id,
//             context,
//             node_id,
//             port_id,
//             port_type,
//             resource_name,
//             is_running: Arc::new(Mutex::new(false)),
//             links: Arc::new(Mutex::new(links)),
//         })
//     }

//     /// Sends a data to the link.
//     ///
//     /// This is the replay.
//     ///
//     /// # Errors
//     /// An error variant is returned in case of:
//     /// -  link send fails
//     async fn send_data(&self, msg: Message) -> ZFResult<()> {
//         log::trace!("ZenohReplay {} - {} - SendData ", self.id, self.node_id);
//         let links = self.links.lock().await;
//         let msg = Arc::new(msg);
//         for link in links.iter() {
//             log::trace!("ZenohReplay - OUT =>{:?} ", msg);
//             link.send(msg.clone()).await?;
//         }
//         Ok(())
//     }

//     /// Starts the replay.
//     async fn start(&self) {
//         *self.is_running.lock().await = true;
//     }
// }

// #[async_trait]
// impl Runner for ZenohReplay {
//     fn get_id(&self) -> NodeId {
//         self.id.clone()
//     }
//     fn get_kind(&self) -> RunnerKind {
//         RunnerKind::Source
//     }
//     async fn add_output(&self, output: LinkSender) -> ZFResult<()> {
//         (*self.links.lock().await).push(output);
//         Ok(())
//     }

//     async fn add_input(&self, _input: LinkReceiver) -> ZFResult<()> {
//         Err(ZFError::SourceDoNotHaveInputs)
//     }

//     async fn clean(&self) -> ZFResult<()> {
//         Ok(())
//     }

//     fn get_outputs(&self) -> HashMap<PortId, PortType> {
//         let mut outputs = HashMap::with_capacity(1);
//         outputs.insert(self.port_id.clone(), self.port_type.clone());
//         outputs
//     }

//     fn get_inputs(&self) -> HashMap<PortId, PortType> {
//         HashMap::with_capacity(0)
//     }

//     async fn get_outputs_links(&self) -> HashMap<PortId, Vec<LinkSender>> {
//         let mut outputs = HashMap::with_capacity(1);
//         outputs.insert(self.port_id.clone(), self.links.lock().await.clone());
//         outputs
//     }

//     async fn take_input_links(&self) -> HashMap<PortId, LinkReceiver> {
//         HashMap::with_capacity(0)
//     }

//     async fn start_recording(&self) -> ZFResult<String> {
//         Err(ZFError::Unsupported)
//     }

//     async fn stop_recording(&self) -> ZFResult<String> {
//         Err(ZFError::Unsupported)
//     }

//     async fn is_recording(&self) -> bool {
//         false
//     }

//     async fn is_running(&self) -> bool {
//         *self.is_running.lock().await
//     }

//     fn stop(&mut self) -> ZFResult<()> {
//         // *self.is_running.lock().await = false;

//         // FIXME
//         unimplemented!()
//     }

//     fn start(&mut self) -> ZFResult<()> {
//         // self.start().await;

//         // let res = {
//         //     log::debug!("ZenohReplay - {} - Started", self.resource_name);
//         //     let query_target = QueryTarget {
//         //         kind: queryable::STORAGE,
//         //         target: Target::default(),
//         //     };
//         //     let res_name = format!("{}?(starttime=0)", self.resource_name);
//         //     let replies = self
//         //         .context
//         //         .runtime
//         //         .session
//         //         .get(&res_name)
//         //         .target(query_target)
//         //         .consolidation(QueryConsolidation::none())
//         //         .await?;

//         //     // Placeholder
//         //     let mut last_ts = self.context.runtime.hlc.new_timestamp();

//         //     // Here we need to get all the data and then order it.
//         //     let data = replies.collect::<Vec<Reply>>().await;
//         //     let mut zf_data: Vec<Message> = data
//         //         .iter()
//         //         .filter_map(|msg| {
//         //             bincode::deserialize::<Message>(&msg.sample.value.payload.contiguous()).ok()
//         //         })
//         //         .collect();
//         //     zf_data.sort();
//         //     log::trace!("ZenohReplay - Total samples {} ", zf_data.len());

//         //     for de in zf_data {
//         //         log::trace!("ZenohReplay - {}<={:?} ", self.resource_name, de);
//         //         match &de {
//         //             Message::Control(ref ctrl_msg) => match &ctrl_msg {
//         //                 ControlMessage::RecordingStart(ref ts) => {
//         //                     log::trace!("ZenohReplay - Recording start {:?} ", ts);
//         //                     last_ts = ts.timestamp;
//         //                 }
//         //                 ControlMessage::RecordingStop(ref rs) => {
//         //                     log::trace!("ZenohReplay - Recording Stop {:?} ", rs);
//         //                 } // Commented because Control messages are not yet defined.
//         //                   // _ => {
//         //                   //     self.send_data(de).await?;
//         //                   // }
//         //             },
//         //             Message::Data(ref data_msg) => {
//         //                 let data_ts = data_msg.timestamp;
//         //                 let wait_time = data_ts.get_diff_duration(&last_ts);

//         //                 log::trace!("ZenohReplay - Wait for {:?} ", wait_time);
//         //                 task::sleep(wait_time).await;

//         //                 self.send_data(de).await?;

//         //                 // Updating last sent timestamp
//         //                 last_ts = data_ts;
//         //             }
//         //         }
//         //     }
//         // };

//         // self.stop().await;

//         // Ok(res)

//         // FIXME
//         unimplemented!()
//     }
// }

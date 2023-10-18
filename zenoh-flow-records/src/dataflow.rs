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

use crate::{
    connectors::{ZenohReceiver, ZenohSender},
    LinkRecord, OperatorRecord, SinkRecord, SourceRecord,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;
use zenoh_flow_commons::{NodeId, RuntimeId};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DataFlowRecord {
    pub uuid: Uuid,
    pub flow: Arc<str>,
    pub sources: HashMap<NodeId, SourceRecord>,
    pub operators: HashMap<NodeId, OperatorRecord>,
    pub sinks: HashMap<NodeId, SinkRecord>,
    pub receivers: HashMap<NodeId, ZenohReceiver>,
    pub senders: HashMap<NodeId, ZenohSender>,
    pub links: Vec<LinkRecord>,
    pub mapping: HashMap<NodeId, RuntimeId>,
}

// impl DataFlowRecord {
//     pub fn from_flattened(data_flow: FlattenedDataFlowDescriptor, runtime: RuntimeContext) -> Self {
//         let uuid = Uuid::new_v4();

//         let sources = data_flow
//             .sources
//             .into_iter()
//             .map(|source| {
//                 (
//                     source.id.clone(),
//                     SourceRecord::from_flattened(source, runtime.id.clone()),
//                 )
//             })
//             .collect::<HashMap<_, _>>();

//         let operators = data_flow
//             .operators
//             .into_iter()
//             .map(|operator| {
//                 (
//                     operator.id.clone(),
//                     OperatorRecord::from_flattened(operator, runtime.id.clone()),
//                 )
//             })
//             .collect::<HashMap<_, _>>();

//         let sinks = data_flow
//             .sinks
//             .into_iter()
//             .map(|sink| {
//                 (
//                     sink.id.clone(),
//                     SinkRecord::from_flattened(sink, runtime.id.clone()),
//                 )
//             })
//             .collect::<HashMap<_, _>>();

//         let (links, receivers, senders) = connect_runtimes(data_flow.links, runtime);

//         Self {
//             uuid,
//             flow: data_flow.flow,
//             sources,
//             operators,
//             sinks,
//             receivers,
//             senders,
//             links,
//         }
//     }
// }

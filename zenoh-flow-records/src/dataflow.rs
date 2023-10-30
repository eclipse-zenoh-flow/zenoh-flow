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

use crate::connectors::{ReceiverRecord, SenderRecord};
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use zenoh_flow_commons::{NodeId, RecordId, Result, RuntimeId};
use zenoh_flow_descriptors::{
    FlattenedDataFlowDescriptor, FlattenedOperatorDescriptor, FlattenedSinkDescriptor,
    FlattenedSourceDescriptor, InputDescriptor, LinkDescriptor, OutputDescriptor,
};
use zenoh_keyexpr::OwnedKeyExpr;

const SENDER_SUFFIX: &str = "__zenoh_flow_sender";
const RECEIVER_SUFFIX: &str = "__zenoh_flow_receiver";

/// TODO@J-Loudet
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DataFlowRecord {
    pub(crate) record_id: RecordId,
    pub(crate) name: Arc<str>,
    pub sources: HashMap<NodeId, FlattenedSourceDescriptor>,
    pub operators: HashMap<NodeId, FlattenedOperatorDescriptor>,
    pub sinks: HashMap<NodeId, FlattenedSinkDescriptor>,
    pub senders: HashMap<NodeId, SenderRecord>,
    pub receivers: HashMap<NodeId, ReceiverRecord>,
    pub links: Vec<LinkDescriptor>,
    pub mapping: HashMap<NodeId, RuntimeId>,
}

impl DataFlowRecord {
    /// TODO@J-Loudet
    ///
    /// # Errors
    ///
    /// The creation of the should, in theory, not fail. The only failure point is during the creation of the
    /// connectors: the [`SenderRecord`] and [`ReceiverRecord`] that are automatically generated when two nodes that
    /// need to communicate are located on different runtimes.
    ///
    /// To generate these connectors, a Zenoh key expression is computed. Computing this expression can result in an
    /// error if the [`NodeId`] or [`PortId`] are not valid chunks. This should not happen as, when deserializing from a
    /// descriptor, verifications are performed.
    pub fn try_new(
        data_flow: FlattenedDataFlowDescriptor,
        default_runtime: &RuntimeId,
    ) -> Result<Self> {
        let FlattenedDataFlowDescriptor {
            name,
            sources,
            operators,
            sinks,
            mut links,
            mut mapping,
        } = data_flow;

        let record_id = uuid::Uuid::new_v4().into();

        // Nodes that are not running on the same runtime need to be connected.
        let mut additional_links = Vec::default();
        let mut receivers = HashMap::default();
        let mut senders = HashMap::default();

        for link in links.iter_mut() {
            let runtime_from = mapping
                .entry(link.from.node.clone())
                .or_insert(default_runtime.clone())
                .clone(); // extra clone to not mutably borrow `mapping` twice
            let runtime_to = mapping
                .entry(link.to.node.clone())
                .or_insert(default_runtime.clone());

            if runtime_from != *runtime_to {
                let key_expr_str = format!("{}/{}/{}", record_id, link.from.node, link.from.output);
                let key_expression =
                    OwnedKeyExpr::autocanonize(key_expr_str.clone()).map_err(|e| {
                        // NOTE: This error should not happen as we ensure that (i) all node ids and port ids are valid
                        // key expressions in their canonical form and (ii) they do not contain any of '*', '#', '?' or
                        // '$' characters (look for `deserialize_id` in zenoh_flow_commons).
                        anyhow!(
                            r#"
Zenoh-Flow encountered a fatal internal error:

  The key expression generated to connect the nodes < {} > and < {} > is not valid:

  {}

Caused by:

{:?}
"#,
                            link.from.node,
                            link.to.node,
                            key_expr_str,
                            e
                        )
                    })?;

                let sender_id: NodeId = format!("{}{SENDER_SUFFIX}", link.from.node).into();
                let receiver_id: NodeId = format!("{}{RECEIVER_SUFFIX}", link.to.node).into();

                let mut input = InputDescriptor {
                    node: sender_id.clone(),
                    input: key_expression.to_string().into(),
                };
                input = std::mem::replace(&mut link.to, input);
                let output = OutputDescriptor {
                    node: receiver_id.clone(),
                    output: key_expression.to_string().into(),
                };

                additional_links.push(LinkDescriptor {
                    from: output,
                    to: input,
                });

                senders.insert(
                    sender_id.clone(),
                    SenderRecord {
                        id: sender_id,
                        resource: key_expression.clone(),
                    },
                );

                receivers.insert(
                    receiver_id.clone(),
                    ReceiverRecord {
                        id: receiver_id,
                        resource: key_expression,
                    },
                );
            }
        }
        links.append(&mut additional_links);

        Ok(Self {
            record_id,
            name,
            sources: sources
                .into_iter()
                .map(|source| (source.id.clone(), source))
                .collect::<HashMap<_, _>>(),
            operators: operators
                .into_iter()
                .map(|operator| (operator.id.clone(), operator))
                .collect::<HashMap<_, _>>(),
            sinks: sinks
                .into_iter()
                .map(|sink| (sink.id.clone(), sink))
                .collect::<HashMap<_, _>>(),
            senders,
            receivers,
            links,
            mapping,
        })
    }

    /// Returns the unique identifier of this [`DataFlowRecord`].
    ///
    /// The identifier is automatically generated by Zenoh-Flow upon creation.
    ///
    /// # Performance
    ///
    /// The id is internally stored behind an [`Arc`](std::sync::Arc) so there is limited overhead to cloning it.
    pub fn id(&self) -> &RecordId {
        &self.record_id
    }

    /// Returns the name of the data flow from which this [`DataFlowRecord`] was generated.
    pub fn name(&self) -> &Arc<str> {
        &self.name
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

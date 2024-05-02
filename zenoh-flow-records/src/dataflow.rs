//
// Copyright (c) 2021 - 2024 ZettaScale Technology
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

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::{anyhow, bail, Context};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use zenoh_flow_commons::{InstanceId, NodeId, Result, RuntimeId};
use zenoh_flow_descriptors::{
    FlattenedDataFlowDescriptor, FlattenedOperatorDescriptor, FlattenedSinkDescriptor,
    FlattenedSourceDescriptor, InputDescriptor, LinkDescriptor, OutputDescriptor,
};
use zenoh_keyexpr::OwnedKeyExpr;

use crate::connectors::{ReceiverRecord, SenderRecord};

const SENDER_SUFFIX: &str = "__zenoh_flow_sender";
const RECEIVER_SUFFIX: &str = "__zenoh_flow_receiver";

/// A `DataFlowRecord` represents a single deployment of a [FlattenedDataFlowDescriptor] on an infrastructure, i.e. on a
/// set of Zenoh-Flow runtimes.
///
/// A `DataFlowRecord` can only be created by processing a [FlattenedDataFlowDescriptor] and providing a default
/// Zenoh-Flow [runtime](RuntimeId) -- that will manage the nodes that have no explicit mapping. See the
/// [try_new](DataFlowRecord::try_new()) method.
///
/// The differences between a [FlattenedDataFlowDescriptor] and a [DataFlowRecord] are:
/// - In a record, all nodes are mapped to a Zenoh-Flow runtime.
/// - A record leverages two additional nodes: [Sender](SenderRecord) and [Receiver](ReceiverRecord). These nodes take
///   care of connecting nodes that are running on different Zenoh-Flow runtimes.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DataFlowRecord {
    pub(crate) id: InstanceId,
    pub(crate) name: Arc<str>,
    pub(crate) sources: HashMap<NodeId, FlattenedSourceDescriptor>,
    pub(crate) operators: HashMap<NodeId, FlattenedOperatorDescriptor>,
    pub(crate) sinks: HashMap<NodeId, FlattenedSinkDescriptor>,
    pub(crate) senders: HashMap<NodeId, SenderRecord>,
    pub(crate) receivers: HashMap<NodeId, ReceiverRecord>,
    pub(crate) links: Vec<LinkDescriptor>,
    pub(crate) mapping: HashMap<RuntimeId, HashSet<NodeId>>,
}

impl DataFlowRecord {
    /// Attempts to create a [DataFlowRecord] from the provided [FlattenedDataFlowDescriptor], assigning nodes without
    /// a mapping to the default [runtime](RuntimeId).
    ///
    /// If the [FlattenedDataFlowDescriptor] did not specify a unique identifier, one will be randomly generated.
    ///
    /// # Errors
    ///
    /// The creation of the [DataFlowRecord] should, in theory, not fail. The only failure point is during the creation
    /// of the connectors: the [Sender](SenderRecord) and [Receiver](ReceiverRecord) that are automatically generated
    /// when two nodes that need to communicate are located on different runtimes.
    ///
    /// To generate these connectors, a Zenoh key expression is computed. Computing this expression can result in an
    /// error if the [NodeId] or [PortId](zenoh_flow_commons::PortId) are not valid chunks (see Zenoh's
    /// [keyexpr](https://docs.rs/zenoh-keyexpr/0.10.1-rc/zenoh_keyexpr/key_expr/struct.keyexpr.html) documentation for
    /// more details).
    ///
    /// Node that this should not happen if the [FlattenedDataFlowDescriptor] was obtained by parsing and flattening a
    /// [DataFlowDescriptor](zenoh_flow_descriptors::DataFlowDescriptor).
    pub fn try_new(
        data_flow: &FlattenedDataFlowDescriptor,
        default_runtime: &RuntimeId,
    ) -> Result<Self> {
        let FlattenedDataFlowDescriptor {
            id,
            name,
            sources,
            operators,
            sinks,
            mut links,
            mut mapping,
        } = data_flow.clone();

        let id = id.unwrap_or_else(|| Uuid::new_v4().into());

        // Nodes that are not running on the same runtime need to be connected.
        let mut additional_links = Vec::default();
        let mut receivers = HashMap::default();
        let mut senders = HashMap::default();

        let mut default_mapping_if_unassigned = |node_id: &NodeId| {
            for (_, nodes) in mapping.iter() {
                if nodes.contains(node_id) {
                    return;
                }
            }

            let runtime_entry = mapping
                .entry(default_runtime.clone())
                .or_insert_with(HashSet::default);
            runtime_entry.insert(node_id.clone());
        };

        let sources = sources
            .into_iter()
            .map(|source| {
                default_mapping_if_unassigned(&source.id);
                (source.id.clone(), source)
            })
            .collect::<HashMap<_, _>>();

        let operators = operators
            .into_iter()
            .map(|operator| {
                default_mapping_if_unassigned(&operator.id);
                (operator.id.clone(), operator)
            })
            .collect::<HashMap<_, _>>();

        let sinks = sinks
            .into_iter()
            .map(|sink| {
                default_mapping_if_unassigned(&sink.id);
                (sink.id.clone(), sink)
            })
            .collect::<HashMap<_, _>>();

        let try_get_mapping = |node_id: &NodeId| -> Result<&RuntimeId> {
            for (runtime, nodes) in mapping.iter() {
                if nodes.contains(node_id) {
                    return Ok(runtime);
                }
            }

            bail!(
                r#"
Zenoh-Flow encountered a fatal error: the node < {} > is not mapped to a runtime.
Is its name valid (i.e. does it reference an actual node)?
"#,
                node_id
            )
        };

        let mut additional_mappings: HashMap<RuntimeId, HashSet<NodeId>> = HashMap::default();
        for link in links.iter_mut() {
            let runtime_from = try_get_mapping(&link.from.node)
                .context(format!("Failed to process link:\n{}", link))?;
            let runtime_to = try_get_mapping(&link.to.node)
                .context(format!("Failed to process link:\n{}", link))?;

            if runtime_from != runtime_to {
                let key_expr_str = format!("{}/{}/{}", id, link.from.node, link.from.output);
                let key_expression =
                    OwnedKeyExpr::autocanonize(key_expr_str.clone()).map_err(|e| {
                        // NOTE: This error should not happen as we ensure that (i) all node ids and port ids are valid
                        // key expressions in their canonical form and (ii) they do not contain any of '*', '#', '?' or
                        // '$' characters (look for `deserialize_id` in zenoh_flow_commons).
                        anyhow!(
                            r#"
Zenoh-Flow encountered a fatal internal error: the key expression generated to connect the nodes < {} > and < {} > is
not valid:
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
                    #[cfg(feature = "shared-memory")]
                    shared_memory: link.shared_memory,
                });

                senders.insert(
                    sender_id.clone(),
                    SenderRecord {
                        id: sender_id.clone(),
                        resource: key_expression.clone(),
                    },
                );
                additional_mappings
                    .entry(runtime_from.clone())
                    .or_insert_with(HashSet::default)
                    .insert(sender_id);

                receivers.insert(
                    receiver_id.clone(),
                    ReceiverRecord {
                        id: receiver_id.clone(),
                        resource: key_expression,
                    },
                );
                additional_mappings
                    .entry(runtime_to.clone())
                    .or_insert_with(HashSet::default)
                    .insert(receiver_id);
            }
        }

        links.append(&mut additional_links);
        additional_mappings
            .into_iter()
            .for_each(|(runtime_id, nodes)| {
                mapping
                    .entry(runtime_id)
                    .or_insert_with(HashSet::default)
                    .extend(nodes);
            });

        Ok(Self {
            id,
            name,
            sources,
            operators,
            sinks,
            senders,
            receivers,
            links,
            mapping,
        })
    }

    /// Returns the unique identifier of this [`DataFlowRecord`].
    ///
    /// # Performance
    ///
    /// The id is internally stored behind an [`Arc`](std::sync::Arc) so there is limited overhead to cloning it.
    pub fn instance_id(&self) -> &InstanceId {
        &self.id
    }

    /// Returns the name of the data flow from which this [`DataFlowRecord`] was generated.
    pub fn name(&self) -> &Arc<str> {
        &self.name
    }

    /// Returns the mapping of the data flow: which Zenoh-Flow runtime manages which set of nodes.
    pub fn mapping(&self) -> &HashMap<RuntimeId, HashSet<NodeId>> {
        &self.mapping
    }

    /// Returns the set of [Senders](SenderRecord) of the data flow.
    ///
    /// A [Sender](SenderRecord) sends data, through a publication on Zenoh, to [Receiver(s)](ReceiverRecord).
    pub fn senders(&self) -> &HashMap<NodeId, SenderRecord> {
        &self.senders
    }

    /// Returns the set of [Receivers](ReceiverRecord) of the data flow.
    ///
    /// A [Receiver](ReceiverRecord) receives data, through a subscription on Zenoh, from [Sender(s)](SenderRecord).
    pub fn receivers(&self) -> &HashMap<NodeId, ReceiverRecord> {
        &self.receivers
    }

    /// Returns the set of links of the data flow: how the nodes are connected.
    ///
    /// Compared to links found in a [FlattenedDataFlowDescriptor], the links in a [DataFlowRecord] have been updated to
    /// take into account the [Sender](SenderRecord) and [Receiver](ReceiverRecord) connecting the Zenoh-Flow runtimes.
    pub fn links(&self) -> &[LinkDescriptor] {
        &self.links
    }

    /// Returns the set of [Source(s)](FlattenedSourceDescriptor) of the data flow.
    ///
    /// A `Source` will feed external data in the data flow to be processed by downstream nodes.
    pub fn sources(&self) -> &HashMap<NodeId, FlattenedSourceDescriptor> {
        &self.sources
    }

    /// Returns the set of [Operator(s)](FlattenedOperatorDescriptor) of the data flow.
    ///
    /// An `Operator` performs computation on the data it receives, either modifying it or producing new data that it
    /// forwards to downstream nodes.
    pub fn operators(&self) -> &HashMap<NodeId, FlattenedOperatorDescriptor> {
        &self.operators
    }

    /// Returns the set of [Sink(s)](FlattenedSinkDescriptor) of the data flow.
    ///
    /// A `Sink` exposes the result of the data flow pipeline, such that it can be ingested by external components.
    pub fn sinks(&self) -> &HashMap<NodeId, FlattenedSinkDescriptor> {
        &self.sinks
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

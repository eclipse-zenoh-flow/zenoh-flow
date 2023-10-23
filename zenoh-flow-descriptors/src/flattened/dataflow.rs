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

use std::{collections::HashMap, fmt::Display, mem, sync::Arc};

use crate::{
    FlattenedOperatorDescriptor, FlattenedSinkDescriptor, FlattenedSourceDescriptor,
    InputDescriptor, LinkDescriptor, OutputDescriptor,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use zenoh_flow_commons::{NodeId, PortId, RuntimeContext, RuntimeId, SharedMemoryParameters};
use zenoh_flow_records::{DataFlowRecord, ZenohReceiver, ZenohSender};

/// TODO@J-Loudet Documentation?
///
/// # Examples
///
/// ```
/// use zenoh_flow_descriptors::FlattenedDataFlowDescriptor;
///
/// let yaml = "
/// flow: DataFlow
///
/// sources:
///   - id: Source-0
///     description: Source
///     configuration:
///       foo: bar
///       answer: 0
///     uri: file:///home/zenoh-flow/node/libsource.so
///     outputs:
///       - out-operator
///
/// operators:
///   - id: Operator-1
///     description: Operator
///     configuration:
///       foo: bar
///       answer: 1
///     uri: file:///home/zenoh-flow/node/liboperator.so
///     inputs:
///       - in-source
///     outputs:
///       - out-sink
///
/// sinks:
///   - id: Sink-2
///     description: Sink
///     configuration:
///       foo: bar
///       answer: 2
///     uri: file:///home/zenoh-flow/node/libsink.so
///     inputs:
///       - in-operator
///
/// links:
///   - from:
///       node: Source-0
///       output : out-operator
///     to:
///       node : Operator-1
///       input : in-source
///
///   - from:
///       node : Operator-1
///       output : out-sink
///     to:
///       node : Sink-2
///       input : in-operator
/// ";
///
/// let data_flow_yaml = serde_yaml::from_str::<FlattenedDataFlowDescriptor>(yaml).unwrap();
///
/// let json = "
/// {
///   \"flow\": \"DataFlow\",
///
///   \"configuration\": {
///     \"foo\": \"bar\"
///   },
///
///   \"sources\": [
///     {
///       \"id\": \"Source-0\",
///       \"description\": \"Source\",
///       \"configuration\": {
///         \"foo\": \"bar\",
///         \"answer\": 0
///       },
///       \"uri\": \"file:///home/zenoh-flow/node/libsource.so\",
///       \"outputs\": [
///         \"out-operator\"
///       ],
///       \"mapping\": \"zenoh-flow-plugin-0\"
///     }
///   ],
///
///   \"operators\": [
///     {
///       \"id\": \"Operator-1\",
///       \"description\": \"Operator\",
///       \"configuration\": {
///         \"foo\": \"bar\",
///         \"answer\": 1
///       },
///       \"uri\": \"file:///home/zenoh-flow/node/liboperator.so\",
///       \"inputs\": [
///         \"in-source\"
///       ],
///       \"outputs\": [
///         \"out-sink\"
///       ]
///     }
///   ],
///
///   \"sinks\": [
///     {
///       \"id\": \"Sink-2\",
///       \"description\": \"Sink\",
///       \"configuration\": {
///         \"foo\": \"bar\",
///         \"answer\": 2
///       },
///       \"uri\": \"file:///home/zenoh-flow/node/libsink.so\",
///       \"inputs\": [
///         \"in-operator\"
///       ]
///     }
///   ],
///
///   \"links\": [
///     {
///       \"from\": {
///         \"node\": \"Source-0\",
///         \"output\": \"out-operator\"
///       },
///       \"to\": {
///         \"node\": \"Operator-1\",
///         \"input\": \"in-source\"
///       }
///     },
///     {
///       \"from\": {
///         \"node\": \"Operator-1\",
///         \"output\": \"out-sink\"
///       },
///       \"to\": {
///         \"node\": \"Sink-2\",
///         \"input\": \"in-operator\"
///       }
///     }
///   ]
/// }
/// ";
///
/// let data_flow_json = serde_json::from_str::<FlattenedDataFlowDescriptor>(json).unwrap();
/// assert_eq!(data_flow_yaml, data_flow_json);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlattenedDataFlowDescriptor {
    pub flow: Arc<str>,
    pub sources: Vec<FlattenedSourceDescriptor>,
    pub operators: Vec<FlattenedOperatorDescriptor>,
    pub sinks: Vec<FlattenedSinkDescriptor>,
    pub links: Vec<LinkDescriptor>,
    #[serde(default)]
    pub mapping: HashMap<NodeId, RuntimeId>,
}

// TODO@J-Loudet
impl Display for FlattenedDataFlowDescriptor {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl FlattenedDataFlowDescriptor {
    /// Assign the nodes without an explicit mapping to the provided runtime and generate the connections between the
    /// nodes running on different runtimes.
    ///
    /// This method will consume the `FlattenedDataFlowDescriptor` and produce a `DataFlowRecord`.
    pub fn complete_mapping_and_connect(self, default_runtime: RuntimeContext) -> DataFlowRecord {
        let mut senders = HashMap::default();
        let mut receivers = HashMap::default();
        let mut connector_links = Vec::default();

        let flow_id = Uuid::new_v4();

        let FlattenedDataFlowDescriptor {
            flow,
            sources,
            operators,
            sinks,
            mut links,
            mut mapping,
        } = self;

        for link in links.iter_mut() {
            let runtime_from = mapping
                .entry(link.from.node.clone())
                .or_insert(default_runtime.id.clone())
                .clone();
            let runtime_to = mapping
                .entry(link.to.node.clone())
                .or_insert(default_runtime.id.clone())
                .clone();

            // The nodes will run on the same runtime, no need for connectors.
            if runtime_from == runtime_to {
                continue;
            }

            // Nodes are on different runtimes: we generate a special key expression for communications through Zenoh
            // (either Shared Memory or standard pub/sub).
            //
            // We also need to:
            // - update the link and replace the "to" part with the sender,
            // - add another link that goes from the receiver to the "to".
            let resource: Arc<str> = format!(
                "{}/{}/{}/{}",
                &flow, &flow_id, link.from.node, link.from.output
            )
            .into();

            let shared_memory_parameters = SharedMemoryParameters::from_configuration(
                &link.shared_memory,
                &default_runtime.shared_memory,
            );

            let sender_id: NodeId =
                format!("z-sender/{}/{}", link.from.node, link.from.output).into();
            let sender_input: PortId = format!("{}-input", sender_id).into();

            let receiver_id: NodeId =
                format!("z-receiver/{}/{}", link.to.node, link.to.input).into();
            let receiver_output: PortId = format!("{}-output", receiver_id).into();

            // Update the link, replacing "to" with the sender.
            let to = mem::replace(
                &mut link.to,
                InputDescriptor {
                    node: sender_id.clone(),
                    input: sender_input.clone(),
                },
            );

            // Create a new link, connecting the receiver to the  "to".
            connector_links.push(LinkDescriptor {
                from: OutputDescriptor {
                    node: receiver_id.clone(),
                    output: receiver_output.clone(),
                },
                to,
                shared_memory: link.shared_memory.clone(),
            });

            // Create the connector nodes — without forgetting their mapping.
            let sender = ZenohSender {
                id: sender_id.clone(),
                resource: resource.clone(),
                input: sender_input.clone(),
                shared_memory: shared_memory_parameters.clone(),
            };
            senders.insert(sender_id.clone(), sender);
            mapping.insert(sender_id, runtime_from);

            let receiver = ZenohReceiver {
                id: receiver_id.clone(),
                resource,
                output: receiver_output,
                shared_memory: shared_memory_parameters,
            };
            receivers.insert(receiver_id.clone(), receiver);
            mapping.insert(receiver_id, runtime_to);
        }

        // Add the new links.
        links.append(&mut connector_links);

        DataFlowRecord {
            uuid: flow_id,
            flow,
            sources: sources
                .into_iter()
                .map(|source| (source.id.clone(), source.into()))
                .collect::<HashMap<_, _>>(),
            operators: operators
                .into_iter()
                .map(|operator| (operator.id.clone(), operator.into()))
                .collect::<HashMap<_, _>>(),
            sinks: sinks
                .into_iter()
                .map(|sink| (sink.id.clone(), sink.into()))
                .collect::<HashMap<_, _>>(),
            receivers,
            senders,
            links: links
                .into_iter()
                .map(|link| link.into_record(&default_runtime.shared_memory))
                .collect::<Vec<_>>(),
            mapping,
        }
    }
}

#[cfg(test)]
#[path = "./tests.rs"]
mod tests;

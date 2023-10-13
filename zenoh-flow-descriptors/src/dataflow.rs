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
    CompositeOperatorDescriptor, FlattenedDataFlowDescriptor, LinkDescriptor, NodeDescriptor,
    SinkDescriptor, SourceDescriptor,
};

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use zenoh_flow_commons::{Configuration, IMergeOverwrite, NodeId, Result, RuntimeId, Vars};

/// TODO@J-Loudet Documentation?
///
/// # Example
///
/// ```
/// use zenoh_flow_descriptors::DataFlowDescriptor;
///
/// let yaml = "
/// flow: DataFlow
///
/// configuration:
///   foo: bar
///
/// sources:
///   - id: Source-0
///     descriptor: file:///home/zenoh-flow/nodes/source.yaml
///     configuration:
///       answer: 0
///
/// operators:
///   - id: Operator-1
///     descriptor: file:///home/zenoh-flow/nodes/operator.yaml
///     configuration:
///       answer: 1
///
/// sinks:
///   - id: Sink-2
///     descriptor: file:///home/zenoh-flow/nodes/sink.yaml
///     configuration:
///       answer: 2
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
///
/// mapping:
///   Source-0: zenoh-flow-plugin-0
/// ";
///
/// let data_flow_yaml = serde_yaml::from_str::<DataFlowDescriptor>(yaml).unwrap();
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
///       \"descriptor\": \"file:///home/zenoh-flow/nodes/source.yaml\",
///       \"configuration\": {
///         \"answer\": 0
///       }
///     }
///   ],
///
///   \"operators\": [
///     {
///       \"id\": \"Operator-1\",
///       \"descriptor\": \"file:///home/zenoh-flow/nodes/operator.yaml\",
///       \"configuration\": {
///         \"answer\": 1
///       }
///     }
///   ],
///
///   \"sinks\": [
///     {
///       \"id\": \"Sink-2\",
///       \"descriptor\": \"file:///home/zenoh-flow/nodes/sink.yaml\",
///       \"configuration\": {
///         \"answer\": 2
///       }
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
///   ],
///
///   \"mapping\": {
///     \"Source-0\": \"zenoh-flow-plugin-0\"
///   }
/// }
/// ";
///
/// let data_flow_json = serde_json::from_str::<DataFlowDescriptor>(json).unwrap();
/// assert_eq!(data_flow_yaml, data_flow_json);
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DataFlowDescriptor {
    pub flow: String,
    #[serde(default)]
    pub configuration: Configuration,
    pub operators: Vec<NodeDescriptor>,
    pub sources: Vec<NodeDescriptor>,
    pub sinks: Vec<NodeDescriptor>,
    pub links: Vec<LinkDescriptor>,
    pub mapping: Option<HashMap<NodeId, RuntimeId>>,
}

impl DataFlowDescriptor {
    pub fn flatten(self, vars: Vars) -> Result<FlattenedDataFlowDescriptor> {
        let DataFlowDescriptor {
            flow,
            configuration: flow_configuration,
            operators,
            sources,
            sinks,
            mut links,
            mapping,
        } = self;

        let mapping = mapping.unwrap_or_default();

        let mut flattened_sources = Vec::with_capacity(sources.len());
        for source_desc in sources {
            // The configuration of the Node has higher priority than the configuration of the Data Flow.
            let overwriting_configuration = source_desc
                .configuration
                .clone()
                .merge_overwrite(flow_configuration.clone());
            let runtime = mapping.get(&source_desc.id).cloned();

            flattened_sources.push(source_desc.flatten::<SourceDescriptor>(
                overwriting_configuration,
                runtime,
                vars.clone(),
            )?);
        }

        let mut flattened_sinks = Vec::with_capacity(sinks.len());
        for sink_desc in sinks {
            // The configuration of the Node has higher priority than the configuration of the Data Flow.
            let overwriting_configuration = sink_desc
                .configuration
                .clone()
                .merge_overwrite(flow_configuration.clone());

            let runtime = mapping.get(&sink_desc.id).cloned();

            flattened_sinks.push(sink_desc.flatten::<SinkDescriptor>(
                overwriting_configuration,
                runtime,
                vars.clone(),
            )?);
        }

        let mut flattened_operators = Vec::with_capacity(operators.len());
        for operator_desc in operators {
            // The configuration of the Node has higher priority than the configuration of the Data Flow.
            let overwriting_configuration = operator_desc
                .configuration
                .clone()
                .merge_overwrite(flow_configuration.clone());
            let runtime = mapping.get(&operator_desc.id).cloned();

            let (mut flat_operators, mut flat_links, patch) = operator_desc
                .flatten_maybe_composite::<CompositeOperatorDescriptor>(
                overwriting_configuration,
                runtime,
                vars.clone(),
                &mut HashSet::default(),
            )?;

            flattened_operators.append(&mut flat_operators);
            patch.apply(&mut links);
            links.append(&mut flat_links);
        }

        Ok(FlattenedDataFlowDescriptor {
            flow,
            sources: flattened_sources,
            operators: flattened_operators,
            sinks: flattened_sinks,
            links,
        })
    }
}

#[cfg(test)]
#[path = "./tests.rs"]
mod tests;

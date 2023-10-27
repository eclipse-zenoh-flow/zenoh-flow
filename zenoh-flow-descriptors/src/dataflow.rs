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

use crate::{LinkDescriptor, OperatorDescriptor, SinkDescriptor, SourceDescriptor};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use zenoh_flow_commons::{Configuration, NodeId, RuntimeId};

/// TODO@J-Loudet Documentation?
///
/// # Example
///
/// ```
/// use zenoh_flow_descriptors::DataFlowDescriptor;
///
/// let yaml = r#"
/// name: DataFlow
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
/// "#;
///
/// let data_flow_yaml = serde_yaml::from_str::<DataFlowDescriptor>(yaml).unwrap();
///
/// let json = r#"
/// {
///   "name": "DataFlow",
///
///   "configuration": {
///     "foo": "bar"
///   },
///
///   "sources": [
///     {
///       "id": "Source-0",
///       "descriptor": "file:///home/zenoh-flow/nodes/source.yaml",
///       "configuration": {
///         "answer": 0
///       }
///     }
///   ],
///
///   "operators": [
///     {
///       "id": "Operator-1",
///       "descriptor": "file:///home/zenoh-flow/nodes/operator.yaml",
///       "configuration": {
///         "answer": 1
///       }
///     }
///   ],
///
///   "sinks": [
///     {
///       "id": "Sink-2",
///       "descriptor": "file:///home/zenoh-flow/nodes/sink.yaml",
///       "configuration": {
///         "answer": 2
///       }
///     }
///   ],
///
///   "links": [
///     {
///       "from": {
///         "node": "Source-0",
///         "output": "out-operator"
///       },
///       "to": {
///         "node": "Operator-1",
///         "input": "in-source"
///       }
///     },
///     {
///       "from": {
///         "node": "Operator-1",
///         "output": "out-sink"
///       },
///       "to": {
///         "node": "Sink-2",
///         "input": "in-operator"
///       }
///     }
///   ],
///
///   "mapping": {
///     "Source-0": "zenoh-flow-plugin-0"
///   }
/// }
/// "#;
///
/// let data_flow_json = serde_json::from_str::<DataFlowDescriptor>(json).unwrap();
/// assert_eq!(data_flow_yaml, data_flow_json);
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DataFlowDescriptor {
    pub name: Arc<str>,
    #[serde(default)]
    pub configuration: Configuration,
    pub operators: Vec<OperatorDescriptor>,
    pub sources: Vec<SourceDescriptor>,
    pub sinks: Vec<SinkDescriptor>,
    pub links: Vec<LinkDescriptor>,
    #[serde(default)]
    pub mapping: HashMap<NodeId, RuntimeId>,
}

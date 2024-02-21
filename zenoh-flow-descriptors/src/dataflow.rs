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

use crate::nodes::operator::OperatorDescriptor;
use crate::nodes::sink::SinkDescriptor;
use crate::nodes::source::SourceDescriptor;
use crate::LinkDescriptor;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use uuid::Uuid;
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
/// "#;
///
/// let data_flow_yaml = serde_yaml::from_str::<DataFlowDescriptor>(yaml).unwrap();
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DataFlowDescriptor {
    /// The `uuid` uniquely identifies an instance of a data flow.
    ///
    /// If provided, Zenoh-Flow will not generate one when instantiating the flow and keep this value instead. This
    /// behavior can be useful when it is impossible to rely on a correctly configured Zenoh router (i.e. where a
    /// storage subscribing to Zenoh-Flow's key expression exists).
    pub(crate) uuid: Option<Uuid>,
    pub(crate) name: Arc<str>,
    #[serde(default)]
    pub(crate) configuration: Configuration,
    /// *(optional)* A list of Operator(s), the nodes that manipulate and / or produce data.
    #[serde(default)]
    pub(crate) operators: Vec<OperatorDescriptor>,
    /// A non-empty list of Source(s), the nodes that provide data to the data flow.
    pub(crate) sources: Vec<SourceDescriptor>,
    /// A non-empty list of Sink(s), the nodes that returns the result of the manipulation.
    pub(crate) sinks: Vec<SinkDescriptor>,
    pub(crate) links: Vec<LinkDescriptor>,
    #[serde(default)]
    pub(crate) mapping: HashMap<RuntimeId, HashSet<NodeId>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialization_deserialization() {
        let flow_json_str = r#"
{
   "name": "DataFlow",

   "configuration": {
     "foo": "bar"
   },

   "sources": [
     {
       "id": "Source-0",
       "descriptor": "file:///home/zenoh-flow/nodes/source.yaml",
       "configuration": {
         "answer": 0
       }
     }
   ],

   "operators": [
     {
       "id": "Operator-1",
       "descriptor": "file:///home/zenoh-flow/nodes/operator.yaml",
       "configuration": {
         "answer": 1
       }
     }
   ],

   "sinks": [
     {
       "id": "Sink-2",
       "descriptor": "file:///home/zenoh-flow/nodes/sink.yaml",
       "configuration": {
         "answer": 2
       }
     }
   ],

   "links": [
     {
       "from": {
         "node": "Source-0",
         "output": "out-operator"
       },
       "to": {
         "node": "Operator-1",
         "input": "in-source"
       }
     },
     {
       "from": {
         "node": "Operator-1",
         "output": "out-sink"
       },
       "to": {
         "node": "Sink-2",
         "input": "in-operator"
       }
     }
   ]
 }
 "#;
        let data_flow_json = serde_json::from_str::<DataFlowDescriptor>(flow_json_str)
            .expect("Failed to deserialize flow from JSON");
        assert!(serde_json::to_string(&data_flow_json).is_ok());
    }
}

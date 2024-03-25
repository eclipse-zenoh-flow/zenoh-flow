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

use crate::nodes::operator::OperatorDescriptor;
use crate::nodes::sink::SinkDescriptor;
use crate::nodes::source::SourceDescriptor;
use crate::LinkDescriptor;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use zenoh_flow_commons::{Configuration, InstanceId, NodeId, RuntimeId};

/// A `DataFlowDescriptor` describes an entire Zenoh-Flow application and is obtained after a parsing step.
///
/// A `DataFlowDescriptor` is meant to be parsed from a textual representation, (for now) either in the JSON or YAML
/// formats.
///
/// Once in possession of a `DataFlowDescriptor` the next step is to
/// *[flatten](crate::FlattenedDataFlowDescriptor::try_flatten())* it.
///
/// See the example for an exhaustive textual representation.
///
/// # Data Flow descriptor structure
///
/// A data flow descriptor describes the nodes that compose an application, how they are connected and, if needed, where
/// they run.
///
/// In Zenoh-Flow, we divide a descriptor into sections, each responsible for a single aspect of the applications.
///
/// The *main* sections are:
/// - `name`     : A short human-readable summary of what your data flow will do.
/// - `sources`  : A non-empty list of Source(s) to feed data into your data flow.
/// - `operators`: A list of Operator(s) to perform transformation on the data. A data flow can have no Operator, in
///                which case this section can be omitted.
/// - `sinks`    : A non-empty list of Sink(s) to output the result of the transformation(s) performed in your data
///                flow.
/// - `links`    : How the different nodes of your data flow are connected.
///
/// Special, *optional*, sections can also be added to tweak a data flow:
/// - `uuid`: The unique identifier to give to an instance of this data flow.
///   If not provided, Zenoh-Flow will generate a random one when instantiating the flow.
///
///   ⚠️ *Note that providing a `uuid` will prevent having multiple instances of this data flow on the same Zenoh
///   network*.
/// - `configuration`: To pass down values to your node when Zenoh-Flow creates it. This is useful if you want to change
///   the behaviour of your node without having to recompile it or if you want to reuse the same node several times with
///   just a tweak in its parameters. This section can be written at several places (at the data flow level, at the node
///   level, at the composite operator level) which forces Zenoh-Flow to employ a [merging
///   strategy](zenoh_flow_commons::Configuration).
///
/// - `vars`: To perform text replacements inside a descriptor and any of its nested descriptor(s) before your data flow
///   is processed by Zenoh-Flow. See its [documentation](zenoh_flow_commons::Vars) for more details.
///
/// - `mapping`: To force the deployment of a node on a Zenoh-Flow runtime. Note that it is not mandatory to assign all
///   nodes to Zenoh-Flow runtimes. The runtime that instantiates the data flow will self-assign all unassigned nodes.
///
/// # Node descriptor structure
///
/// The three types of nodes -- Sources, Sinks and Operators -- share a similar structure.
///
/// The *required* sections are:
/// - `id`     : A unique name -- within your data flow.
/// - `library`: A [Url](url::Url) pointing at the implementation of the node's logic.
/// - `inputs` : The entry points of the node (i.e. to receive data). ⚠️ Only for **Sinks** and **Operators**.
/// - `outputs`: The exit points of the node (i.e. to forward data). ⚠️ Only for **Sources** and **Operators**.
///
/// The *optional* sections are:
/// - `description`  : A human-readable description of what the node does.
/// - `configuration`: To pass down values to the node when Zenoh-Flow creates it. Values in a node's section will
///   *overwrite* that of the data flow.
///
/// # "Remote" node descriptors
///
/// Zenoh-Flow does not require to describe your nodes within the data flow descriptor. Nodes can be described in their
/// own file and then imported inside a data flow.
///
/// The descriptor is identical in both cases, except for the `id` that should not appear (in practice it can but that
/// field will be ignored).
///
/// In the data flow descriptor, the **required** sections for a "remote" node then become:
/// - `id`        : A unique name -- within your data flow.
/// - `descriptor`: A [Url](url::Url) pointing at the node's descriptor.
///
/// A remote descriptor can also optionally declare a `configuration` section. This section would overwrite both the
/// data flow **and** the remote descriptor configurations.
///
/// # Built-in Zenoh nodes
///
/// Zenoh-Flow supports two built-in nodes that require no implementation:
/// 1. A Zenoh Source: a set of Zenoh subscribers that will forward the received subscriptions to downstream nodes.
/// 2. A Zenoh Sink  : a set of Zenoh publishers that will publish the data received.
///
/// The descriptor for built-in Zenoh nodes follows this structure:
/// - `id`   : A unique name -- within your data flow.
/// - `zenoh`: An associative list of "identifier" and "key expression".
///
/// Built-in nodes do not accept `configuration`.
///
/// # Example
///
/// Below is a valid data flow descriptor that illustrates all the sections and all the possible ways of declaring nodes
/// (within the data flow descriptor, "remote" and built-in).
///
/// ```
/// # use zenoh_flow_descriptors::DataFlowDescriptor;
/// # let yaml = r#"
/// name: DataFlow
///
/// vars:
///   ULTIMATE_ANSWER: 42
///   DLL_EXT: so
///
/// configuration:
///   answer: "{{ ULTIMATE_ANSWER }}"
///
/// sources:
///   - id: Source
///     description: My Zenoh-Flow Source
///     library: "file:///home/zenoh-flow/nodes/libsource.{{ DLL_EXT }}"
///     outputs:
///       - o-source
///     configuration:
///       answer: 0
///
///   - id: Remote-Source
///     descriptor: "file:///home/zenoh-flow/nodes/source.yaml"
///     configuration:
///       answer: 1
///
///   - id: Zenoh-Source
///     zenoh-subscribers:
///       temperature: "home/*/temp"
///
/// operators:
///   - id: Operator
///     description: My Zenoh-Flow Operator
///     library: "file:///home/zenoh-flow/nodes/liboperator.{{ DLL_EXT }}"
///     inputs:
///       - i-operator
///     outputs:
///       - o-operator
///     configuration:
///       answer: 2
///
///   - id: Remote-Operator
///     descriptor: "file:///home/zenoh-flow/nodes/operator.json"
///     configuration:
///       answer: 3
///
/// sinks:
///   - id: Sink
///     description: My Zenoh-Flow Sink
///     library: "file:///home/zenoh-flow/nodes/libsink.{{ DLL_EXT }}"
///     inputs:
///       - i-sink
///     configuration:
///       answer: 4
///
///   - id: Remote-Sink
///     descriptor: "file:///home/zenoh-flow/nodes/sink.yaml"
///     configuration:
///       answer: 5
///
///   - id: Zenoh-Sink
///     zenoh-publishers:
///       average: home/average/temp
///
/// links:
///   - from:
///       node: Source
///       output : o-source
///     to:
///       node : Operator
///       input : i-operator
///
///   - from:
///       node: Remote-Source
///       output : o-source
///     to:
///       node : Remote-Operator
///       input : i-operator
///
///   - from:
///       node: Zenoh-Source
///       output : temperature
///     to:
///       node : Operator
///       input : i-operator
///
///   - from:
///       node : Operator
///       output : o-operator
///     to:
///       node : Sink
///       input : i-sink
///
///   - from:
///       node : Remote-Operator
///       output : o-operator
///     to:
///       node : Remote-Sink
///       input : i-sink
///
///   - from:
///       node : Operator
///       output : o-operator
///     to:
///       node : Zenoh-Sink
///       input : average
///
/// mapping:
///   a100ae41d10b4ec58dccba4cfa30d732:
///     - Zenoh-Source
///     - Remote-Operator
///   d8c50f6160154e409c77b61866c5cb47:
///     - Zenoh-Sink
///     - Sink
/// # "#;
/// # let data_flow_yaml = serde_yaml::from_str::<DataFlowDescriptor>(yaml).unwrap();
/// ```
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct DataFlowDescriptor {
    /// A unique identifier of an instance of this data flow.
    ///
    /// If provided, Zenoh-Flow will not generate one when instantiating the flow and keep this value instead.
    ///
    /// ⚠️ *Note that this will prevent having multiple instances of this data flow on the same Zenoh network*.
    pub(crate) id: Option<InstanceId>,
    /// A human-readable description of the data flow.
    pub(crate) name: Arc<str>,
    /// *(optional)* Pairs of `(key, value)` that are transmitted to the nodes at their creation.
    ///
    /// Each node can specify its own configuration. See the documentation of [Configuration] to see how multiple
    /// sections are merged together.
    #[serde(default)]
    pub(crate) configuration: Configuration,
    /// *(optional)* A list of Operator(s), the nodes that manipulate and / or produce data.
    #[serde(default)]
    pub(crate) operators: Vec<OperatorDescriptor>,
    /// A non-empty list of Source(s), the nodes that provide data to the data flow.
    pub(crate) sources: Vec<SourceDescriptor>,
    /// A non-empty list of Sink(s), the nodes that returns the result of the manipulation.
    pub(crate) sinks: Vec<SinkDescriptor>,
    /// A list of [LinkDescriptor], that specifies how the nodes are interconnected.
    ///
    /// Note that these links do not (and should not) consider the underlying deployment: it does not matter if a link
    /// connects nodes that are on different machines.
    pub(crate) links: Vec<LinkDescriptor>,
    /// *(optional)* A list specifying on which device the nodes run.
    ///
    /// Note that, if this field is omitted or only covers a part of the nodes, Zenoh-Flow will assign the nodes without
    /// a mapping to the Zenoh-Flow runtime that was requested to instantiate the data flow.
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

    #[test]
    fn test_serialization_deserialization_no_operators() {
        let flow_json_str = r#"
{
   "name": "DataFlow",

   "configuration": {
     "foo": "bar"
   },

   "sources": [
     {
       "id": "Source",
       "descriptor": "file:///home/zenoh-flow/nodes/source.yaml",
       "configuration": {
         "answer": 0
       }
     }
   ],

   "sinks": [
     {
       "id": "Sink",
       "descriptor": "file:///home/zenoh-flow/nodes/sink.yaml",
       "configuration": {
         "answer": 2
       }
     }
   ],

   "links": [
     {
       "from": {
         "node": "Source",
         "output": "o-source"
       },
       "to": {
         "node": "Sink",
         "input": "i-sink"
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
